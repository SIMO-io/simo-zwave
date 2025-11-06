import asyncio
import logging
import time
from typing import Any, Dict

from simo.core.controllers import (
    BinarySensor, NumericSensor,
    Switch, Dimmer, RGBWLight, Button, ControllerBase
)
from simo.core.forms import BaseComponentForm
from .gateways import ZwaveGatewayHandler
from .base_types import ZwaveDeviceType
from .forms import (
    ZwaveKnobComponentConfigForm,
    RGBLightComponentConfigForm, ZwaveNumericSensorConfigForm,
    ZwaveSwitchConfigForm
)
from simo.core.app_widgets import NumericSensorWidget

try:
    from zwave_js_server.client import Client as ZJSClient
except Exception:  # pragma: no cover
    ZJSClient = None

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None


class ZwaveDynamicConfigMixin:
    """Add dynamic Z-Wave device settings to component config forms.

    Forms using ConfigFieldsMixin will call these hooks if present.
    """

    _zjs_cfg_map: Dict[str, Dict[str, Any]] = None

    def _ws_url(self) -> str:
        try:
            gw = self.component.gateway
            if gw and gw.config and gw.config.get('ws_url'):
                return gw.config['ws_url']
        except Exception:
            pass
        return 'ws://127.0.0.1:3000'

    def _get_dynamic_config_fields(self) -> Dict[str, Any]:
        if ZJSClient is None or aiohttp is None:
            return {}
        zw = (self.component.config or {}).get('zwave') or {}
        node_id = zw.get('nodeId')
        endpoint = zw.get('endpoint') or 0
        if not node_id:
            return {}
        # Use small TTL cache to avoid repeated costly reads during admin usage
        data = None
        try:
            data = self._fetch_config_with_cache(self._ws_url(), int(node_id), int(endpoint))
        except Exception:
            logging.getLogger(__name__).exception("Failed to fetch Z-Wave config parameters")
            return {}
        fields: Dict[str, Any] = {}
        self._zjs_cfg_map = {}
        from django import forms
        for item in data:
            vid = item['valueId']
            meta = item.get('metadata') or {}
            cur = item.get('value')
            fname = f"cfg_112_{vid.get('endpoint',0)}_{vid.get('property')}_{vid.get('propertyKey','0')}"
            label = str(meta.get('label') or vid.get('propertyName') or vid.get('property'))
            unit = str(meta.get('unit') or '')
            if unit:
                label = f"{label} ({unit})"
            dtype = str(meta.get('type') or '').lower()
            choices = meta.get('states') or {}
            required = False
            if isinstance(choices, dict) and choices:
                opts = [(str(k), str(v)) for k, v in choices.items()]
                field = forms.ChoiceField(label=label, choices=opts, required=required)
                field.initial = str(cur) if cur is not None else None
            elif dtype == 'boolean':
                field = forms.BooleanField(label=label, required=required)
                field.initial = bool(cur) if cur is not None else False
            elif dtype in ('number', 'int', 'integer'):
                minimum = meta.get('min')
                maximum = meta.get('max')
                if isinstance(minimum, float) or isinstance(maximum, float):
                    field = forms.FloatField(label=label, required=required)
                else:
                    field = forms.IntegerField(label=label, required=required)
                if minimum is not None:
                    field.min_value = minimum
                if maximum is not None:
                    field.max_value = maximum
                field.initial = cur
            else:
                field = forms.CharField(label=label, required=required)
                field.initial = str(cur) if cur is not None else ''
            fields[fname] = field
            self._zjs_cfg_map[fname] = {'valueId': vid, 'initial': cur, 'metadata': meta}
        return fields

    def _apply_dynamic_config(self, cleaned_data: Dict[str, Any]):
        if ZJSClient is None or aiohttp is None:
            return
        if not (self._zjs_cfg_map and isinstance(self._zjs_cfg_map, dict)):
            return
        zw = (self.component.config or {}).get('zwave') or {}
        node_id = zw.get('nodeId')
        if not node_id:
            return
        updates = []
        for fname, info in self._zjs_cfg_map.items():
            if fname not in cleaned_data:
                continue
            new_val = cleaned_data[fname]
            old_val = info.get('initial')
            states = (info.get('metadata') or {}).get('states') or {}
            if states and isinstance(new_val, str):
                try:
                    if new_val.isdigit():
                        new_val = int(new_val)
                except Exception:
                    pass
            if new_val != old_val:
                updates.append({'valueId': info.get('valueId'), 'value': new_val})
        if not updates:
            return
        try:
            asyncio.run(asyncio.wait_for(self._async_apply_config_updates(self._ws_url(), int(node_id), updates), timeout=4.0))
        except Exception:
            logging.getLogger(__name__).exception("Failed to apply Z-Wave config updates")

    # ----- Async helpers -----
    async def _async_fetch_config_parameters(self, ws_url: str, node_id: int, endpoint: int):
        session = aiohttp.ClientSession()
        client = ZJSClient(ws_url, session)
        try:
            await client.connect()
            # Start listener to process responses
            driver_ready = asyncio.Event()
            listen_task = asyncio.create_task(client.listen(driver_ready))
            # give the connection a brief moment to initialize schema
            try:
                await asyncio.wait_for(driver_ready.wait(), timeout=1.0)
            except Exception:
                # proceed even if not fully ready; commands may still work
                await asyncio.sleep(0.05)
            # guard: if server is slow/unreachable, let outer wait_for handle timeout
            resp = await client.async_send_command({'command': 'node.get_defined_value_ids', 'nodeId': node_id})
            items = []
            if isinstance(resp, dict):
                items = resp.get('valueIds') or resp.get('result') or []
            if not isinstance(items, list):
                items = []
            def getf(item, key, fallback=None):
                if isinstance(item, dict):
                    return item.get(key, fallback)
                return getattr(item, key, fallback)
            # Prefer exact endpoint; if none found, include endpoint 0 globals
            candidates = [i for i in items if getf(i, 'commandClass') == 112 and (getf(i, 'endpoint') or 0) == (endpoint or 0)]
            if not candidates:
                candidates = [i for i in items if getf(i, 'commandClass') == 112 and (getf(i, 'endpoint') or 0) == 0]
            # Limit to reasonable count
            candidates = candidates[:40]

            async def fetch_one(it):
                vid = {
                    'commandClass': getf(it, 'commandClass'),
                    'endpoint': getf(it, 'endpoint') or 0,
                    'property': getf(it, 'property'),
                }
                pk = getf(it, 'propertyKey')
                if pk is not None:
                    vid['propertyKey'] = pk
                # Metadata
                try:
                    meta_resp = await client.async_send_command({'command': 'node.get_value_metadata', 'nodeId': node_id, 'valueId': vid})
                    if isinstance(meta_resp, dict):
                        metadata = meta_resp.get('metadata') or meta_resp.get('result') or meta_resp
                        if not isinstance(metadata, dict):
                            metadata = {}
                    else:
                        metadata = {}
                except Exception:
                    metadata = {}
                if not (isinstance(metadata, dict) and metadata.get('writeable')):
                    return None
                try:
                    val_resp = await client.async_send_command({'command': 'node.get_value', 'nodeId': node_id, 'valueId': vid})
                    if isinstance(val_resp, dict):
                        cur = val_resp.get('value', val_resp.get('result'))
                    else:
                        cur = None
                except Exception:
                    cur = None
                return {'valueId': vid, 'metadata': metadata, 'value': cur}

            results = await asyncio.gather(*[fetch_one(it) for it in candidates], return_exceptions=True)
            # filter out None/exceptions
            out = []
            for r in results:
                if isinstance(r, dict):
                    out.append(r)
            return out
        finally:
            try:
                try:
                    listen_task.cancel()
                except Exception:
                    pass
                await client.disconnect()
            except Exception:
                pass
            await session.close()

    async def _async_apply_config_updates(self, ws_url: str, node_id: int, updates: list):
        session = aiohttp.ClientSession()
        client = ZJSClient(ws_url, session)
        try:
            await client.connect()
            for up in updates:
                await client.async_send_command({
                    'command': 'node.set_value',
                    'nodeId': node_id,
                    'valueId': up['valueId'],
                    'value': up['value'],
                })
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            await session.close()

    # simple process cache with TTL
    _CFG_CACHE: Dict[str, Any] = {}

    def _fetch_config_with_cache(self, ws_url: str, node_id: int, endpoint: int):
        key = (ws_url, node_id, endpoint)
        now = time.time()
        entry = self._CFG_CACHE.get(key)
        if entry and (now - entry['ts'] < 120):
            return entry['data']
        # give the fetch up to 3 seconds
        data = asyncio.run(asyncio.wait_for(self._async_fetch_config_parameters(ws_url, node_id, endpoint), timeout=6.0))
        self._CFG_CACHE[key] = {'ts': now, 'data': data}
        return data


class ZwaveBinarySensor(ZwaveDynamicConfigMixin, BinarySensor):
    gateway_class = ZwaveGatewayHandler
    config_form = BaseComponentForm
    manual_add = False


class ZwaveNumericSensor(ZwaveDynamicConfigMixin, NumericSensor):
    gateway_class = ZwaveGatewayHandler
    config_form = ZwaveNumericSensorConfigForm
    manual_add = False


class ZwaveSwitch(ZwaveDynamicConfigMixin, Switch):
    gateway_class = ZwaveGatewayHandler
    config_form = ZwaveSwitchConfigForm
    manual_add = False


class ZwaveDimmer(ZwaveDynamicConfigMixin, Dimmer):
    gateway_class = ZwaveGatewayHandler
    config_form = ZwaveKnobComponentConfigForm
    manual_add = False

    def _send_to_device(self, value):
        conf = self.component.config

        com_amplitude = conf.get('max', 1.0) - conf.get('min', 0.0)
        float_value = (value - conf.get('min', 0.0)) / com_amplitude

        zwave_amplitude = conf.get('zwave_max', 99.0) - conf.get('zwave_min', 0.0)
        set_val = float_value * zwave_amplitude + conf.get('zwave_min', 0.0)

        return super()._send_to_device(set_val)

    def _receive_from_device(self, val, **kwargs):
        conf = self.component.config

        zwave_amplitude = conf.get('zwave_max', 99.0) - conf.get('zwave_min', 0.0)
        try:
            float_value = (val - conf.get('zwave_min', 0.0)) / zwave_amplitude
        except Exception:
            float_value = 0

        com_amplitude = conf.get('max', 99.0) - conf.get('min', 0.0)
        set_val = float_value * com_amplitude + conf.get('min', 0.0)

        return super()._receive_from_device(set_val, **kwargs)


class ZwaveRGBWLight(ZwaveDynamicConfigMixin, RGBWLight):
    gateway_class = ZwaveGatewayHandler
    config_form = RGBLightComponentConfigForm
    manual_add = False


class ZwaveButton(ZwaveDynamicConfigMixin, Button):
    gateway_class = ZwaveGatewayHandler
    config_form = BaseComponentForm
    manual_add = False

    def _receive_from_device(self, val, **kwargs):
        # Map Z-Wave JS Central Scene event values to Button states.
        # Accept both numeric codes and string labels.
        mapping_num = {
            0: 'click',            # KeyPressed
            1: 'up',               # KeyReleased
            2: 'hold',             # KeyHeldDown
            3: 'double-click',     # KeyPressed2x
            4: 'triple-click',     # KeyPressed3x
            5: 'quadruple-click',  # KeyPressed4x
            6: 'quintuple-click',  # KeyPressed5x
        }
        mapping_str = {
            'KeyPressed': 'click',
            'KeyReleased': 'up',
            'KeyHeldDown': 'hold',
            'KeyPressed2x': 'double-click',
            'KeyPressed3x': 'triple-click',
            'KeyPressed4x': 'quadruple-click',
            'KeyPressed5x': 'quintuple-click',
        }
        try:
            if isinstance(val, (int, float)):
                v = mapping_num.get(int(val))
                if v:
                    return super()._receive_from_device(v, **kwargs)
            elif isinstance(val, str):
                v = mapping_str.get(val) or val.lower()
                # accept already-normalized values too
                return super()._receive_from_device(v, **kwargs)
        except Exception:
            pass
        # Fallback: ignore unknowns
        return


class ZwaveDevice(ControllerBase):
    """Z-Wave pairing placeholder used to start inclusion/adoption.

    Users select this single type when adding a new Z-Wave component. The
    controller starts Z-Wave inclusion and listens for node activity; actual
    device-specific components (e.g., Switch/Dimmer/Sensor) are created by
    discovery handlers based on what the node reports.
    """

    gateway_class = ZwaveGatewayHandler
    config_form = BaseComponentForm
    name = "Z-Wave Device"
    base_type = ZwaveDeviceType
    default_value = False
    manual_add = True
    app_widget = NumericSensorWidget
    accepts_value = False
    discovery_msg = (
        "Press include on the device or operate it; we will create the matching components."
    )

    def _validate_val(self, value, occasion=None):
        return value

    @classmethod
    def _init_discovery(cls, form_cleaned_data):
        """Begin Z-Wave inclusion and mark discovery active.

        Stores the initial form data so the gateway/UI can finalize when the
        first useful node activity is observed.
        """
        from simo.core.models import Gateway
        from simo.core.utils.serialization import serialize_form_data
        gw = Gateway.objects.filter(type=cls.gateway_class.uid).first()
        if not gw:
            return {'error': 'Z-Wave gateway is not configured.'}
        try:
            import logging
            logging.getLogger(__name__).info("ZwaveDevice: start_discovery requested")
        except Exception:
            pass
        gw.start_discovery(cls.uid, serialize_form_data(form_cleaned_data), timeout=120)
        # Nudge the gateway to start controller inclusion over WS
        from simo.core.events import GatewayObjectCommand
        try:
            GatewayObjectCommand(gw, gw, command='discover', type=cls.uid).publish()
        except Exception:
            import logging
            logging.getLogger(__name__).error("Failed to publish discover command", exc_info=True)

    @classmethod
    def _process_discovery(cls, started_with, data):
        """Handled by the gateway upon actual node activity.

        This controller does not create a component for itself; instead the
        gateway will create device-specific components and append results to
        the discovery record. Nothing to do here.
        """
        return None
