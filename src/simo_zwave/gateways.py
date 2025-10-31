import asyncio
import json
import logging
import threading
import time
from typing import Dict, Any, Optional

import paho.mqtt.client as mqtt
from django.conf import settings

from simo.core.models import Component
from simo.core.gateways import BaseObjectCommandsGatewayHandler
from simo.core.events import GatewayObjectCommand, get_event_obj
from .forms import ZwaveGatewayForm
from .models import ZwaveNode, NodeValue

try:
    from zwave_js_server.client import Client as ZJSClient
except Exception:  # pragma: no cover - library not installed yet
    ZJSClient = None


class ZwaveGatewayHandler(BaseObjectCommandsGatewayHandler):
    name = "Z-Wave JS"
    config_form = ZwaveGatewayForm
    auto_create = True
    periodic_tasks = (('maintain', 10), ('ufw_expiry_check', 60), ('sync_values', 10))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws_url = self._build_ws_url()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client: Optional[ZJSClient] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = False
        self._last_state: Dict[str, Any] = {}

    # --------------- Lifecycle ---------------
    def run(self, exit):
        self.exit = exit
        # Start WS thread immediately to avoid early send attempts failing
        self._start_ws_thread()
        # Start MQTT command listener (BaseObjectCommandsGatewayHandler)
        super().run(exit)

    def _start_ws_thread(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._ws_main, daemon=True)
        self._thread.start()

    def _ws_main(self):
        if ZJSClient is None:
            self.logger.error("zwave-js-server-python not installed; cannot connect")
            return
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._ws_connect_and_listen())

    async def _ws_connect_and_listen(self):
        backoff = 1
        while not self.exit.is_set():
            try:
                import aiohttp
                session = aiohttp.ClientSession()
                self._client = ZJSClient(self._ws_url, session)
                await self._client.connect()
                self._connected = True
                backoff = 1
                # Start listening and wait until driver is ready
                driver_ready = asyncio.Event()
                listen_task = asyncio.create_task(self._client.listen(driver_ready))
                await driver_ready.wait()
                # Import full state from driver model
                await self._import_driver_state()
                # Keep task running until closed
                await listen_task
            except Exception as e:
                self._connected = False
                try:
                    self.logger.warning(f"WS disconnected: {e}")
                except Exception:
                    pass
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

    # --------------- Periodic tasks ---------------
    def maintain(self):
        # Ensure WS thread is running
        # Refresh WS URL from config in case it changed
        self._ws_url = self._build_ws_url()
        self._start_ws_thread()

    def ufw_expiry_check(self):
        try:
            cfg = self.gateway_instance.config or {}
            if not cfg.get('ui_open'):
                return
            if cfg.get('ui_expires_at', 0) < time.time():
                from .forms import ZwaveGatewayForm
                # Reuse helper to close rules
                form = ZwaveGatewayForm(instance=self.gateway_instance)
                form._ufw_deny_8091_lan()
                cfg['ui_open'] = False
                cfg.pop('ui_expires_at', None)
                self.gateway_instance.config = cfg
                self.gateway_instance.save(update_fields=['config'])
                self.logger.info("Closed temporary Z-Wave UI access (expired)")
        except Exception:
            pass

    def sync_values(self):
        # Periodically import current driver state into DB so values remain fresh
        try:
            if self._client and self._client.connected:
                self._async_call(self._import_driver_state())
        except Exception:
            pass

    # --------------- MQTT commands ---------------
    def perform_value_send(self, component, value):
        # If WS is not connected yet, skip with a concise log
        if not self._client or not self._client.connected:
            try:
                self.logger.info("WS not connected; skipping send")
            except Exception:
                pass
            return
        node_val = NodeValue.objects.filter(
            pk=component.config.get('zwave_item')
        ).first()
        if not node_val:
            return
        try:
            # Attempt to coerce string values
            if isinstance(value, str):
                if value.lower() in ('true', 'on'):
                    value = True
                elif value.lower() in ('false', 'off'):
                    value = False
                else:
                    try:
                        value = float(value) if '.' in value else int(value)
                    except Exception:
                        pass
            # If addressing is missing (legacy rows), try to find a sibling with addressing
            nv = node_val
            if not nv.command_class:
                from django.db.models import Q
                alt = NodeValue.objects.filter(node=nv.node).filter(
                    Q(name__iexact=nv.name) | Q(label__iexact=nv.label)
                ).exclude(pk=nv.pk).first()
                if alt and alt.command_class:
                    nv = alt
            self._async_call(self._set_value(nv, value))
        except Exception as e:
            self.logger.error(f"Send error: {e}", exc_info=True)

    def perform_bulk_send(self, data):
        components = {c.id: c for c in Component.objects.filter(
            gateway=self.gateway_instance, id__in=[int(i) for i in data.keys()]
        )}
        for comp_id, val in data.items():
            comp = components.get(int(comp_id))
            if not comp:
                continue
            try:
                self.perform_value_send(comp, val)
            except Exception as e:
                self.logger.error(e, exc_info=True)

    # Extend parent MQTT handler to support controller commands
    def _on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload)
        except Exception:
            return super()._on_mqtt_message(client, userdata, msg)
        if 'zwave_command' in payload:
            cmd = payload.get('zwave_command')
            node_id = payload.get('node_id')
            try:
                self._async_call(self._controller_command(cmd, node_id))
            except Exception as e:
                self.logger.error(f"Controller command error: {e}")
            return
        # fallback to default handler (set_val, bulk_send)
        return super()._on_mqtt_message(client, userdata, msg)

    async def _controller_command(self, cmd: str, node_id: Optional[int]):
        if not self._client or not self._client.connected:
            return
        # Map legacy commands to server API
        mapping = {
            'add_node': {'command': 'begin_inclusion'},
            'remove_node': {'command': 'begin_exclusion'},
            'stop_inclusion': {'command': 'stop_inclusion'},
            'stop_exclusion': {'command': 'stop_exclusion'},
        }
        if cmd in mapping:
            await self._client.async_send_command(mapping[cmd])
            return
        if cmd == 'cancel_command':
            # Try to stop both inclusion and exclusion
            try:
                await self._client.async_send_command({'command': 'stop_inclusion'})
            except Exception:
                pass
            try:
                await self._client.async_send_command({'command': 'stop_exclusion'})
            except Exception:
                pass
            return
        # Node-scoped ops
        if node_id:
            if cmd == 'remove_failed_node':
                await self._client.async_send_command({'command': 'remove_failed_node', 'nodeId': node_id})
            elif cmd == 'replace_failed_node':
                await self._client.async_send_command({'command': 'replace_failed_node', 'nodeId': node_id})

    # --------------- WS helpers ---------------
    def _async_call(self, coro):
        if not self._loop:
            raise RuntimeError('WS loop not started')
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=15)

    def _build_ws_url(self) -> str:
        try:
            cfg = self.gateway_instance.config or {}
            host = cfg.get('ws_host') or '127.0.0.1'
            port = int(cfg.get('ws_port') or 3000)
            return f'ws://{host}:{port}'
        except Exception:
            return 'ws://127.0.0.1:3000'

    def _build_value_id(self, nv: NodeValue) -> Dict[str, Any]:
        def _coerce(val: Any) -> Any:
            if isinstance(val, str) and val.isdigit():
                try:
                    return int(val)
                except Exception:
                    return val
            return val
        prop = _coerce(nv.property)
        # For Switch command classes, writes go to targetValue
        try:
            if nv.command_class in (37, 38) and prop == 'currentValue':
                prop = 'targetValue'
        except Exception:
            pass
        vid: Dict[str, Any] = {
            'commandClass': nv.command_class,
            'endpoint': nv.endpoint or 0,
            'property': prop,
        }
        pk = nv.property_key
        if pk not in (None, ''):
            vid['propertyKey'] = _coerce(pk)
        return vid

    async def _set_value(self, node_val: NodeValue, value):
        if not self._client or not self._client.connected:
            raise RuntimeError('Z-Wave JS not connected')
        # Prefer modern API: node.set_value
        value_id = self._build_value_id(node_val)
        try:
            await self._client.async_send_command({
                'command': 'node.set_value',
                'nodeId': node_val.node.node_id,
                'valueId': value_id,
                'value': value,
            })
        except Exception:
            # Fallback for older servers
            payload = {
                'command': 'set_value',
                'nodeId': node_val.node.node_id,
                'commandClass': value_id['commandClass'],
                'endpoint': value_id['endpoint'],
                'property': value_id['property'],
                'value': value,
            }
            if 'propertyKey' in value_id:
                payload['propertyKey'] = value_id['propertyKey']
            await self._client.async_send_command(payload)

    async def _import_driver_state(self):
        if not self._client or not self._client.driver:
            return
        driver = self._client.driver
        try:
            nodes = list(driver.controller.nodes.values())
        except Exception:
            nodes = []
        for node in nodes:
            try:
                # Build a pseudo state dict for import
                values = []
                for v in getattr(node, 'values', {}).values():
                    try:
                        meta = getattr(v, 'metadata', None)
                        values.append({
                            'commandClass': getattr(v, 'command_class', None),
                            'endpoint': getattr(v, 'endpoint', 0) or 0,
                            'property': getattr(v, 'property_', None),
                            'propertyKey': getattr(v, 'property_key', None),
                            'propertyName': getattr(v, 'property_name', None),
                            'value': getattr(v, 'value', None),
                            'metadata': {
                                'label': getattr(meta, 'label', None),
                                'unit': getattr(meta, 'unit', ''),
                                'writeable': getattr(meta, 'writeable', False),
                                'type': getattr(meta, 'type', ''),
                                'states': getattr(meta, 'states', None) or [],
                            },
                        })
                    except Exception:
                        continue
                state = {
                    'nodeId': node.node_id,
                    'name': getattr(node, 'name', '') or '',
                    'productLabel': getattr(node, 'product_label', '') or '',
                    'values': values,
                }
                # Run ORM imports in thread to avoid async DB access
                import asyncio as _asyncio
                await _asyncio.to_thread(self._import_node_sync, state)
            except Exception:
                self.logger.error("Failed to import node state", exc_info=True)

    def _import_node_sync(self, node_state: Dict[str, Any]):
        node_id = node_state.get('nodeId') or node_state.get('id')
        if not node_id:
            return
        name = node_state.get('name') or ''
        product = node_state.get('productLabel') or node_state.get('productType') or ''
        zn, _ = ZwaveNode.objects.get_or_create(
            node_id=node_id, gateway=self.gateway_instance,
            defaults={'product_name': product, 'product_type': product}
        )
        if name and zn.name != name:
            zn.name = name
        zn.alive = True
        zn.save()
        values = node_state.get('values', {})
        if isinstance(values, dict):
            vals_iter = values.values()
        else:
            vals_iter = values
        for v in vals_iter:
            self._import_value(zn, v)

    def _import_value(self, zn: ZwaveNode, val: Dict[str, Any]):
        from django.db.models import Q
        cc = val.get('commandClass')
        endpoint = val.get('endpoint') or 0
        prop = val.get('property')
        prop_key = val.get('propertyKey')
        label = (val.get('metadata') or {}).get('label') or val.get('propertyName') or str(prop)
        units = (val.get('metadata') or {}).get('unit') or ''
        read_only = not (val.get('metadata') or {}).get('writeable', False)
        vtype = (val.get('metadata') or {}).get('type') or ''
        current = val.get('value')
        data = {
            'genre': None,
            'type': str(vtype),
            'label': label,
            'is_read_only': read_only,
            'index': None,
            'units': units,
            'value': current,
            'value_new': current,
            'value_choices': (val.get('metadata') or {}).get('states') or [],
            'command_class': cc,
            'endpoint': endpoint,
            'property': str(prop),
            'property_key': '' if prop_key is None else str(prop_key),
        }
        # Try to match existing rows by user-assigned name or label
        base_qs = NodeValue.objects.filter(node=zn).filter(
            Q(name__iexact=label) | Q(label__iexact=label)
        )
        try:
            nv = base_qs.filter(component__isnull=False).first()
        except Exception:
            nv = None
        if not nv:
            nv = base_qs.first()
        created = False
        if not nv:
            # Try to reuse a single existing assigned value with matching type/units (best-effort)
            cands = NodeValue.objects.filter(
                node=zn, component__isnull=False, type=str(vtype), units=units
            )
            if cands.count() == 1:
                nv = cands.first()
                created = False
            else:
                nv, created = NodeValue.objects.get_or_create(
                    node=zn,
                    value_id=hash((cc, endpoint, str(prop), str(prop_key))),
                    defaults=data,
                )
        else:
            for k, v in data.items():
                setattr(nv, k, v)
        nv.save()
        # Battery level shortcut
        if cc == 0x80 and units == '%':
            zn.battery_level = current
            zn.save(update_fields=['battery_level'])
        # Push to component if linked
        if nv.component:
            try:
                nv.component.controller._receive_from_device(nv.value)
            except Exception:
                self.logger.error("Failed to set component value", exc_info=True)

    # With client.listen(), the library updates the driver model internally.
    # We keep DB in sync via periodic full imports or future event hooks.
