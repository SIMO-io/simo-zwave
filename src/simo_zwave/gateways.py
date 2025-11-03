import asyncio
import json
import logging
import threading
import time
from typing import Dict, Any, Optional, List, Tuple

import paho.mqtt.client as mqtt
from django.conf import settings

from simo.core.models import Component
from simo.core.gateways import BaseObjectCommandsGatewayHandler
from simo.core.events import GatewayObjectCommand, get_event_obj
from simo.core.loggers import get_gw_logger
from .forms import ZwaveGatewayForm

try:
    from zwave_js_server.client import Client as ZJSClient
except Exception:  # pragma: no cover - library not installed yet
    ZJSClient = None


class ZwaveGatewayHandler(BaseObjectCommandsGatewayHandler):
    name = "Z-Wave JS"
    config_form = ZwaveGatewayForm
    auto_create = True
    periodic_tasks = (
        ('maintain', 10),
        ('ufw_expiry_check', 60),
        # Poll a small set of bound sensor values directly from server for reliability
        ('sync_bound_values', 20),
        # Proactively ping dead nodes to bring them back quickly
        ('ping_dead_nodes', 10),
        # Sync node name/location from bound SIMO components
        ('sync_node_labels', 60),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws_url = self._build_ws_url()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client: Optional[ZJSClient] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = False
        self._last_state: Dict[str, Any] = {}
        self._last_node_refresh: Dict[int, float] = {}
        self._last_sync_log: float = 0.0
        self._last_dead_ping: Dict[int, float] = {}
        self._last_push: Dict[int, Any] = {}
        self._last_node_labels: Dict[int, tuple] = {}
        # Config-driven routing caches for Component.config-based mapping
        self._value_map: Dict[tuple, list] = {}
        self._node_to_components: Dict[int, list] = {}
        

    # --------------- Helpers ---------------
    @staticmethod
    def _normalize_label(txt: Optional[str]) -> str:
        """Normalize common sensor label names."""
        if not txt:
            return ''
        t = str(txt).strip().lower()
        # Simple canonicalization
        repl = {
            'air temperature': 'temperature',
            'temperature': 'temperature',
            'temp': 'temperature',
            'illuminance': 'luminance',
            'luminance': 'luminance',
            'light': 'luminance',
            'light level': 'luminance',
            'lux': 'luminance',
            'relative humidity': 'humidity',
            'humidity': 'humidity',
            'home security': 'motion',
            'motion alarm': 'motion',
            'motion': 'motion',
            'sensor': 'motion',
            'burglar': 'motion',
            'motion sensor status': 'motion',
        }
        # Try exact, else partial contains for common words
        if t in repl:
            return repl[t]
        for key, val in repl.items():
            if key in t:
                return val
        return t

    # --------------- Lifecycle ---------------
    def run(self, exit):
        self.exit = exit
        try:
            self.logger = get_gw_logger(self.gateway_instance.id)
        except Exception:
            logging.exception("Failed to initialize gateway logger")
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
                try:
                    self.logger.info(f"Connecting WS {self._ws_url}")
                except Exception:
                    pass
                await self._client.connect()
                self._connected = True
                backoff = 1
                try:
                    self.logger.info("WS connected; waiting for driver ready")
                except Exception:
                    pass
                # Start listening and wait until driver is ready
                driver_ready = asyncio.Event()
                listen_task = asyncio.create_task(self._client.listen(driver_ready))
                await driver_ready.wait()
                try:
                    self.logger.info("Driver ready; importing full state")
                except Exception:
                    pass
                # Import full state from driver model
                await self._import_driver_state()
                # Attach event listeners for real-time updates
                try:
                    self._attach_event_listeners()
                except Exception:
                    try:
                        self.logger.info("Failed to attach event listeners; falling back to periodic sync only")
                    except Exception:
                        pass
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
        # Start/stop inclusion based on discovery state for ZwaveDevice pairing
        try:
            disc = (self.gateway_instance.discovery or {})
            uid = str(disc.get('controller_uid') or '')
            # Defer import to avoid module import cycles
            from simo_zwave.controllers import ZwaveDevice  # type: ignore
            zw_uid = ZwaveDevice.uid
        except Exception:
            disc = {}
            zw_uid = None
        try:
            if zw_uid and uid == zw_uid and not disc.get('finished') and self._client and self._client.connected:
                # Begin inclusion once per discovery session
                if not disc.get('inclusion_started'):
                    try:
                        self.logger.info("Starting Z-Wave inclusion (pairing mode)")
                    except Exception:
                        pass
                    try:
                        self._async_call(self._controller_command('add_node', None), timeout=10)
                    except Exception:
                        try:
                            self.logger.error("Failed to start inclusion", exc_info=True)
                        except Exception:
                            pass
                    else:
                        disc['inclusion_started'] = time.time()
                        self.gateway_instance.discovery = disc
                        try:
                            self.gateway_instance.save(update_fields=['discovery'])
                        except Exception:
                            pass
            # If discovery finished, make sure inclusion is stopped
            if zw_uid and uid == zw_uid and disc.get('finished') and self._client and self._client.connected:
                if disc.get('inclusion_started') and not disc.get('inclusion_stopped'):
                    try:
                        self.logger.info("Stopping Z-Wave inclusion (finish discovery)")
                    except Exception:
                        pass
                    try:
                        self._async_call(self._controller_command('stop_inclusion', None), timeout=10)
                    except Exception:
                        pass
                    disc['inclusion_stopped'] = time.time()
                    self.gateway_instance.discovery = disc
                    try:
                        self.gateway_instance.save(update_fields=['discovery'])
                    except Exception:
                        pass
        except Exception:
            try:
                self.logger.error("Inclusion maintenance error", exc_info=True)
            except Exception:
                pass
        # Rebuild config routing map
        try:
            self._rebuild_config_map()
        except Exception:
            try:
                self.logger.error("Failed to rebuild config routing map", exc_info=True)
            except Exception:
                pass

    def _attach_event_listeners(self):
        if not self._client or not self._client.driver:
            return
        controller = self._client.driver.controller
        for node in list(getattr(controller, 'nodes', {}).values()):
            try:
                node.on('value updated', lambda event, n=node: self._on_value_event(event, n))
                node.on('value added', lambda event, n=node: self._on_value_event(event, n))
                node.on('value removed', lambda event, n=node: self._on_value_event(event, n))
                node.on('value notification', lambda event, n=node: self._on_value_event(event, n))
                node.on('notification', lambda event, n=node: self._on_value_event(event, n))
                node.on('metadata updated', lambda event, n=node: self._on_value_event(event, n))
                node.on('dead', lambda event, n=node: self._on_node_status_event(event, n))
                node.on('alive', lambda event, n=node: self._on_node_status_event(event, n))
                node.on('sleep', lambda event, n=node: self._on_node_status_event(event, n))
                node.on('wake up', lambda event, n=node: self._on_node_status_event(event, n))
            except Exception:
                self.logger.error(f"Failed to attach listeners for node {getattr(node,'node_id',None)}", exc_info=True)
                continue

    def _on_mqtt_message(self, client, userdata, msg):
        # Extend base handling with a lightweight 'discover' trigger for inclusion
        super()._on_mqtt_message(client, userdata, msg)
        try:
            payload = json.loads(msg.payload)
        except Exception:
            return
        cmd = payload.get('command')
        if cmd != 'discover':
            return
        # Start inclusion only when asked to discover ZwaveDevice
        typ = payload.get('type')
        try:
            from simo_zwave.controllers import ZwaveDevice  # type: ignore
            zw_uid = ZwaveDevice.uid
        except Exception:
            zw_uid = None
        if not zw_uid or typ != zw_uid:
            return
        try:
            if not (self._client and self._client.connected):
                return
            self.logger.info("MQTT: begin Z-Wave inclusion")
            self._async_call(self._controller_command('add_node', None), timeout=10)
            disc = self.gateway_instance.discovery or {}
            disc['inclusion_started'] = time.time()
            self.gateway_instance.discovery = disc
            try:
                self.gateway_instance.save(update_fields=['discovery'])
            except Exception:
                pass
        except Exception:
            try:
                self.logger.error("Failed to begin inclusion from MQTT discover", exc_info=True)
            except Exception:
                pass

    def _on_value_event(self, event, node=None):
        try:
            # Normalize event to dict payload
            data = event
            if hasattr(event, 'data') and isinstance(event.data, dict):
                data = event.data
            if not isinstance(data, dict):
                return
            event_name = str(data.get('event') or '').lower()
            args = data.get('args') or {}
            # Derive node id
            node_id = getattr(node, 'node_id', None) or data.get('nodeId')
            if not node_id:
                return
            if event_name == 'notification':
                # Log full notification context for visibility
                try:
                    self.logger.warning(f"Notification event node={node_id} data={data}")
                except Exception:
                    pass
                # Proactively poll bound values on this node (e.g. CC48/113 motion)
                try:
                    # Run ORM-bound poll work in a thread
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(self._poll_node_bound_values, node_id), self._loop)
                except Exception:
                    self.logger.error(f"Notification follow-up poll failed node={node_id}", exc_info=True)
                return
            if event_name == 'value removed':
                # Do not push a None/removed value into components; rely on next update
                try:
                    self.logger.info(f"Skip fast-path for value removed node={node_id} args={args}")
                except Exception:
                    pass
                return
            # Build a val dict similar to _import_driver_state using args
            val = {
                'commandClass': args.get('commandClass') or args.get('ccId'),
                'endpoint': args.get('endpoint') or args.get('endpointIndex') or 0,
                'property': args.get('property'),
                'propertyKey': args.get('propertyKey'),
                'propertyName': args.get('propertyName'),
                'value': args.get('newValue', args.get('value')),
                'metadata': args.get('metadata') or {},
            }
            # For Basic/Binary Sensor/Notification CC events, proactively poll this node immediately
            try:
                cc_here = val.get('commandClass')
                if cc_here in (32, 48, 113):
                    # Don't block the event loop; fire-and-forget
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(self._poll_node_bound_values, node_id), self._loop)
            except Exception:
                pass
            if val.get('commandClass') is None or (val.get('property') is None and val.get('propertyName') is None):
                # Fail loudly for unmapped value events to guide improvements
                try:
                    self.logger.error(f"Unmapped value event node={node_id} event={event_name} args={args}")
                except Exception:
                    pass
                # As a fallback, poll this node.
                try:
                    self._poll_node_bound_values(node_id)
                except Exception:
                    pass
                return
            try:
                self.logger.info(
                    f"Event value node={node_id} cc={val.get('commandClass')} ep={val.get('endpoint')} prop={val.get('property')} key={val.get('propertyKey')} val={val.get('value')}"
                )
            except Exception:
                pass
            state = {
                'nodeId': node_id,
                'name': getattr(node, 'name', '') or '',
                'productLabel': getattr(node, 'product_label', '') or '',
                'status': getattr(node, 'status', None) if node is not None else None,
                'values': [val],
                'partial': True,
            }
            # Discovery: if ZwaveDevice pairing is active, lock onto first useful node and adopt
            try:
                disc = self.gateway_instance.discovery or {}
                if disc and not disc.get('finished'):
                    from simo_zwave.controllers import ZwaveDevice  # type: ignore
                    if disc.get('controller_uid') == ZwaveDevice.uid:
                        if val.get('commandClass') is not None and (val.get('property') is not None or val.get('propertyName') is not None):
                            if disc.get('locked_node') is None:
                                disc['locked_node'] = int(node_id)
                                self.gateway_instance.discovery = disc
                                try:
                                    self.gateway_instance.save(update_fields=['discovery'])
                                except Exception:
                                    pass
                            if int(disc.get('locked_node') or -1) == int(node_id):
                                hint = {
                                    'cc': val.get('commandClass'),
                                    'endpoint': val.get('endpoint') or 0,
                                    'property': val.get('property'),
                                    'propertyKey': val.get('propertyKey'),
                                }
                                asyncio.run_coroutine_threadsafe(asyncio.to_thread(self._adopt_from_node, int(node_id), hint), self._loop)
            except Exception:
                pass
            # Fast-path: push via config routing (fetch Components in a thread)
            try:
                cc = val.get('commandClass')
                ep = val.get('endpoint') or 0
                prop = val.get('property')
                pkey = val.get('propertyKey')
                ev_value = val.get('value')
                if cc is not None and prop is not None:
                    comp_ids = self._get_component_ids_for_value(node_id, cc, ep, prop, pkey)
                    if comp_ids:
                        out_val = ev_value
                        if cc == 48 and isinstance(out_val, (int, float)):
                            out_val = bool(int(out_val))
                        if cc == 113:
                            if isinstance(out_val, str):
                                out_val = str(out_val).strip().lower() not in ('idle', 'inactive', 'clear', 'unknown', 'no event')
                            elif isinstance(out_val, (int, float)):
                                out_val = bool(int(out_val))
                        def _push_to_components(ids, value):
                            for comp in Component.objects.filter(id__in=ids):
                                try:
                                    if self._should_push(comp.id, value):
                                        comp.controller._receive_from_device(value, is_alive=True)
                                        self._mark_pushed(comp.id, value)
                                except Exception:
                                    try:
                                        self.logger.error(
                                            f"Fast-path event push failed comp={getattr(comp,'id',None)} node={node_id} cc={cc} ep={ep} prop={prop}",
                                            exc_info=True,
                                        )
                                    except Exception:
                                        pass
                        asyncio.run_coroutine_threadsafe(asyncio.to_thread(_push_to_components, comp_ids, out_val), self._loop)
            except Exception:
                # Do not block event processing
                pass
            # No DB import; config-first route already pushed
        except Exception:
            self.logger.error("Unhandled exception in value event", exc_info=True)

    def _on_node_status_event(self, event, node=None):
        try:
            # Normalize event
            data = event
            if hasattr(event, 'data') and isinstance(event.data, dict):
                data = event.data
            etype = str((data.get('event') or '')).lower()
            is_alive = etype != 'dead'
            node_id = getattr(node, 'node_id', None) or data.get('nodeId')
            # Propagate availability to config-bound components (no DB usage) in a thread
            try:
                comp_ids = list(self._node_to_components.get(int(node_id), []) or [])
                if comp_ids:
                    def _prop(ids, alive):
                        for comp in Component.objects.filter(id__in=ids):
                            try:
                                comp.controller._receive_from_device(comp.value, is_alive=alive)
                            except Exception:
                                try:
                                    self.logger.error(
                                        f"Failed to propagate availability to component {getattr(comp,'id',None)} for node {node_id}",
                                        exc_info=True,
                                    )
                                except Exception:
                                    pass
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(_prop, comp_ids, is_alive), self._loop)
            except Exception:
                self.logger.error("Failed availability propagation sweep", exc_info=True)
            if etype in ('wake up', 'alive') and node_id:
                # On wake-up, proactively poll bound values for this node
                try:
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(self._poll_node_bound_values, node_id), self._loop)
                except Exception:
                    self.logger.error(f"Wake-up follow-up poll failed node={node_id}", exc_info=True)
                # Discovery: adopt sleepy devices on wake-up if pairing is active
                try:
                    disc = self.gateway_instance.discovery or {}
                    if disc and not disc.get('finished'):
                        from simo_zwave.controllers import ZwaveDevice  # type: ignore
                        if disc.get('controller_uid') == ZwaveDevice.uid:
                            if disc.get('locked_node') is None:
                                disc['locked_node'] = int(node_id)
                                self.gateway_instance.discovery = disc
                                try:
                                    self.gateway_instance.save(update_fields=['discovery'])
                                except Exception:
                                    pass
                            if int(disc.get('locked_node') or -1) == int(node_id):
                                asyncio.run_coroutine_threadsafe(asyncio.to_thread(self._adopt_from_node, int(node_id), {}), self._loop)
                except Exception:
                    pass
        except Exception:
            self.logger.error("Unhandled exception in node status event", exc_info=True)

    

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
            self.logger.error("UFW expiry check failed", exc_info=True)

    

    def sync_bound_values(self):
        """Poll current values for config-bound components as a backup only."""
        try:
            if not (self._client and self._client.connected):
                return
            # Poll config-bound components (new route)
            try:
                from simo.core.models import Component as _C
                comps = list(_C.objects.filter(gateway=self.gateway_instance, config__has_key='zwave')[:128])
                for comp in comps:
                    try:
                        zw = (comp.config or {}).get('zwave') or {}
                        vid = self._build_value_id_from_config(zw)
                        if not vid.get('commandClass') or vid.get('property') is None:
                            continue
                        resp = self._async_call(self._client.async_send_command({
                            'command': 'node.get_value',
                            'nodeId': zw.get('nodeId'),
                            'valueId': vid,
                        }), timeout=10)
                        cur = resp.get('value', resp.get('result')) if isinstance(resp, dict) else resp
                        out_val = cur
                        cc_here = zw.get('cc')
                        if cc_here == 48 and isinstance(out_val, (int, float)):
                            out_val = bool(int(out_val))
                        if cc_here == 113:
                            if isinstance(out_val, str):
                                out_val = str(out_val).strip().lower() not in ('idle', 'inactive', 'clear', 'unknown', 'no event')
                            elif isinstance(out_val, (int, float)):
                                out_val = bool(int(out_val))
                        if self._should_push(comp.id, out_val):
                            comp.controller._receive_from_device(out_val, is_alive=True)
                            self._mark_pushed(comp.id, out_val)
                    except Exception:
                        continue
            except Exception:
                self.logger.error("Config-bound poll failed", exc_info=True)
        except Exception:
            self.logger.error("Bound values poll failed", exc_info=True)

    def _poll_node_bound_values(self, node_id: int):
        """Poll all config-bound values for a specific node immediately."""
        try:
            # Also poll config-bound components for this node
            try:
                from simo.core.models import Component as _C
                comps = list(_C.objects.filter(gateway=self.gateway_instance, config__has_key='zwave', config__zwave__nodeId=node_id))
                for comp in comps:
                    try:
                        zw = (comp.config or {}).get('zwave') or {}
                        vid = self._build_value_id_from_config(zw)
                        if not vid.get('commandClass') or not vid.get('property'):
                            continue
                        resp = self._async_call(self._client.async_send_command({
                            'command': 'node.get_value',
                            'nodeId': node_id,
                            'valueId': vid,
                        }), timeout=10)
                        cur = resp.get('value', resp.get('result')) if isinstance(resp, dict) else resp
                        out_val = cur
                        cc_here = zw.get('cc')
                        if cc_here == 48 and isinstance(out_val, (int, float)):
                            out_val = bool(int(out_val))
                        if cc_here == 113:
                            if isinstance(out_val, str):
                                out_val = str(out_val).strip().lower() not in ('idle', 'inactive', 'clear', 'unknown', 'no event')
                            elif isinstance(out_val, (int, float)):
                                out_val = bool(int(out_val))
                        if self._should_push(comp.id, out_val):
                            comp.controller._receive_from_device(out_val, is_alive=True)
                            self._mark_pushed(comp.id, out_val)
                    except Exception:
                        continue
            except Exception:
                self.logger.error("Config-bound per-node poll failed", exc_info=True)
        except Exception:
            self.logger.error(f"_poll_node_bound_values failed node={node_id}", exc_info=True)

    # ---------- Config routing helpers ----------
    def _rebuild_config_map(self):
        from simo.core.models import Component as _C
        vmap = {}
        nodemap = {}
        for row in _C.objects.filter(gateway=self.gateway_instance).values('id', 'config'):
            cfg = row.get('config') or {}
            zw = cfg.get('zwave') or None
            if not zw:
                continue
            node_id = zw.get('nodeId')
            cc = zw.get('cc')
            ep = zw.get('endpoint') or 0
            prop = zw.get('property')
            pkey = zw.get('propertyKey') or None
            if node_id is None or cc is None or prop is None:
                continue
            key = (int(node_id), int(cc), int(ep), str(prop), str(pkey) if pkey is not None else None)
            vmap.setdefault(key, []).append(row['id'])
            nodemap.setdefault(int(node_id), []).append(row['id'])
        self._value_map = vmap
        self._node_to_components = nodemap

    def _get_component_ids_for_value(self, node_id: int, cc: int, ep: int, prop: Any, pkey: Any):
        key = (int(node_id), int(cc), int(ep), str(prop), str(pkey) if pkey is not None else None)
        ids = self._value_map.get(key, [])
        # For switches/dimmers, map currentValue to targetValue on same endpoint
        if not ids and cc in (37, 38) and str(prop) == 'currentValue':
            key2 = (int(node_id), int(cc), int(ep), 'targetValue', None)
            ids = self._value_map.get(key2, [])
        # For Basic CC events, try to map to Binary/Multilevel Switch bindings
        if not ids and cc == 32:
            # Prefer Binary Switch
            for cc2 in (37, 38):
                for prop2 in ('targetValue', 'currentValue'):
                    keyx = (int(node_id), int(cc2), int(ep), prop2, None)
                    ids = self._value_map.get(keyx, [])
                    if ids:
                        break
                if ids:
                    break
        return ids or []

    def ping_dead_nodes(self):
        """Periodically ping nodes marked dead by the driver to bring them alive.

        Uses only config-bound nodes to limit scope; no DB objects.
        """
        try:
            if not (self._client and getattr(self._client, 'driver', None) and self._client.connected):
                return
            try:
                nodes_map = getattr(self._client.driver.controller, 'nodes', {}) or {}
            except Exception:
                nodes_map = {}
            # Only consider nodes referenced by components in this gateway
            candidate_ids = set(self._node_to_components.keys())
            now = time.time()
            for nid in candidate_ids:
                try:
                    node = nodes_map.get(nid)
                    status = getattr(node, 'status', None)
                    is_dead = (status == 3)  # NodeStatus.Dead
                    if not is_dead:
                        continue
                    last = self._last_dead_ping.get(nid, 0)
                    if now - last < 9:
                        continue
                    self._last_dead_ping[nid] = now
                    try:
                        self.logger.info(f"Pinging dead node {nid}")
                    except Exception:
                        pass
                    resp = self._async_call(self._client.async_send_command({
                        'command': 'node.ping',
                        'nodeId': nid,
                    }), timeout=10)
                    responded = None
                    if isinstance(resp, dict):
                        responded = resp.get('responded', resp.get('result'))
                    elif isinstance(resp, bool):
                        responded = resp
                    if responded:
                        # Optimistically mark comps alive while awaiting events
                        comp_ids = self._node_to_components.get(nid, []) or []
                        for comp in Component.objects.filter(id__in=comp_ids):
                            try:
                                comp.controller._receive_from_device(comp.value, is_alive=True)
                            except Exception:
                                pass
                except Exception as e:
                    if 'node_not_found' in str(e).lower():
                        try:
                            self.logger.info(f"Skip ping node={nid} (node_not_found)")
                        except Exception:
                            pass
                        continue
                    self.logger.error(f"Dead node ping failed node={nid}", exc_info=True)
        except Exception:
            self.logger.error("ping_dead_nodes sweep failed", exc_info=True)


    def sync_node_labels(self):
        """Synchronize Z-Wave node name/location from bound SIMO components."""
        try:
            if not (self._client and self._client.connected):
                return
            # Ensure routing map is up to date
            try:
                self._rebuild_config_map()
            except Exception:
                pass
            # Access driver nodes if available for current labels
            try:
                nodes_map = getattr(self._client.driver.controller, 'nodes', {}) or {}
            except Exception:
                nodes_map = {}
            from simo.core.models import Component as _C
            for nid, comp_ids in list((self._node_to_components or {}).items()):
                try:
                    comps = list(_C.objects.filter(id__in=comp_ids).select_related('zone').only('id', 'name', 'zone'))
                    # Build unique, ordered name and location lists
                    def _uniq(seq):
                        seen = set(); out = []
                        for s in seq:
                            if not s:
                                continue
                            if s not in seen:
                                seen.add(s); out.append(s)
                        return out
                    names = _uniq([str(getattr(c, 'name', '')).strip() for c in comps])
                    zones = _uniq([str(getattr(getattr(c, 'zone', None), 'name', '')).strip() for c in comps])
                    desired_name = ', '.join(names) if names else ''
                    desired_loc = ', '.join(zones) if zones else ''
                    last = self._last_node_labels.get(nid)
                    # Compare with driver model if available
                    try:
                        node = nodes_map.get(nid)
                        current_name = (getattr(node, 'name', None) or '').strip()
                        current_loc = (getattr(node, 'location', None) or '').strip()
                    except Exception:
                        current_name = ''
                        current_loc = ''
                    # Only send when changed compared to both cache and driver
                    want_set_name = bool(desired_name) and desired_name != current_name
                    want_set_loc = bool(desired_loc) and desired_loc != current_loc
                    if last and not want_set_name and not want_set_loc:
                        continue
                    # Send updates
                    if want_set_name:
                        try:
                            self._async_call(self._client.async_send_command({
                                'command': 'node.set_name',
                                'nodeId': nid,
                                'name': desired_name,
                                'updateCC': True,
                            }), timeout=10)
                        except Exception:
                            self.logger.error(f"Failed to set node name nid={nid}", exc_info=True)
                    if want_set_loc:
                        try:
                            self._async_call(self._client.async_send_command({
                                'command': 'node.set_location',
                                'nodeId': nid,
                                'location': desired_loc,
                                'updateCC': True,
                            }), timeout=10)
                        except Exception:
                            self.logger.error(f"Failed to set node location nid={nid}", exc_info=True)
                    # Update cache if we attempted any change
                    if want_set_name or want_set_loc:
                        self._last_node_labels[nid] = (desired_name, desired_loc)
                except Exception:
                    self.logger.error(f"sync_node_labels failed nid={nid}", exc_info=True)
        except Exception:
            self.logger.error("sync_node_labels sweep failed", exc_info=True)


    # --------------- MQTT commands ---------------
    def perform_value_send(self, component, value):
        # If WS is not connected yet, skip with a concise log
        if not self._client or not self._client.connected:
            try:
                self.logger.info("WS not connected; skipping send")
            except Exception:
                pass
            return
        cfg = component.config or {}
        zwcfg = cfg.get('zwave') or None
        if not zwcfg:
            try:
                self.logger.error(f"Missing config.zwave for comp={component.id}; cannot send")
            except Exception:
                pass
            return
        try:
            try:
                self.logger.info(f"Send comp={component.id} '{component.name}' cfg zwave raw={value}")
            except Exception:
                pass
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
            addr = {
                'node_id': zwcfg.get('nodeId'),
                'cc': zwcfg.get('cc'),
                'endpoint': zwcfg.get('endpoint') or 0,
                'property': zwcfg.get('property'),
                'property_key': zwcfg.get('propertyKey'),
                'label': component.name,
                'comp_id': component.id,
            }
            # If cc/property missing, we cannot send
            if not addr['cc'] or addr.get('property') is None:
                try:
                    self.logger.error(f"Incomplete zwave addr for comp={component.id}; aborting send")
                except Exception:
                    pass
                return
            try:
                self.logger.info(
                    f"Addr node={addr['node_id']} cc={addr['cc']} ep={addr['endpoint']} prop={addr['property']} key={addr['property_key']}"
                )
            except Exception:
                pass
            self._async_call(self._set_value(addr, value))
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
        # Map controller commands to server API
        mapping = {
            'add_node': {'command': 'controller.begin_inclusion'},
            'remove_node': {'command': 'controller.begin_exclusion'},
            'stop_inclusion': {'command': 'controller.stop_inclusion'},
            'stop_exclusion': {'command': 'controller.stop_exclusion'},
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
                await self._client.async_send_command({'command': 'controller.remove_failed_node', 'nodeId': node_id})
            elif cmd == 'replace_failed_node':
                await self._client.async_send_command({'command': 'controller.replace_failed_node', 'nodeId': node_id})

    # --------------- WS helpers ---------------
    def _async_call(self, coro, timeout: int = 15):
        if not self._loop:
            raise RuntimeError('WS loop not started')
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def _build_ws_url(self) -> str:
        return 'ws://127.0.0.1:3000'

    

    def _build_value_id_from_config(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        def _coerce(val: Any) -> Any:
            if isinstance(val, str) and val.isdigit():
                try:
                    return int(val)
                except Exception:
                    return val
            return val
        vid: Dict[str, Any] = {
            'commandClass': cfg.get('cc'),
            'endpoint': cfg.get('endpoint') or 0,
            'property': cfg.get('property'),
        }
        pk = cfg.get('propertyKey')
        if pk not in (None, ''):
            vid['propertyKey'] = _coerce(pk)
        return vid

    async def _resolve_value_id_async(self, node_id: int, cc: Optional[int], endpoint: Optional[int], prop: Optional[Any], prop_key: Optional[Any], label: Optional[str], desired_value: Any = None) -> Optional[Dict[str, Any]]:
        """Ask server for defined value IDs and pick the best writable match.

        Strategy:
        - Prefer same commandClass and endpoint.
        - If CC is Binary/Multilevel Switch (37/38), prefer property 'targetValue'.
        - Otherwise, try matching our current property/propertyKey or propertyName == label.
        Returns a valueId dict or None.
        """
        try:
            resp = await self._client.async_send_command({'command': 'node.get_defined_value_ids', 'nodeId': node_id})
            try:
                cnt = (resp.get('valueIds') if isinstance(resp, dict) else [])
                cnt = len(cnt) if isinstance(cnt, list) else 0
                self.logger.info(f"Resolver: server returned {cnt} valueIds for node {node_id}")
            except Exception:
                pass
        except Exception:
            self.logger.error(f"Resolver: get_defined_value_ids failed for node {node_id}", exc_info=True)
            resp = None

        items = resp
        if isinstance(resp, dict):
            items = resp.get('valueIds') or resp.get('result') or []
        if not isinstance(items, list):
            items = []

        def getf(item, key, fallback=None):
            if isinstance(item, dict):
                return item.get(key, fallback)
            # try attribute style
            attr = key
            # translate camelCase to snake_case for common fields
            trans = {
                'commandClass': 'command_class',
                'propertyKey': 'property_key',
                'propertyName': 'property_name',
            }
            attr = trans.get(key, key)
            return getattr(item, attr, fallback)

        # Optionally fetch metadata for scoring
        async def get_meta(item) -> Dict[str, Any]:
            try:
                val_id = {
                    'commandClass': getf(item, 'commandClass'),
                    'endpoint': getf(item, 'endpoint') or 0,
                    'property': getf(item, 'property'),
                }
                pk = getf(item, 'propertyKey')
                if pk is not None:
                    val_id['propertyKey'] = pk
                meta_resp = await self._client.async_send_command({'command': 'node.get_value_metadata', 'nodeId': node_id, 'valueId': val_id})
                if isinstance(meta_resp, dict):
                    # Some servers may return directly, others nested
                    md = meta_resp.get('metadata') or meta_resp.get('result') or meta_resp
                    if isinstance(md, dict):
                        return md
                return {}
            except Exception:
                self.logger.error(f"Resolver: get_value_metadata failed for node {node_id}", exc_info=True)
                return {}

        # Determine expected type
        expected_type = None
        if isinstance(desired_value, bool):
            expected_type = 'boolean'
        elif isinstance(desired_value, (int, float)):
            expected_type = 'number'

        meta_cache: Dict[int, Dict[str, Any]] = {}
        # If server returned nothing, fall back to driver model values
        if not items and getattr(self._client, 'driver', None):
            try:
                node = self._client.driver.controller.nodes.get(node_id)
            except Exception:
                node = None
            if node and getattr(node, 'values', None):
                for v in node.values.values():
                    try:
                        item = {
                            'commandClass': getattr(v, 'command_class', None),
                            'endpoint': getattr(v, 'endpoint', 0) or 0,
                            'property': getattr(v, 'property_', None),
                            'propertyKey': getattr(v, 'property_key', None),
                            'propertyName': getattr(v, 'property_name', None),
                        }
                        items.append(item)
                        meta_cache[id(item)] = {
                            'label': getattr(getattr(v, 'metadata', None), 'label', None),
                            'unit': getattr(getattr(v, 'metadata', None), 'unit', ''),
                            'writeable': getattr(getattr(v, 'metadata', None), 'writeable', False),
                            'type': getattr(getattr(v, 'metadata', None), 'type', ''),
                            'states': getattr(getattr(v, 'metadata', None), 'states', None) or [],
                        }
                    except Exception:
                        continue
                try:
                    self.logger.info(f"Resolver: driver fallback yielded {len(items)} valueIds for node {node_id}")
                except Exception:
                    pass

        # Preload metadata for candidates with matching CC/endpoint only (limit scope)
        filtered = [i for i in items if getf(i, 'commandClass') == cc and (getf(i, 'endpoint') or 0) == (endpoint or 0)]
        if not filtered:
            filtered = items
        # Limit to reasonable number to avoid heavy calls
        limited = filtered[:30]
        # Fetch metadata concurrently for those we don't already have
        to_fetch = [i for i in limited if id(i) not in meta_cache]
        try:
            metas = await asyncio.gather(*[get_meta(i) for i in to_fetch])
            for idx, md in enumerate(metas):
                meta_cache[id(to_fetch[idx])] = md
        except Exception:
            self.logger.error("Resolver: metadata prefetch failed", exc_info=True)

        def score(item) -> int:
            s = 0
            if getf(item, 'commandClass') == cc:
                s += 5
            if (getf(item, 'endpoint') or 0) == (endpoint or 0):
                s += 3
            prop_i = getf(item, 'property')
            pname = getf(item, 'propertyName')
            # Switch/dimmer preference
            if cc in (37, 38) and prop_i == 'targetValue':
                s += 5
            if (prop is not None and prop_i == prop) or (prop is not None and pname == prop):
                s += 2
            if prop_key not in (None, '') and getf(item, 'propertyKey') == prop_key:
                s += 1
            if pname and label and str(pname).lower() == str(label).lower():
                s += 1
            # Normalized label matching boosts
            try:
                norm_label = self._normalize_label(label)
                norm_pname = self._normalize_label(pname)
                if norm_label and norm_pname and norm_label == norm_pname:
                    s += 3
                # For sensor synonyms with different property names (Air temperature, Illuminance)
                if norm_label == 'temperature' and self._normalize_label(prop_i) in ('temperature', 'air temperature'):
                    s += 2
                if norm_label == 'luminance' and self._normalize_label(prop_i) in ('luminance', 'illuminance', 'lux'):
                    s += 2
                if norm_label == 'humidity' and self._normalize_label(prop_i) in ('humidity', 'relative humidity'):
                    s += 2
                if norm_label == 'motion' and getf(item, 'commandClass') in (48, 113):
                    s += 2
            except Exception:
                pass
            # writable/read-only preference: prefer writeable only for switches/dimmers
            meta = meta_cache.get(id(item), {})
            is_writeable = isinstance(meta, dict) and meta.get('writeable')
            if cc in (37, 38):
                if is_writeable:
                    s += 2
            else:
                if is_writeable:
                    s -= 2
                else:
                    s += 2
            # expected type preference
            if expected_type and isinstance(meta, dict) and meta.get('type') == expected_type:
                s += 1
            # penalize clearly wrong Basic helpers for sensors
            if cc not in (37, 38) and getf(item, 'commandClass') == 32:
                # 'Basic' should not be preferred for sensors
                s -= 3
            # prefer currentValue for reads in non-switch contexts
            if cc not in (37, 38) and prop_i == 'currentValue':
                s += 2
            # de-prioritize 'restorePrevious'
            if str(prop_i) == 'restorePrevious':
                s -= 4
            return s

        candidates = [i for i in items if isinstance(i, (dict, object))]
        if not candidates:
            return None
        candidates.sort(key=score, reverse=True)
        best = candidates[0]
        try:
            self.logger.info(
                f"Resolver: best match node={node_id} CC={getf(best,'commandClass')} ep={getf(best,'endpoint') or 0} prop={getf(best,'property')} pname={getf(best,'propertyName')}"
            )
        except Exception:
            pass
        vid = {
            'commandClass': getf(best, 'commandClass'),
            'endpoint': getf(best, 'endpoint') or 0,
            'property': getf(best, 'property'),
        }
        pk = getf(best, 'propertyKey')
        if pk is not None:
            vid['propertyKey'] = pk
        return vid

    async def _set_value(self, addr: Dict[str, Any], value):
        if not self._client or not self._client.connected:
            raise RuntimeError('Z-Wave JS not connected')
        node_id = addr['node_id']
        cc = addr.get('cc')
        endpoint = addr.get('endpoint') or 0
        prop = addr.get('property')
        prop_key = addr.get('property_key')
        label = addr.get('label')
        comp_id = addr.get('comp_id')
        try:
            if cc == 38:
                if isinstance(value, bool):
                    value = 99 if value else 0
                if isinstance(value, (int, float)):
                    value = max(0, min(int(value), 99))
            elif cc == 37:
                if isinstance(value, (int, float)):
                    value = bool(value)
        except Exception:
            pass
        # If address is incomplete, try to resolve before sending
        if not cc or not prop:
            resolved = await self._resolve_value_id_async(node_id, cc, endpoint, prop, prop_key, label, value)
            if resolved:
                await self._client.async_send_command({
                    'command': 'node.set_value',
                    'nodeId': node_id,
                    'valueId': resolved,
                    'value': value,
                })
                # Persist resolved addressing for future sends into Component.config
                try:
                    if comp_id:
                        def _persist_cfg(cid, res):
                            comp = Component.objects.filter(pk=cid).first()
                            if not comp:
                                return
                            cfg = comp.config or {}
                            zw = cfg.get('zwave') or {}
                            zw['cc'] = res.get('commandClass', zw.get('cc'))
                            zw['endpoint'] = res.get('endpoint', zw.get('endpoint'))
                            zw['property'] = res.get('property', zw.get('property'))
                            if 'propertyKey' in res:
                                zw['propertyKey'] = res.get('propertyKey')
                            cfg['zwave'] = zw
                            comp.config = cfg
                            comp.save(update_fields=['config'])
                        await asyncio.to_thread(_persist_cfg, comp_id, resolved)
                except Exception:
                    pass
                return
            # Could not resolve a valid valueId; skip sending to avoid ZW0322
            try:
                self.logger.info(f"Skip send: unresolved ValueID for node={node_id} (cc={cc}, ep={endpoint}, prop={prop}, key={prop_key})")
            except Exception:
                pass
            # Try to trigger a values refresh once in a while to aid future resolution
            try:
                now = time.time()
                last = self._last_node_refresh.get(node_id, 0)
                if now - last > 300:
                    await self._client.async_send_command({'command': 'node.refresh_values', 'nodeId': node_id})
                    self._last_node_refresh[node_id] = now
            except Exception:
                self.logger.error(f"Failed to refresh node {node_id} values", exc_info=True)
            return
        # Build ValueID from address (config-based). For switches/dimmers, writes go to targetValue
        write_prop = prop
        try:
            if cc in (37, 38) and prop == 'currentValue':
                write_prop = 'targetValue'
        except Exception:
            pass
        value_id = self._build_value_id_from_config({'cc': cc, 'endpoint': endpoint, 'property': write_prop, 'propertyKey': prop_key})
        log_prop = value_id.get('property') if isinstance(value_id, dict) else prop
        try:
            self.logger.info(f"Set start node={node_id} cc={cc} ep={endpoint} prop={log_prop} key={prop_key} value={value}")
            res = await self._client.async_send_command({
                'command': 'node.set_value',
                'nodeId': node_id,
                'valueId': value_id,
                'value': value,
            })
            try:
                self.logger.info(f"Set result node={node_id}: {res}")
            except Exception:
                pass
            # No post-send verification here; rely purely on events
        except Exception as e:
            # Try to resolve to a valid valueId if invalid, then retry once
            msg = str(e)
            if 'Invalid ValueID' in msg or 'ZW0322' in msg or 'zwave_error' in msg:
                resolved = await self._resolve_value_id_async(node_id, cc, endpoint, prop, prop_key, label, value)
                if resolved:
                    self.logger.info(f"Retry with resolved valueId node={node_id} {resolved}")
                    res2 = await self._client.async_send_command({
                        'command': 'node.set_value',
                        'nodeId': node_id,
                        'valueId': resolved,
                        'value': value,
                    })
                    try:
                        self.logger.info(f"Set resolved result node={node_id}: {res2}")
                    except Exception:
                        pass
                    # Persist resolved addressing for future sends into Component.config
                    try:
                        if comp_id:
                            def _persist_cfg(cid, res):
                                comp = Component.objects.filter(pk=cid).first()
                                if not comp:
                                    return
                                cfg = comp.config or {}
                                zw = cfg.get('zwave') or {}
                                zw['cc'] = res.get('commandClass', zw.get('cc'))
                                zw['endpoint'] = res.get('endpoint', zw.get('endpoint'))
                                zw['property'] = res.get('property', zw.get('property'))
                                if 'propertyKey' in res:
                                    zw['propertyKey'] = res.get('propertyKey')
                                cfg['zwave'] = zw
                                comp.config = cfg
                                comp.save(update_fields=['config'])
                            await asyncio.to_thread(_persist_cfg, comp_id, resolved)
                    except Exception:
                        pass
                    return
                # As a last resort for switches, call CC API directly
                try:
                    if cc in (37, 38):
                        self.logger.info(f"Fallback invoke_cc_api set node={node_id} cc={cc} ep={endpoint} value={value}")
                        await self._client.async_send_command({
                            'command': 'endpoint.invoke_cc_api',
                            'nodeId': node_id,
                            'endpoint': endpoint,
                            'commandClass': cc,
                            'methodName': 'set',
                            'args': [value],
                        })
                        return
                except Exception:
                    pass
            # No support for old API; re-raise
            raise

    async def _import_driver_state(self):
        """Initial sync: poll all config-bound component values and propagate availability.

        We no longer import/save per-value DB state; this only serves to prime
        component values and availability after connection.
        """
        try:
            # Rebuild routing map and poll bound components in threads (avoid async ORM)
            import asyncio as _asyncio
            await _asyncio.to_thread(self._rebuild_config_map)
            await _asyncio.to_thread(self._poll_all_bound_values)
            # Propagate availability from driver to components
            if getattr(self._client, 'driver', None):
                nodes_map = getattr(self._client.driver.controller, 'nodes', {}) or {}
                for nid, comp_ids in (self._node_to_components or {}).items():
                    try:
                        node = nodes_map.get(nid)
                        status = getattr(node, 'status', None)
                        is_alive = status != 3
                        def _push(ids, alive):
                            for comp in Component.objects.filter(id__in=ids):
                                try:
                                    comp.controller._receive_from_device(comp.value, is_alive=alive)
                                except Exception:
                                    pass
                        await _asyncio.to_thread(_push, comp_ids, is_alive)
                    except Exception:
                        continue
        except Exception:
            self.logger.error("Initial driver sync failed", exc_info=True)

    def _poll_all_bound_values(self):
        try:
            from simo.core.models import Component as _C
            comps = list(_C.objects.filter(gateway=self.gateway_instance, config__has_key='zwave')[:256])
            for comp in comps:
                try:
                    zw = (comp.config or {}).get('zwave') or {}
                    vid = self._build_value_id_from_config(zw)
                    if not vid.get('commandClass') or vid.get('property') is None:
                        continue
                    resp = self._async_call(self._client.async_send_command({
                        'command': 'node.get_value',
                        'nodeId': zw.get('nodeId'),
                        'valueId': vid,
                    }), timeout=10)
                    cur = resp.get('value', resp.get('result')) if isinstance(resp, dict) else resp
                    out_val = cur
                    cc_here = zw.get('cc')
                    if cc_here == 48 and isinstance(out_val, (int, float)):
                        out_val = bool(int(out_val))
                    if cc_here == 113:
                        if isinstance(out_val, str):
                            out_val = str(out_val).strip().lower() not in ('idle', 'inactive', 'clear', 'unknown', 'no event')
                        elif isinstance(out_val, (int, float)):
                            out_val = bool(int(out_val))
                    # Dedup short-window to avoid duplicate history entries
                    if not self._should_push(comp.id, out_val):
                        continue
                    comp.controller._receive_from_device(out_val, is_alive=True)
                    self._mark_pushed(comp.id, out_val)
                except Exception:
                    continue
        except Exception:
            self.logger.error("All-bound poll failed", exc_info=True)

    # --------------- Adopt/Discovery helpers ---------------
    def _adopt_from_node(self, node_id: int, hint: Dict[str, Any]):
        """Create missing SIMO components for a node during discovery.

        - Creates missing Switch/Dimmer/RGBW components for actuator endpoints.
        - If no actuators are created, uses the event hint to create one sensor/button.
        - Appends created component ids to discovery results and finishes discovery.
        """
        try:
            disc = self.gateway_instance.discovery or {}
            if not disc or disc.get('finished'):
                return
            if disc.get('locked_node') is not None and int(disc['locked_node']) != int(node_id):
                return
            if not (self._client and self._client.connected):
                return

            # Fetch defined value IDs
            try:
                resp = self._async_call(self._client.async_send_command({
                    'command': 'node.get_defined_value_ids',
                    'nodeId': int(node_id),
                }), timeout=10)
                items = resp.get('valueIds') if isinstance(resp, dict) else resp
                if not isinstance(items, list):
                    items = []
            except Exception:
                items = []

            def gi(it, key, default=None):
                if isinstance(it, dict):
                    return it.get(key, default)
                return getattr(it, key, default)

            # Group by endpoint
            eps: Dict[int, list] = {}
            for it in items:
                ep = gi(it, 'endpoint') or 0
                eps.setdefault(int(ep), []).append(it)

            # Determine actuator endpoints 51 > 38 > 37
            actuator_eps: List[Tuple[int, int, Any, Any]] = []  # (ep, cc, property, pkey)
            for ep, elist in eps.items():
                for cc in (51, 38, 37):
                    cand = None
                    for it in elist:
                        if int(gi(it, 'commandClass') or 0) != cc:
                            continue
                        prop = gi(it, 'property')
                        pkey = gi(it, 'propertyKey')
                        if str(prop) == 'targetValue':
                            cand = (ep, cc, prop, pkey)
                            break
                        if not cand and str(prop) == 'currentValue':
                            cand = (ep, cc, prop, pkey)
                    if cand:
                        actuator_eps.append(cand)
                        break

            created_ids: List[int] = []

            # Initial form data (zone/category/name)
            from simo.core.utils.serialization import deserialize_form_data
            try:
                started_with = deserialize_form_data(disc.get('init_data') or {})
            except Exception:
                started_with = {}
            zone = started_with.get('zone')
            category = started_with.get('category')
            base_name = (started_with.get('name') or '').strip()

            from simo_zwave.controllers import (
                ZwaveSwitch, ZwaveDimmer, ZwaveRGBWLight,
                ZwaveBinarySensor, ZwaveNumericSensor, ZwaveButton,
            )

            def ctrl_for_cc(cc: int):
                if cc == 37:
                    return ZwaveSwitch
                if cc == 38:
                    return ZwaveDimmer
                if cc == 51:
                    return ZwaveRGBWLight
                return None

            # Create missing actuator components
            for ep, cc, prop, pkey in actuator_eps:
                try:
                    exists = Component.objects.filter(
                        gateway=self.gateway_instance,
                        config__zwave__nodeId=int(node_id),
                        config__zwave__endpoint=int(ep),
                        config__zwave__cc=int(cc),
                        config__zwave__property='targetValue' if str(prop) == 'currentValue' else str(prop),
                    ).exists()
                    if exists:
                        continue
                    ctrl_cls = ctrl_for_cc(int(cc))
                    if not ctrl_cls:
                        continue
                    ctrl_uid = ctrl_cls.uid
                    bt = getattr(ctrl_cls, 'base_type', None)
                    bt_slug = bt if isinstance(bt, str) else getattr(bt, 'slug', None)
                    store_prop = 'targetValue' if str(prop) == 'currentValue' else str(prop)
                    cfg = {
                        'zwave': {
                            'nodeId': int(node_id),
                            'cc': int(cc),
                            'endpoint': int(ep),
                            'property': store_prop,
                        }
                    }
                    if pkey not in (None, ''):
                        cfg['zwave']['propertyKey'] = pkey
                    name = base_name or f"Z-Wave {int(node_id)}"
                    if len(eps) > 1:
                        name = f"{name} EP{int(ep)}"
                    comp = Component(
                        name=name,
                        zone=zone or None,
                        category=category or None,
                        gateway=self.gateway_instance,
                        controller_uid=ctrl_uid,
                        base_type=bt_slug or '',
                        config=cfg,
                    )
                    comp.save()
                    try:
                        comp.value = comp.controller.default_value
                        comp.save(update_fields=['value'])
                    except Exception:
                        pass
                    created_ids.append(comp.id)
                except Exception:
                    self.logger.error("Failed to create actuator component", exc_info=True)

            # If none created, try one sensor/button based on hint
            if not created_ids and hint and isinstance(hint, dict):
                try:
                    cc = int(hint.get('cc')) if hint.get('cc') is not None else None
                except Exception:
                    cc = None
                ep = int(hint.get('endpoint') or 0)
                prop = hint.get('property')
                pkey = hint.get('propertyKey')
                ctrl_cls = None
                if cc in (48, 113):
                    ctrl_cls = ZwaveBinarySensor
                elif cc == 49:
                    ctrl_cls = ZwaveNumericSensor
                elif cc == 91:
                    ctrl_cls = ZwaveButton
                if ctrl_cls:
                    try:
                        ctrl_uid = ctrl_cls.uid
                        bt = getattr(ctrl_cls, 'base_type', None)
                        bt_slug = bt if isinstance(bt, str) else getattr(bt, 'slug', None)
                        cfg = {
                            'zwave': {
                                'nodeId': int(node_id),
                                'cc': int(cc) if cc is not None else None,
                                'endpoint': int(ep),
                                'property': str(prop) if prop is not None else 'currentValue',
                            }
                        }
                        if pkey not in (None, ''):
                            cfg['zwave']['propertyKey'] = pkey
                        name = base_name or f"Z-Wave {int(node_id)}"
                        if len(eps) > 1:
                            name = f"{name} EP{int(ep)}"
                        comp = Component(
                            name=name,
                            zone=zone or None,
                            category=category or None,
                            gateway=self.gateway_instance,
                            controller_uid=ctrl_uid,
                            base_type=bt_slug or '',
                            config=cfg,
                        )
                        comp.save()
                        try:
                            comp.value = comp.controller.default_value
                            comp.save(update_fields=['value'])
                        except Exception:
                            pass
                        created_ids.append(comp.id)
                    except Exception:
                        self.logger.error("Failed to create sensor/button component", exc_info=True)

            if not created_ids:
                return
            # Append to discovery and finish
            try:
                for cid in created_ids:
                    self.gateway_instance.append_discovery_result(cid)
                self.gateway_instance.save(update_fields=['discovery'])
            except Exception:
                pass
            try:
                self.gateway_instance.finish_discovery()
            except Exception:
                pass
            # Stop inclusion if active
            try:
                if self._client and self._client.connected:
                    self._async_call(self._controller_command('stop_inclusion', None), timeout=10)
            except Exception:
                pass
        except Exception:
            self.logger.error("_adopt_from_node failed", exc_info=True)

    # ---------- Dedup helpers ----------
    def _should_push(self, comp_id: int, value: Any) -> bool:
        try:
            info = self._last_push.get(comp_id)
            if info is None:
                return True
            last_val, ts = info
            if last_val != value:
                return True
            # If same value within 2s window, skip as duplicate
            return (time.time() - ts) > 2.0
        except Exception:
            return True

    def _mark_pushed(self, comp_id: int, value: Any):
        try:
            self._last_push[comp_id] = (value, time.time())
        except Exception:
            pass

    

    # End of class
