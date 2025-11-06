import asyncio
import json
import logging
import threading
import time
from typing import Dict, Any, Optional, List, Tuple

import paho.mqtt.client as mqtt
from django.db import models
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
        # Auto-finish discovery when UI stops polling
        ('push_discoveries', 6),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ws_url = self._build_ws_url()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client: Optional[ZJSClient] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = False
        self._last_node_refresh: Dict[int, float] = {}
        self._last_dead_ping: Dict[int, float] = {}
        self._last_push: Dict[int, Any] = {}
        self._last_node_labels: Dict[int, tuple] = {}
        # Config-driven routing caches for Component.config-based mapping
        self._value_map: Dict[tuple, list] = {}
        self._node_to_components: Dict[int, list] = {}
        # Throttles
        self._last_battery_poll: Dict[int, float] = {}
        # Throttles
        self._last_battery_poll: Dict[int, float] = {}
        

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
            try:
                self.logger.info(
                    f"maintain: ws_connected={bool(self._client and self._client.connected)} uid='{uid}' started={bool(disc.get('inclusion_started'))} finished={bool(disc.get('finished'))}"
                )
            except Exception:
                pass
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

    # (Note: consolidated MQTT handler is defined later in the file)

    def _on_value_event(self, event, node=None):
        try:
            # If discovery has been finished externally, ensure inclusion is stopped promptly
            self._ensure_discovery_stopped()
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
                # Battery level updates (CC 128) → propagate to all bound components
                if cc_here == 128:
                    prop = val.get('property') or val.get('propertyName')
                    try:
                        if str(prop).lower() == 'level':
                            level = val.get('value')
                            if isinstance(level, (int, float)):
                                lvl = max(0, min(int(level), 100))
                                asyncio.run_coroutine_threadsafe(asyncio.to_thread(self._propagate_battery_level, int(node_id), lvl), self._loop)
                    except Exception:
                        pass
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
                self.logger.debug(
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
                try:
                    self.gateway_instance.refresh_from_db(fields=['discovery'])
                except Exception:
                    pass
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
                        out_val = self._normalize_cc_value(cc, ev_value)
                        def _push_to_components(ids, value):
                            for comp in Component.objects.filter(id__in=ids):
                                self._push_value(comp, value, node_id=node_id)
                        asyncio.run_coroutine_threadsafe(asyncio.to_thread(_push_to_components, comp_ids, out_val), self._loop)
            except Exception:
                # Do not block event processing
                pass
            # No DB import; config-first route already pushed
        except Exception:
            self.logger.error("Unhandled exception in value event", exc_info=True)

    def _on_node_status_event(self, event, node=None):
        try:
            # If discovery has been finished externally, ensure inclusion is stopped promptly
            self._ensure_discovery_stopped()
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
                    try:
                        self.gateway_instance.refresh_from_db(fields=['discovery'])
                    except Exception:
                        pass
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
                        vid = self._build_value_id_for_read(zw)
                        if not vid.get('commandClass') or vid.get('property') is None:
                            continue
                        try:
                            resp = self._async_call(self._client.async_send_command({
                                'command': 'node.get_value',
                                'nodeId': zw.get('nodeId'),
                                'valueId': vid,
                            }), timeout=10)
                        except Exception:
                            # Even if value read fails (eg node dead), still propagate availability
                            is_alive = self._is_node_alive(zw.get('nodeId'))
                            try:
                                if getattr(comp, 'alive', None) != is_alive:
                                    comp.controller._receive_from_device(comp.value, is_alive=is_alive)
                            except Exception:
                                pass
                            continue
                        cur = resp.get('value', resp.get('result')) if isinstance(resp, dict) else resp
                        out_val = self._normalize_cc_value(zw.get('cc'), cur)
                        self._push_value(comp, out_val, node_id=zw.get('nodeId'))
                    except Exception:
                        continue
            except Exception:
                self.logger.error("Config-bound poll failed", exc_info=True)
        except Exception:
            self.logger.error("Bound values poll failed", exc_info=True)

    def push_discoveries(self):
        """Finish discovery when the UI stops polling.

        The admin UI periodically updates discovery['last_check'] via the
        RunningDiscoveries endpoint. If that heartbeat stops for >10s,
        automatically finish discovery to clean up pairing state.
        """
        try:
            import time as _time
            from simo.core.models import Gateway as _Gateway
            # Consider all gateways of this type
            for gw in _Gateway.objects.filter(type=self.uid, discovery__has_key='start').exclude(discovery__has_key='finished'):
                disc = gw.discovery or {}
                last = disc.get('last_check')
                try:
                    stale = (last is None) or ((_time.time() - float(last)) > 10)
                except Exception:
                    stale = True
                if stale:
                    try:
                        gw.finish_discovery()
                    except Exception:
                        pass
        except Exception:
            try:
                self.logger.error("push_discoveries failed", exc_info=True)
            except Exception:
                pass

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
                        vid = self._build_value_id_for_read(zw)
                        if not vid.get('commandClass') or not vid.get('property'):
                            continue
                        try:
                            resp = self._async_call(self._client.async_send_command({
                                'command': 'node.get_value',
                                'nodeId': node_id,
                                'valueId': vid,
                            }), timeout=10)
                        except Exception:
                            # On read failure, at least propagate availability
                            is_alive = self._is_node_alive(node_id)
                            try:
                                if getattr(comp, 'alive', None) != is_alive:
                                    comp.controller._receive_from_device(comp.value, is_alive=is_alive)
                            except Exception:
                                pass
                            continue
                        cur = resp.get('value', resp.get('result')) if isinstance(resp, dict) else resp
                        out_val = self._normalize_cc_value(zw.get('cc'), cur)
                        self._push_value(comp, out_val, node_id=node_id)
                    except Exception:
                        continue
            except Exception:
                self.logger.error("Config-bound per-node poll failed", exc_info=True)
            # Also try to read battery level for this node
            # Throttled battery poll
            try:
                self._maybe_poll_battery_for_node(int(node_id))
            except Exception:
                pass
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
            if node_id is None:
                continue
            # Always map node -> components to propagate availability even if value binding is incomplete
            nodemap.setdefault(int(node_id), []).append(row['id'])
            # Only map value routing when value binding is complete
            if cc is None or prop is None:
                continue
            key = (int(node_id), int(cc), int(ep), str(prop), str(pkey) if pkey is not None else None)
            vmap.setdefault(key, []).append(row['id'])
        self._value_map = vmap
        self._node_to_components = nodemap

    def _is_node_alive(self, node_id: int) -> bool:
        """Return current availability for a node from the driver model.

        - Prefers the boolean `node.is_alive` when present.
        - Falls back to inspecting `node.status` for 'dead' or enum value 3.
        - If no driver context is available, returns True (don't flap to offline).
        """
        try:
            if not (self._client and getattr(self._client, 'driver', None) and self._client.connected):
                return True
            node = getattr(self._client.driver.controller, 'nodes', {}).get(int(node_id))
            if not node:
                return False
            # Prefer dedicated boolean if exposed by zwave-js-server-python
            if hasattr(node, 'is_alive'):
                try:
                    return bool(getattr(node, 'is_alive'))
                except Exception:
                    pass
            status = getattr(node, 'status', None)
            try:
                s_txt = str(status).lower()
            except Exception:
                s_txt = ''
            try:
                s_int = int(status)
            except Exception:
                s_int = None
            if 'dead' in s_txt:
                return False
            if s_int == 3:  # NodeStatus.Dead
                return False
            return True
        except Exception:
            # On unexpected errors, keep current availability unchanged by returning True
            return True

    def _propagate_battery_level(self, node_id: int, level: int):
        """Update battery_level on all components bound to this node."""
        try:
            comp_ids = list(self._node_to_components.get(int(node_id), []) or [])
            if not comp_ids:
                # ensure routing map is fresh
                try:
                    self._rebuild_config_map()
                    comp_ids = list(self._node_to_components.get(int(node_id), []) or [])
                except Exception:
                    pass
            if not comp_ids:
                return
            alive_now = self._is_node_alive(node_id)
            for comp in Component.objects.filter(id__in=comp_ids):
                try:
                    comp.controller._receive_from_device(comp.value, is_alive=alive_now, battery_level=level)
                except Exception:
                    continue
        except Exception:
            try:
                self.logger.error(f"Battery propagate failed node={node_id}", exc_info=True)
            except Exception:
                pass

    def _poll_battery_for_node(self, node_id: int):
        """Poll Battery CC level and propagate it, if available."""
        try:
            if not (self._client and self._client.connected):
                return
            vid = {'commandClass': 128, 'endpoint': 0, 'property': 'level'}
            resp = self._async_call(self._client.async_send_command({
                'command': 'node.get_value',
                'nodeId': int(node_id),
                'valueId': vid,
            }), timeout=10)
            level = None
            if isinstance(resp, dict):
                level = resp.get('value', resp.get('result'))
            else:
                level = resp
            if isinstance(level, (int, float)):
                self._propagate_battery_level(int(node_id), max(0, min(int(level), 100)))
        except Exception:
            # Silently ignore if CC not present
            pass

    def _maybe_poll_battery_for_node(self, node_id: int, min_interval: int = 3600):
        """Poll battery for node if past throttle window (default 1 hour)."""
        try:
            now = time.time()
            last = self._last_battery_poll.get(int(node_id), 0)
            if (now - last) < min_interval:
                return
            self._last_battery_poll[int(node_id)] = now
            self._poll_battery_for_node(int(node_id))
        except Exception:
            pass

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
            # Only consider nodes referenced by components in this gateway
            candidate_ids = set(self._node_to_components.keys())
            now = time.time()
            for nid in candidate_ids:
                try:
                    is_dead = not self._is_node_alive(nid)
                    if not is_dead:
                        continue
                    # Ensure components are marked unavailable
                    try:
                        comp_ids = list(self._node_to_components.get(int(nid), []) or [])
                        if comp_ids:
                            def _prop(ids):
                                for comp in Component.objects.filter(id__in=ids):
                                    try:
                                        comp.controller._receive_from_device(comp.value, is_alive=False)
                                    except Exception:
                                        pass
                            asyncio.run_coroutine_threadsafe(asyncio.to_thread(_prop, comp_ids), self._loop)
                    except Exception:
                        pass
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
                    # Use a clean queryset without default select_related from manager
                    # to avoid "Field ... cannot be both deferred and traversed" errors
                    # when combining select_related and only(). We only need zone/name here.
                    comps_qs = _C.objects.filter(id__in=comp_ids).select_related(None).select_related('zone')
                    comps = list(comps_qs.only('id', 'name', 'zone'))
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
        """Handle MQTT commands for this gateway.

        Supports:
          - zwave_command: controller-level commands (add/remove/cancel/etc.)
          - command=discover with type=ZwaveDevice.uid: begin inclusion
        Falls back to core handler for set_val / bulk_send.
        """
        # First, allow core handler (set_val/bulk_send) to process
        super()._on_mqtt_message(client, userdata, msg)
        try:
            payload = json.loads(msg.payload)
        except Exception:
            return
        # Controller-scoped command passthrough
        if 'zwave_command' in payload:
            cmd = payload.get('zwave_command')
            node_id = payload.get('node_id')
            try:
                self.logger.info(f"MQTT zwave_command '{cmd}' node_id={node_id}")
            except Exception:
                pass
            try:
                self._async_call(self._controller_command(cmd, node_id))
            except Exception:
                self.logger.error("Controller command error", exc_info=True)
            return
        # Discovery trigger
        if payload.get('command') == 'discover':
            typ = payload.get('type')
            try:
                from simo_zwave.controllers import ZwaveDevice  # type: ignore
                zw_uid = ZwaveDevice.uid
            except Exception:
                zw_uid = None
            if not zw_uid or typ != zw_uid:
                try:
                    self.logger.info(f"MQTT discover ignored: type mismatch typ='{typ}' expected='{zw_uid}'")
                except Exception:
                    pass
                return
            if not (self._client and self._client.connected):
                self.logger.warning("MQTT discover ignored: driver not connected")
                return
            try:
                # Do NOT reset discovery/init_data here; it was set by the UI form
                self.logger.info("MQTT: begin Z-Wave inclusion")
                self._async_call(self._controller_command('add_node', None), timeout=10)
                try:
                    self.gateway_instance.refresh_from_db(fields=['discovery'])
                except Exception:
                    pass
                disc = self.gateway_instance.discovery or {}
                # Ensure controller is set but preserve init_data
                if not disc.get('controller_uid'):
                    disc['controller_uid'] = zw_uid
                # Clear any stale flags from previous sessions
                for k in ('finished', 'locked_node', 'inclusion_stopped'):
                    disc.pop(k, None)
                disc['inclusion_started'] = time.time()
                self.gateway_instance.discovery = disc
                self.gateway_instance.save(update_fields=['discovery'])
            except Exception:
                self.logger.error("Failed to begin inclusion from MQTT discover", exc_info=True)
            return

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
            try:
                self.logger.info(f"Controller cmd '{cmd}' -> {mapping[cmd]}")
            except Exception:
                pass
            resp = await self._client.async_send_command(mapping[cmd])
            try:
                self.logger.info(f"Controller cmd '{cmd}' result: {resp}")
            except Exception:
                pass
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

    
    def _ensure_discovery_stopped(self):
        """If discovery has finished, ensure inclusion is stopped immediately."""
        try:
            try:
                self.gateway_instance.refresh_from_db(fields=['discovery'])
            except Exception:
                pass
            disc = self.gateway_instance.discovery or {}
            if not disc.get('finished'):
                return
            if self._client and self._client.connected and disc.get('inclusion_started') and not disc.get('inclusion_stopped'):
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
                self.logger.error("_ensure_discovery_stopped failed", exc_info=True)
            except Exception:
                pass

    # ---------- Small helpers to reduce duplication ----------
    def _driver_ready(self) -> bool:
        return bool(self._client and self._client.connected)

    @staticmethod
    def _normalize_cc_value(cc: Optional[int], raw: Any) -> Any:
        try:
            if cc == 48:  # Binary Sensor
                if isinstance(raw, (int, float)):
                    return bool(int(raw))
            if cc == 113:  # Notification
                if isinstance(raw, str):
                    return str(raw).strip().lower() not in ('idle', 'inactive', 'clear', 'unknown', 'no event')
                if isinstance(raw, (int, float)):
                    return bool(int(raw))
        except Exception:
            pass
        return raw

    def _push_value(self, comp: 'Component', value: Any, node_id: Optional[int] = None):
        try:
            alive = self._is_node_alive(int(node_id)) if node_id is not None else True
        except Exception:
            alive = True
        try:
            if self._should_push(comp.id, value):
                comp.controller._receive_from_device(value, is_alive=alive)
                self._mark_pushed(comp.id, value)
            else:
                # Value unchanged; still propagate availability flips
                try:
                    if getattr(comp, 'alive', None) != alive:
                        comp.controller._receive_from_device(comp.value, is_alive=alive)
                except Exception:
                    pass
        except Exception:
            try:
                self.logger.error(f"Push failed for comp={getattr(comp,'id',None)}", exc_info=True)
            except Exception:
                pass

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

    def _build_value_id_for_read(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Build a ValueID for reads. For switches/dimmers (cc 37/38),
        always read 'currentValue' regardless of stored config property.
        """
        vid = self._build_value_id_from_config(cfg)
        try:
            cc = cfg.get('cc')
            if cc in (37, 38):
                vid['property'] = 'currentValue'
                # propertyKey is not used for currentValue on 37/38
                vid.pop('propertyKey', None)
        except Exception:
            pass
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
                        # If node missing treat as dead; otherwise use helper
                        is_alive = self._is_node_alive(nid)
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
            polled_nodes = set()
            for comp in comps:
                try:
                    zw = (comp.config or {}).get('zwave') or {}
                    vid = self._build_value_id_for_read(zw)
                    if not vid.get('commandClass') or vid.get('property') is None:
                        continue
                    try:
                        resp = self._async_call(self._client.async_send_command({
                            'command': 'node.get_value',
                            'nodeId': zw.get('nodeId'),
                            'valueId': vid,
                        }), timeout=10)
                    except Exception:
                        # Even if the read fails (eg node dead), still push availability state
                        self._push_value(comp, comp.value, node_id=zw.get('nodeId'))
                        continue
                    cur = resp.get('value', resp.get('result')) if isinstance(resp, dict) else resp
                    out_val = self._normalize_cc_value(zw.get('cc'), cur)
                    self._push_value(comp, out_val, node_id=zw.get('nodeId'))
                except Exception:
                    continue
            # After value poll, try to fetch battery once per node
            try:
                for comp in comps:
                    nid = ((comp.config or {}).get('zwave') or {}).get('nodeId')
                    if nid in polled_nodes or nid is None:
                        continue
                    polled_nodes.add(nid)
                    self._maybe_poll_battery_for_node(int(nid))
            except Exception:
                pass
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
            # Always operate on the freshest discovery state
            try:
                self.gateway_instance.refresh_from_db(fields=['discovery'])
            except Exception:
                pass
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
            icon = started_with.get('icon')
            alarm_pref = started_with.get('alarm_category')
            base_name = (started_with.get('name') or '').strip()

            from simo_zwave.controllers import (
                ZwaveSwitch, ZwaveDimmer, ZwaveRGBWLight,
                ZwaveBinarySensor, ZwaveNumericSensor, ZwaveButton,
            )
            if not zone:
                try:
                    self.logger.error("Discovery adoption aborted: no Zone provided in init data")
                except Exception:
                    pass
                return
            inst = getattr(zone, 'instance', None)

            # Category helpers based on type if user did not preselect
            def default_category_for_actuator():
                try:
                    from simo.core.models import Category
                    if inst:
                        return Category.objects.filter(instance=inst, name__icontains='lights').first()
                except Exception:
                    return None
            def default_category_for_binary_sensor():
                try:
                    from simo.core.models import Category
                    if inst:
                        return Category.objects.filter(instance=inst, name__icontains='security').first()
                except Exception:
                    return None
            def default_category_for_numeric_sensor():
                try:
                    from simo.core.models import Category
                    if inst:
                        return Category.objects.filter(instance=inst, name__icontains='climate').first()
                except Exception:
                    return None
            def default_category_for_button():
                try:
                    from simo.core.models import Category
                    if inst:
                        return Category.objects.filter(instance=inst, name__icontains='other').first()
                except Exception:
                    return None

            # Icon helpers
            def _get_icon(slug: str):
                try:
                    from simo.core.models import Icon
                    return Icon.objects.filter(slug=slug).first()
                except Exception:
                    return None

            def default_icon_for_actuator():
                return _get_icon('lightbulb')

            def default_icon_for_button():
                return _get_icon('tablet-button')

            def default_icon_for_numeric(prop_any):
                try:
                    p = (str(prop_any) if prop_any is not None else '').lower()
                except Exception:
                    p = ''
                if any(k in p for k in ('illumin', 'lumin', 'lux', 'light')):
                    return _get_icon('brightness')
                if 'temp' in p:
                    return _get_icon('temperature-half')
                return None

            def ctrl_for_cc(cc: int):
                if cc == 37:
                    return ZwaveSwitch
                if cc == 38:
                    return ZwaveDimmer
                if cc == 51:
                    return ZwaveRGBWLight
                return None

            # Create missing actuator components
            # If multiple actuators are created, number them 1..N with the given base name
            numbered_names = {}
            if base_name and len(actuator_eps) > 1:
                for i, tpl in enumerate(sorted(actuator_eps, key=lambda t: int(t[0])), start=1):
                    numbered_names[tpl] = f"{base_name} {i}"
            for ep, cc, prop, pkey in actuator_eps:
                try:
                    exists = Component.objects.filter(
                        gateway=self.gateway_instance,
                        config__zwave__nodeId=int(node_id),
                        config__zwave__endpoint=int(ep),
                        config__zwave__cc=int(cc),
                    ).filter(
                        # consider either property to avoid duplicates across versions
                        models.Q(config__zwave__property='currentValue') | models.Q(config__zwave__property='targetValue')
                    ).exists()
                    if exists:
                        continue
                    ctrl_cls = ctrl_for_cc(int(cc))
                    if not ctrl_cls:
                        continue
                    ctrl_uid = ctrl_cls.uid
                    bt = getattr(ctrl_cls, 'base_type', None)
                    bt_slug = bt if isinstance(bt, str) else getattr(bt, 'slug', None)
                    # Store 'currentValue' for actuators so reads use the true state.
                    store_prop = 'currentValue'
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
                    if base_name:
                        name = numbered_names.get((ep, cc, prop, pkey)) or base_name
                    else:
                        name = f"Z-Wave {int(node_id)}"
                    comp = Component(
                        name=name,
                        zone=zone,
                        category=category or default_category_for_actuator() or None,
                        icon=icon or default_icon_for_actuator() or None,
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

            # If none created, try to create one component per sensor/button type present
            if not created_ids:
                # Determine sensor kinds available on this node from defined valueIds
                def _kind_for_item(it):
                    p = str(gi(it, 'propertyName') or gi(it, 'property') or '').lower()
                    c = int(gi(it, 'commandClass') or 0)
                    if c == 91:
                        return 'button'
                    if c in (48, 113):
                        return 'motion'
                    if c == 49:
                        if any(k in p for k in ('illumin', 'lumin', 'lux', 'light')):
                            return 'brightness'
                        if 'temp' in p:
                            return 'temperature'
                    return None

                sensor_kinds = {}
                for it in items:
                    kind = _kind_for_item(it)
                    if not kind or kind in sensor_kinds:
                        continue
                    cc = int(gi(it, 'commandClass') or 0)
                    ep = int(gi(it, 'endpoint') or 0)
                    prop = gi(it, 'property')
                    pkey = gi(it, 'propertyKey')
                    if prop is None:
                        continue
                    sensor_kinds[kind] = (cc, ep, prop, pkey)

                # Also include hint if it adds a new kind
                if hint and isinstance(hint, dict):
                    try:
                        h_cc = int(hint.get('cc')) if hint.get('cc') is not None else None
                    except Exception:
                        h_cc = None
                    if h_cc is not None:
                        dummy = {'commandClass': h_cc, 'endpoint': hint.get('endpoint'), 'property': hint.get('property')}
                        h_kind = _kind_for_item(dummy)
                        if h_kind and h_kind not in sensor_kinds:
                            sensor_kinds[h_kind] = (
                                int(hint.get('cc')) if hint.get('cc') is not None else 0,
                                int(hint.get('endpoint') or 0),
                                hint.get('property'),
                                hint.get('propertyKey'),
                            )

                # Create components for kinds in a sensible order
                for kind in ['temperature', 'brightness', 'motion', 'button']:
                    if kind not in sensor_kinds:
                        continue
                    cc, ep, prop, pkey = sensor_kinds[kind]
                    try:
                        exists = Component.objects.filter(
                            gateway=self.gateway_instance,
                            config__zwave__nodeId=int(node_id),
                            config__zwave__endpoint=int(ep),
                            config__zwave__cc=int(cc),
                            config__zwave__property=str(prop),
                        ).exists()
                        if exists:
                            continue
                        # Controller per kind
                        ctrl_cls = ZwaveNumericSensor if kind in ('temperature', 'brightness') else (
                            ZwaveBinarySensor if kind == 'motion' else ZwaveButton
                        )
                        ctrl_uid = ctrl_cls.uid
                        bt = getattr(ctrl_cls, 'base_type', None)
                        bt_slug = bt if isinstance(bt, str) else getattr(bt, 'slug', None)
                        cfg = {
                            'zwave': {
                                'nodeId': int(node_id),
                                'cc': int(cc),
                                'endpoint': int(ep),
                                'property': str(prop),
                            }
                        }
                        if pkey not in (None, ''):
                            cfg['zwave']['propertyKey'] = pkey
                        # Name: base_name + kind (when base provided), else generic
                        name = base_name or f"Z-Wave {int(node_id)}"
                        if base_name:
                            name = f"{base_name} {kind}"
                        # Defaults
                        if ctrl_cls is ZwaveBinarySensor:
                            cat = category or default_category_for_binary_sensor() or None
                            alarm_cat = alarm_pref or 'security'
                            icon_obj = icon or None
                        elif ctrl_cls is ZwaveNumericSensor:
                            cat = category or default_category_for_numeric_sensor() or None
                            icon_obj = icon or default_icon_for_numeric(prop) or None
                            alarm_cat = alarm_pref or None
                        else:
                            cat = category or default_category_for_button() or None
                            icon_obj = icon or default_icon_for_button() or None
                            alarm_cat = alarm_pref or None

                        comp = Component(
                            name=name,
                            zone=zone,
                            category=cat,
                            icon=icon_obj,
                            gateway=self.gateway_instance,
                            controller_uid=ctrl_uid,
                            base_type=bt_slug or '',
                            config=cfg,
                            alarm_category=alarm_cat,
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
