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
    # Slow down legacy migration to reduce log noise
    periodic_tasks = (
        ('maintain', 10),
        ('ufw_expiry_check', 60),
        # Slow periodic sync to reduce load/logs; events still keep driver in sync
        ('sync_values', 30),
        ('migrate_legacy', 60),
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

    def _attach_event_listeners(self):
        if not self._client or not self._client.driver:
            return
        controller = self._client.driver.controller
        for node in list(getattr(controller, 'nodes', {}).values()):
            try:
                node.on('value updated', lambda event, n=node: self._on_value_event(event))
                node.on('value added', lambda event, n=node: self._on_value_event(event))
                node.on('value notification', lambda event, n=node: self._on_value_event(event))
                node.on('notification', lambda event, n=node: self._on_value_event(event))
                node.on('metadata updated', lambda event, n=node: self._on_value_event(event))
                node.on('dead', lambda event, n=node: self._on_node_status_event(event))
                node.on('alive', lambda event, n=node: self._on_node_status_event(event))
                node.on('sleep', lambda event, n=node: self._on_node_status_event(event))
                node.on('wake up', lambda event, n=node: self._on_node_status_event(event))
            except Exception:
                self.logger.error(f"Failed to attach listeners for node {getattr(node,'node_id',None)}", exc_info=True)
                continue

    def _on_value_event(self, event):
        try:
            node = event.get('node')
            if not node:
                return
            val_obj = event.get('value') or event.get('value_notification')
            args = event.get('args') or {}
            # Build a val dict similar to _import_driver_state
            if val_obj is not None:
                meta = getattr(val_obj, 'metadata', None)
                val = {
                    'commandClass': getattr(val_obj, 'command_class', None),
                    'endpoint': getattr(val_obj, 'endpoint', 0) or 0,
                    'property': getattr(val_obj, 'property_', None),
                    'propertyKey': getattr(val_obj, 'property_key', None),
                    'propertyName': getattr(val_obj, 'property_name', None),
                    'value': getattr(val_obj, 'value', None),
                    'metadata': {
                        'label': getattr(meta, 'label', None),
                        'unit': getattr(meta, 'unit', ''),
                        'writeable': getattr(meta, 'writeable', False),
                        'type': getattr(meta, 'type', ''),
                        'states': getattr(meta, 'states', None) or [],
                    },
                }
            else:
                # Fall back to event args structure
                val = {
                    'commandClass': args.get('commandClass') or args.get('ccId'),
                    'endpoint': args.get('endpoint') or 0,
                    'property': args.get('property'),
                    'propertyKey': args.get('propertyKey'),
                    'propertyName': args.get('propertyName'),
                    'value': args.get('newValue', args.get('value')),
                    'metadata': args.get('metadata') or {},
                }
            try:
                self.logger.info(
                    f"Event value node={node.node_id} cc={val.get('commandClass')} ep={val.get('endpoint')} prop={val.get('property')} key={val.get('propertyKey')} val={val.get('value')}"
                )
            except Exception:
                pass
            state = {
                'nodeId': node.node_id,
                'name': getattr(node, 'name', '') or '',
                'productLabel': getattr(node, 'product_label', '') or '',
                'status': getattr(node, 'status', None),
                'values': [val],
                'partial': True,
            }
            import asyncio as _asyncio
            _asyncio.run_coroutine_threadsafe(self._import_node_async(state), self._loop)
        except Exception:
            self.logger.error("Unhandled exception in value event", exc_info=True)

    def _on_node_status_event(self, event):
        try:
            node = event.get('node')
            if not node:
                return
            etype = str(event.get('type') or '')
            is_alive = etype != 'dead'
            node_id = getattr(node, 'node_id', None)
            # Defer all DB work to a thread to avoid async ORM errors
            def _apply_status():
                from .models import ZwaveNode as ZN, NodeValue as NV
                zn = ZN.objects.filter(gateway=self.gateway_instance, node_id=node_id).first()
                if not zn:
                    return
                if zn.alive != is_alive:
                    zn.alive = is_alive
                    zn.save(update_fields=['alive'])
                try:
                    comps = [nv.component for nv in NV.objects.filter(node=zn, component__isnull=False).select_related('component')]
                    seen = set()
                    for comp in comps:
                        if not comp or comp.id in seen:
                            continue
                        seen.add(comp.id)
                        try:
                            comp.controller._receive_from_device(comp.value, is_alive=is_alive)
                        except Exception:
                            self.logger.error(
                                f"Failed to propagate availability to component {getattr(comp,'id',None)} for node {zn.node_id}",
                                exc_info=True,
                            )
                            try:
                                comp.alive = is_alive
                                comp.save(update_fields=['alive'])
                            except Exception:
                                self.logger.error(
                                    f"Failed to persist availability directly for component {getattr(comp,'id',None)}",
                                    exc_info=True,
                                )
                except Exception:
                    self.logger.error("Failed availability propagation sweep", exc_info=True)

            asyncio.run_coroutine_threadsafe(asyncio.to_thread(_apply_status), self._loop)
        except Exception:
            self.logger.error("Unhandled exception in node status event", exc_info=True)

    async def _import_node_async(self, state: Dict[str, Any]):
        import asyncio as _asyncio
        await _asyncio.to_thread(self._import_node_sync, state)

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

    def sync_values(self):
        # Periodically import current driver state into DB so values remain fresh
        try:
            if self._client and self._client.connected:
                try:
                    now = time.time()
                    if now - self._last_sync_log > 120:
                        self._last_sync_log = now
                        self.logger.info("Sync tick: importing state")
                except Exception:
                    pass
                self._async_call(self._import_driver_state(), timeout=60)
        except Exception:
            self.logger.error("Periodic sync failed", exc_info=True)

    # --- Legacy bindings migration (one-time) ---
    _migrating = False

    def migrate_legacy(self):
        try:
            if self._migrating:
                return
            cfg = self.gateway_instance.config or {}
            if cfg.get('legacy_migration_done'):
                return
            if not (self._client and self._client.connected):
                return
            self._migrating = True
            from simo_zwave.models import NodeValue
            # Only migrate rows bound to components and without addressing
            pending = list(NodeValue.objects.filter(
                node__gateway=self.gateway_instance,
                component__isnull=False,
                command_class__isnull=True
            )[:25])
            if not pending:
                cfg['legacy_migration_done'] = True
                self.gateway_instance.config = cfg
                self.gateway_instance.save(update_fields=['config'])
                return
            for nv in pending:
                try:
                    try:
                        self.logger.info(f"Migrating NV pk={nv.pk} comp={nv.component_id} base_type={nv.component.base_type}")
                    except Exception:
                        self.logger.error("Failed to log migration start", exc_info=True)
                    self._migrate_one_legacy(nv)
                except Exception:
                    self.logger.error(f"Legacy migration failed for NV pk={nv.pk}", exc_info=True)
                    continue
        finally:
            self._migrating = False

    def _migrate_one_legacy(self, nv: 'NodeValue'):
        comp = nv.component
        if not comp:
            return
        # Map base_type to CC target and expected write pattern
        base_type = (comp.base_type or '').lower()
        if base_type in ('switch', 'lock', 'blinds', 'gate'):
            cc = 37  # Binary Switch as default for switch-like
        elif base_type in ('dimmer', 'rgbw-light'):
            cc = 38  # Multilevel Switch
        elif base_type in ('binary-sensor',):
            # Resolve any boolean value matching this node/label
            cc = None
        elif base_type in ('numeric-sensor',):
            # Resolve any numeric value matching this node/label
            cc = None
        else:
            try:
                self.logger.info(f"Skip migration for NV pk={nv.pk} base_type={base_type}")
            except Exception:
                pass
            return
        # Resolve a writable ValueID using resolver (falls back to driver model)
        try:
            label = nv.label or (comp.name if comp else None)
            resolved = self._async_call(self._resolve_value_id_async(
                nv.node.node_id, cc, None, None, None, label, desired_value=nv.value
            ))
        except Exception:
            resolved = None
        if not resolved:
            try:
                dbg = {}
                try:
                    if self._client and self._client.driver:
                        node = self._client.driver.controller.nodes.get(nv.node.node_id)
                        if node and getattr(node, 'values', None):
                            vals = list(node.values.values())
                            dbg['total_values'] = len(vals)
                            dbg['endpoints'] = sorted({getattr(v, 'endpoint', 0) or 0 for v in vals})
                            dbg['ccs'] = sorted({getattr(v, 'command_class', None) for v in vals})
                            desired_cc = 37 if base_type in ('switch','lock','blinds','gate','rgbw-light') else 38 if base_type == 'dimmer' else None
                            if desired_cc is not None:
                                props = [str(getattr(v, 'property_', None)) for v in vals if getattr(v, 'command_class', None) == desired_cc]
                                eps = sorted({getattr(v, 'endpoint', 0) or 0 for v in vals if getattr(v, 'command_class', None) == desired_cc})
                                dbg['desired_cc'] = desired_cc
                                dbg['desired_cc_endpoints'] = eps
                                dbg['desired_cc_props_sample'] = props[:8]
                except Exception:
                    self.logger.error("Failed building migration debug info", exc_info=True)
                self.logger.info(
                    f"NV pk={nv.pk} no suitable target found; skipping (base_type={base_type}) node={nv.node.node_id} comp={nv.component_id} dbg={dbg}"
                )
            except Exception:
                self.logger.error("Failed to log migration debug info", exc_info=True)
            # For switches/dimmers where we failed to resolve, ask node to refresh its values (throttled)
            if cc in (37, 38):
                now = time.time()
                last = self._last_node_refresh.get(nv.node.node_id, 0)
                if now - last > 300:
                    try:
                        self._async_call(self._client.async_send_command({'command': 'node.refresh_values', 'nodeId': nv.node.node_id}))
                        self._last_node_refresh[nv.node.node_id] = now
                        self.logger.info(f"Requested node {nv.node.node_id} values refresh for migration")
                    except Exception:
                        pass
            return
        # Persist mapping
        nv.command_class = resolved.get('commandClass')
        nv.endpoint = resolved.get('endpoint') or 0
        nv.property = resolved.get('property')
        nv.property_key = resolved.get('propertyKey')
        nv.save(update_fields=['command_class', 'endpoint', 'property', 'property_key'])
        try:
            self.logger.info(
                f"Migrated NV pk={nv.pk} to CC={nv.command_class} ep={nv.endpoint} prop={nv.property} key={nv.property_key} label={label}"
            )
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
        node_val = NodeValue.objects.filter(pk=component.config.get('zwave_item')).first()
        if not node_val:
            return
        # If node is marked dead, skip the send and reflect availability
        try:
            if hasattr(node_val, 'node') and node_val.node and node_val.node.alive is False:
                self.logger.info(f"Node {node_val.node.node_id} is dead; skipping send for comp={component.id}")
                try:
                    component.controller._receive_from_device(component.value, is_alive=False)
                except Exception:
                    self.logger.error(
                        f"Failed to mark component {component.id} unavailable for dead node {node_val.node.node_id}",
                        exc_info=True,
                    )
                # Persist availability regardless to ensure UI reflects reality
                try:
                    if component.alive:
                        component.alive = False
                        component.save(update_fields=['alive'])
                except Exception:
                    self.logger.error(
                        f"Failed to persist component {component.id} alive=False",
                        exc_info=True,
                    )
                return
        except Exception:
            self.logger.error("Availability precheck failed", exc_info=True)
        try:
            try:
                self.logger.info(f"Send comp={component.id} '{component.name}' nv={node_val.pk} raw={value}")
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
                'node_id': node_val.node_id,
                'cc': node_val.command_class,
                'endpoint': node_val.endpoint or 0,
                'property': node_val.property,
                'property_key': node_val.property_key,
                'label': node_val.label,
                'nv_pk': node_val.pk,
            }
            if not addr['cc']:
                from django.db.models import Q
                alt = NodeValue.objects.filter(node_id=node_val.node_id).filter(
                    Q(name__iexact=node_val.name) | Q(label__iexact=node_val.label)
                ).exclude(pk=node_val.pk).first()
                if alt and alt.command_class:
                    addr.update({'cc': alt.command_class, 'endpoint': alt.endpoint or 0, 'property': alt.property, 'property_key': alt.property_key})
            try:
                self.logger.info(f"Addr node={addr['node_id']} cc={addr['cc']} ep={addr['endpoint']} prop={addr['property']} key={addr['property_key']}")
                if not addr['cc'] or not addr['property']:
                    self.logger.info(f"Addr incomplete for nv={node_val.pk}; will resolve before send")
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
        # Map legacy commands to server API
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
        nv_pk = addr.get('nv_pk')
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
                # Persist resolved addressing for future sends
                try:
                    def _persist(pk, res):
                        from simo_zwave.models import NodeValue as NV
                        NV.objects.filter(pk=pk).update(
                            command_class=res.get('commandClass'),
                            endpoint=res.get('endpoint') or 0,
                            property=res.get('property'),
                            property_key=res.get('propertyKey'),
                        )
                    await asyncio.to_thread(_persist, nv_pk, resolved)
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
        class _NV: pass
        nv_like = _NV(); nv_like.command_class=cc; nv_like.endpoint=endpoint; nv_like.property=prop; nv_like.property_key=prop_key
        value_id = self._build_value_id(nv_like)
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
                    # Persist resolved addressing for future sends
                    try:
                        def _persist(pk, res):
                            from simo_zwave.models import NodeValue as NV
                            NV.objects.filter(pk=pk).update(
                                command_class=res.get('commandClass'),
                                endpoint=res.get('endpoint') or 0,
                                property=res.get('property'),
                                property_key=res.get('propertyKey'),
                            )
                        await asyncio.to_thread(_persist, nv_pk, resolved)
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
                    'status': getattr(node, 'status', None),
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
        status = node_state.get('status')
        # 3 == DEAD in zwave_js_server.const.NodeStatus
        zn.alive = False if status == 3 else True
        zn.save()
        try:
            key_alive = f"node:{node_id}:alive"
            prev_alive = self._last_state.get(key_alive)
            if prev_alive is None or bool(prev_alive) != bool(zn.alive):
                self._last_state[key_alive] = bool(zn.alive)
                # Propagate availability to components bound to this node
                from .models import NodeValue as NV
                comps = [nv2.component for nv2 in NV.objects.filter(node=zn, component__isnull=False).select_related('component')]
                seen = set()
                for comp in comps:
                    if not comp or comp.id in seen:
                        continue
                    seen.add(comp.id)
                    try:
                        comp.controller._receive_from_device(comp.value, is_alive=zn.alive)
                    except Exception:
                        self.logger.error(
                            f"Failed to propagate availability (import) to component {getattr(comp,'id',None)} for node {zn.node_id}",
                            exc_info=True,
                        )
                        try:
                            # Hard-set alive as last resort
                            comp.alive = zn.alive
                            comp.save(update_fields=['alive'])
                        except Exception:
                            self.logger.error(
                                f"Failed to persist availability directly for component {getattr(comp,'id',None)}",
                                exc_info=True,
                            )
        except Exception:
            pass
        # Log node import summary only when value count changes, and only on full imports
        if not node_state.get('partial'):
            try:
                vcount = len(node_state.get('values') or [])
                key = f"node:{node_id}:vcount"
                if self._last_state.get(key) != vcount:
                    self._last_state[key] = vcount
                    self.logger.info(f"Import node {node_id}: values={vcount}")
            except Exception:
                pass
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
        # Try to match existing rows by addressing first
        addr_qs = NodeValue.objects.filter(
            node=zn,
            command_class=cc,
            endpoint=endpoint,
            property=str(prop),
            property_key='' if prop_key is None else str(prop_key),
        )
        nv = addr_qs.filter(component__isnull=False).first() or addr_qs.first()
        # If we still didn't find and this is a switch/dimmer currentValue, try to map
        # to an existing targetValue entry (common binding from OZW days)
        if not nv and cc in (37, 38) and str(prop) == 'currentValue':
            alt_qs = NodeValue.objects.filter(
                node=zn, command_class=cc, endpoint=endpoint, property='targetValue'
            )
            nv = alt_qs.filter(component__isnull=False).first() or alt_qs.first()
        # Binary Sensor (CC 48) special-case: if addressing didn't match, but exactly one
        # bound binary sensor exists for this endpoint, use it regardless of property name.
        if not nv and cc == 48:
            try:
                bound = NodeValue.objects.filter(
                    node=zn, command_class=48, endpoint=endpoint, component__isnull=False
                )
                if bound.count() == 1:
                    nv = bound.first()
            except Exception:
                nv = None
        # Basic CC (32) can reflect binary state; map to a single bound CC48 if present
        if not nv and cc == 32 and str(prop) == 'currentValue':
            try:
                # prefer a single bound CC48 at same endpoint
                bound48 = NodeValue.objects.filter(
                    node=zn, command_class=48, endpoint=endpoint, component__isnull=False
                )
                if bound48.count() == 1:
                    nv = bound48.first()
                else:
                    # fallback to CC32 bound row if exactly one at this endpoint
                    bound32 = NodeValue.objects.filter(
                        node=zn, command_class=32, endpoint=endpoint, component__isnull=False
                    )
                    if bound32.count() == 1:
                        nv = bound32.first()
            except Exception:
                nv = None
        # Fallback to label/name matching
        base_qs = NodeValue.objects.filter(node=zn).filter(Q(name__iexact=label) | Q(label__iexact=label))
        try:
            nv = nv or base_qs.filter(component__isnull=False).first()
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
            # Update only changed fields; avoid flipping addressing for CC 37/38 when a component is linked
            changed_fields = []
            # addressing fields guarded
            allow_addr_change = not nv.component or nv.command_class not in (37, 38)
            if nv.command_class != data['command_class'] and allow_addr_change:
                nv.command_class = data['command_class']; changed_fields.append('command_class')
            if (nv.endpoint or 0) != (data['endpoint'] or 0) and allow_addr_change:
                nv.endpoint = data['endpoint']; changed_fields.append('endpoint')
            incoming_prop = data['property']
            if nv.property != incoming_prop and allow_addr_change:
                # For switches/dimmers keep bound NV on targetValue; don't flip to currentValue
                if not (nv.component and nv.command_class in (37, 38) and str(incoming_prop) == 'currentValue'):
                    nv.property = incoming_prop; changed_fields.append('property')
            incoming_pk = data['property_key']
            if (nv.property_key or '') != (incoming_pk or '') and allow_addr_change:
                nv.property_key = incoming_pk; changed_fields.append('property_key')
            # non-address fields
            for fld in ('genre', 'type', 'label', 'is_read_only', 'index', 'units', 'value_choices'):
                val_new = data[fld]
                if getattr(nv, fld) != val_new:
                    setattr(nv, fld, val_new)
                    changed_fields.append(fld)
            # value change is frequent; update only if changed
            if nv.value != current:
                nv.value = current
                nv.value_new = current
                changed_fields.extend(['value', 'value_new'])
            if changed_fields:
                nv.save(update_fields=list(set(changed_fields)))
            created = False
        # Log only on create or address change
        try:
            if created:
                self.logger.info(f"Import map node={zn.node_id} '{label}' -> NV pk={nv.pk} cc={cc} ep={endpoint} prop={prop} key={prop_key} created=True")
        except Exception:
            pass
        # Battery level shortcut
        if cc == 0x80 and current is not None:
            try:
                zn.battery_level = current
                zn.save(update_fields=['battery_level'])
            except Exception:
                pass
            # Update battery level across all components bound to this node without changing their values
            try:
                from .models import NodeValue as NV
                comps = [nv2.component for nv2 in NV.objects.filter(node=zn, component__isnull=False).select_related('component')]
                seen = set()
                for comp in comps:
                    if not comp or comp.id in seen:
                        continue
                    seen.add(comp.id)
                    try:
                        comp.controller._receive_from_device(comp.value, is_alive=zn.alive, battery_level=current)
                    except Exception:
                        self.logger.error(
                            f"Failed to propagate battery to component {getattr(comp,'id',None)} for node {zn.node_id} level={current}",
                            exc_info=True,
                        )
            except Exception:
                self.logger.error("Battery propagation sweep failed", exc_info=True)
        # Push to component if linked. For switches/dimmers, prefer targetValue/currentValue-bound NV
        if cc in (37, 38) and str(prop) == 'currentValue':
            try:
                # First try exact endpoint
                target_nv = NodeValue.objects.filter(
                    node=zn, command_class=cc, endpoint=endpoint, property='targetValue', component__isnull=False
                ).first()
                # Fallback: try root endpoint 0 if exact not present
                if not target_nv and endpoint not in (0, None):
                    target_nv = NodeValue.objects.filter(
                        node=zn, command_class=cc, endpoint=0, property='targetValue', component__isnull=False
                    ).first()
                # Fallback: any endpoint with a bound targetValue for this CC
                if not target_nv:
                    target_nv = NodeValue.objects.filter(
                        node=zn, command_class=cc, property='targetValue', component__isnull=False
                    ).order_by('endpoint').first()
            except Exception:
                target_nv = None
            if target_nv and target_nv.component:
                try:
                    # keep component in sync with current value
                    if target_nv.value != current:
                        target_nv.value = current
                        target_nv.value_new = current
                        target_nv.save(update_fields=['value', 'value_new'])
                    target_nv.component.controller._receive_from_device(current, is_alive=zn.alive)
                except Exception:
                    self.logger.error(
                        f"Failed to set component value (current->target sync) comp={getattr(target_nv.component,'id',None)} node={zn.node_id} cc={cc} ep={endpoint} prop=currentValue val={current}",
                        exc_info=True,
                    )
                return
        if cc in (37, 38) and str(prop) == 'targetValue':
            try:
                # Symmetric: update a bound currentValue if present
                curr_nv = NodeValue.objects.filter(
                    node=zn, command_class=cc, endpoint=endpoint, property='currentValue', component__isnull=False
                ).first()
                if not curr_nv and endpoint not in (0, None):
                    curr_nv = NodeValue.objects.filter(
                        node=zn, command_class=cc, endpoint=0, property='currentValue', component__isnull=False
                    ).first()
            except Exception:
                curr_nv = None
            if curr_nv and curr_nv.component:
                try:
                    if curr_nv.value != current:
                        curr_nv.value = current
                        curr_nv.value_new = current
                        curr_nv.save(update_fields=['value', 'value_new'])
                    curr_nv.component.controller._receive_from_device(current, is_alive=zn.alive)
                except Exception:
                    self.logger.error(
                        f"Failed to set component value (target->current sync) comp={getattr(curr_nv.component,'id',None)} node={zn.node_id} cc={cc} ep={endpoint} prop=targetValue val={current}",
                        exc_info=True,
                    )
                return
        if nv.component:
            try:
                if nv.value is not None:
                    nv.component.controller._receive_from_device(nv.value, is_alive=zn.alive)
            except Exception:
                self.logger.error(
                    f"Failed to set component value comp={getattr(nv.component,'id',None)} node={zn.node_id} cc={cc} ep={endpoint} prop={prop} key={prop_key} val={nv.value}",
                    exc_info=True,
                )

    # With client.listen(), the library updates the driver model internally.
    # We keep DB in sync via periodic full imports or future event hooks.
