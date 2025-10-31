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
        ('sync_values', 10),
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

    # --------------- Lifecycle ---------------
    def run(self, exit):
        self.exit = exit
        try:
            self.logger = get_gw_logger(self.gateway_instance.id)
        except Exception:
            pass
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
                try:
                    self.logger.info("Sync tick: importing state")
                except Exception:
                    pass
                self._async_call(self._import_driver_state())
        except Exception:
            pass

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
                        pass
                    self._migrate_one_legacy(nv)
                except Exception:
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
                self.logger.info(f"NV pk={nv.pk} no suitable target found; skipping (base_type={base_type})")
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
    def _async_call(self, coro):
        if not self._loop:
            raise RuntimeError('WS loop not started')
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=15)

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
            pass

        def score(item) -> int:
            s = 0
            if getf(item, 'commandClass') == cc:
                s += 5
            if (getf(item, 'endpoint') or 0) == (endpoint or 0):
                s += 3
            prop_i = getf(item, 'property')
            pname = getf(item, 'propertyName')
            if cc in (37, 38) and prop_i == 'targetValue':
                s += 5
            if (prop is not None and prop_i == prop) or (prop is not None and pname == prop):
                s += 2
            if prop_key not in (None, '') and getf(item, 'propertyKey') == prop_key:
                s += 1
            if pname and label and str(pname).lower() == str(label).lower():
                s += 1
            # writable preference
            meta = meta_cache.get(id(item), {})
            if isinstance(meta, dict) and meta.get('writeable'):
                s += 1
            # expected type preference
            if expected_type and isinstance(meta, dict) and meta.get('type') == expected_type:
                s += 1
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
        class _NV: pass
        nv_like = _NV(); nv_like.command_class=cc; nv_like.endpoint=endpoint; nv_like.property=prop; nv_like.property_key=prop_key
        value_id = self._build_value_id(nv_like)
        try:
            await self._client.async_send_command({
                'command': 'node.set_value',
                'nodeId': node_id,
                'valueId': value_id,
                'value': value,
            })
        except Exception as e:
            # Try to resolve to a valid valueId if invalid, then retry once
            msg = str(e)
            if 'Invalid ValueID' in msg or 'ZW0322' in msg or 'zwave_error' in msg:
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
                # As a last resort for switches, call CC API directly
                try:
                    if cc in (37, 38):
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
