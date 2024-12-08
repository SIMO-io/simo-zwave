import os
import logging
import json
import sys
from openzwave.network import ZWaveNetwork
from openzwave.option import ZWaveOption
import paho.mqtt.client as mqtt
from pydispatch import dispatcher
import time
from django.conf import settings
from simo.core.models import Component
from simo.core.gateways import BaseGatewayHandler
from simo.core.events import GatewayObjectCommand, get_event_obj
from simo.conf import dynamic_settings
from .forms import ZwaveGatewayForm
from .models import ZwaveNode, NodeValue


class ZwaveGatewayHandler(BaseGatewayHandler):
    name = "Zwave"
    config_form = ZwaveGatewayForm
    network = None

    def run(self, exit):
        print("Starting Zwave Gateway")
        mqtt_client = mqtt.Client()
        mqtt_client.username_pw_set('root', settings.SECRET_KEY)

        try:
            config_path = dynamic_settings['zwave__ozwave_config_path_community']
            if not os.path.exists(config_path):
                os.makedirs(config_path)
            user_path = dynamic_settings['zwave__ozwave_config_path_user']
            if not os.path.exists(user_path):
                os.makedirs(user_path)

            options = ZWaveOption(
                device=self.gateway_instance.config['device'],
                config_path=dynamic_settings['zwave__ozwave_config_path_community'],
                user_path=dynamic_settings['zwave__ozwave_config_path_user']
            )
            options.addOptionString(
                'NetworkKey', dynamic_settings['zwave__netkey'], False
            )
            options.lock()

            self.network = ZWaveNetwork(options, autostart=False)
            dispatcher.connect(
                self.network_ready, ZWaveNetwork.SIGNAL_NETWORK_AWAKED
            )
            self.network.start()


            mqtt_client.on_connect = self.on_mqtt_connect
            mqtt_client.on_message = self.on_mqtt_message
            mqtt_client.connect(
                host=settings.MQTT_HOST, port=settings.MQTT_PORT
            )
            mqtt_client.loop_start()

            seconds_counter = 0
            while not exit.is_set():
                time.sleep(1)
                if seconds_counter > 30:
                    self.perform_periodic_maintenance()
                    seconds_counter = 0
                else:
                    seconds_counter += 1

            print("I must stop now!")
            mqtt_client.loop_stop()
            print("MQTT stopped!")
            self.network.stop()
            print("Network stopped!")

        except SystemExit:
            print("STOPPING.....")
            mqtt_client.loop_stop()
            self.network.stop()
            raise SystemExit()

    def on_mqtt_connect(self, mqtt_client, userdata, flags, rc):
        print("Connected to mqtt with result code " + str(rc))
        command = GatewayObjectCommand(self.gateway_instance)
        mqtt_client.subscribe(command.get_topic())

    def on_mqtt_message(self, client, userdata, msg):
        payload = json.loads(msg.payload)

        if 'zwave_command' in payload:
            if not hasattr(self.network, payload['zwave_command']):
                return
            zwave_command = getattr(
                self.network.controller, payload['zwave_command']
            )
            if 'node_id' in payload:
                kwargs = {'node_id': payload['node_id']}
            else:
                kwargs = {}
            try:
                zwave_command(**kwargs)
            except Exception as e:
                print(e, file=sys.stderr)
                return
            else:
                print("Command %s initiated!" % payload['command'])
                self.gateway_instance.refresh_from_db()
                if payload['command'] == 'cancel_command' \
                and 'last_controller_command' in self.gateway_instance.config:
                    self.gateway_instance.config.pop('last_controller_command')
                    self.gateway_instance.save()
            return

        if 'bulk_send' in payload:
            components = {c.id: c for c in Component.objects.filter(
                gateway__type=self.uid,
                id__in=[int(id) for id in payload['bulk_send'].keys()]
            )}
            for comp_id, value in payload['bulk_send'].items():
                if int(comp_id) not in components:
                    continue
                comp = components[int(comp_id)]
                try:
                    value = comp.controller._string_to_vals(str(value))[0]
                except:
                    pass
                self.send_val(components[int(comp_id)], value)
            return

        target = get_event_obj(payload)
        if isinstance(target, Component):
            if 'set_val' not in payload:
                return
            self.send_val(target, payload['set_val'])

    def send_val(self, target, val):
        if isinstance(target, NodeValue):
            node_val = target
        else:
            try:
                node_val = NodeValue.objects.get(
                    pk=target.config.get('zwave_item', 0)
                )
            except:
                return
        try:
            nodev = self.network.nodes[
                node_val.node.node_id
            ].values[node_val.value_id]
            nodev.data = val
        except Exception as e:
            logging.error(e, exc_info=True)
            pass

    def update_node_stats(self, node_model):

        try:
            zwave_node = self.network.nodes[node_model.node_id]
            node_model.stats = zwave_node.stats
        except KeyError:
            node_model.alive = False
        else:
            node_model.product_name = zwave_node.product_name
            node_model.product_type = zwave_node.type
            node_model.alive = not zwave_node.is_failed
            node_model.battery_level = zwave_node.get_battery_level()

        node_model.save()

        for node_val in node_model.node_values.all().exclude(component=None):
            node_val.component.alive = node_model.alive
            node_val.component.battery_level = node_model.battery_level
            node_val.component.save()

    def perform_periodic_maintenance(self):
        self.gateway_instance.refresh_from_db()
        last_controller_command = self.gateway_instance.config.get(
            'last_controller_command'
        )
        if last_controller_command \
        and time.time() - last_controller_command > 20:
            self.network.controller.cancel_command()
            self.gateway_instance.config.pop('last_controller_command')
            self.gateway_instance.save()
            print("CANCELED ALL COMMANDS!!!!")
        for node_model in self.gateway_instance.zwave_nodes.all():
            self.update_node_stats(node_model)


    def update_node_val(self, node, val):
        node = ZwaveNode.objects.filter(node_id=node.node_id).first()
        if not node:
            self.update_node_model(node)

        data = {
            'genre': val.genre, 'type': val.type, 'label': val.label,
            'is_read_only': val.is_read_only, 'index': val.index,
            'units': val.units, 'value': val.data,
            'value_new': val.data,
            'value_choices': list(val.data_items) if val.type == 'List' else [],
        }
        node_val, new = NodeValue.objects.get_or_create(
            node=node, value_id=val.value_id, defaults=data
        )
        if not new:
            for attr, v in data.items():
                setattr(node_val, attr, v)
                node_val.save()
        if node_val.label.lower().startswith('battery lev') \
                and val.units == '%':
            node_val.node.battery_level = val.data
            node_val.node.save()

        if node_val.component:
            try:
                node_val.component.controller._receive_from_device(
                    node_val.value
                )
            except Exception as e:
                logging.exception("Unable to save value to component!")
                logging.error(e, exc_info=True)



    def update_node_model(self, node):
        if node.product_type == '0x0001' or node.node_id in (1, 255):
            # Static PC Controller
            return

        node_model, new = ZwaveNode.objects.get_or_create(
            node_id=node.node_id, gateway=self.gateway_instance, defaults={
                'product_name': node.product_name,
                'product_type': node.type
            }
        )
        self.update_node_stats(node_model)

        for key, val in node.values.items():
            self.update_node_val(node, val)

    def network_ready(self, network):
        dispatcher.connect(self.node_update, ZWaveNetwork.SIGNAL_NODE_ADDED)
        dispatcher.connect(self.node_update, ZWaveNetwork.SIGNAL_NODE)
        dispatcher.connect(self.node_removed, ZWaveNetwork.SIGNAL_NODE_REMOVED)
        dispatcher.connect(self.value_update, ZWaveNetwork.SIGNAL_VALUE)

        for node_id, node in self.network.nodes.items():
            self.update_node_model(node)

    def node_update(self, network, node):
        self.update_node_model(node)

    def node_removed(self, network, node):
        try:
            node_model = ZwaveNode.objects.get(
                node_id=node.node_id, gateway=self.gateway_instance
            )
        except ZwaveNode.DoesNotExist:
            pass
        else:
            node_model.delete()

    def value_update(self, network, node, value):
        self.update_node_val(node, value)


