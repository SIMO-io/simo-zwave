from simo.core.controllers import (
    BinarySensor, NumericSensor,
    Switch, Dimmer, RGBWLight
)
from .gateways import ZwaveGatewayHandler
from .forms import (
    BasicZwaveComponentConfigForm, ZwaveKnobComponentConfigForm,
    RGBLightComponentConfigForm, ZwaveNumericSensorConfigForm
)

class ZwaveBinarySensor(BinarySensor):
    gateway_class = ZwaveGatewayHandler
    config_form = BasicZwaveComponentConfigForm

    def _receive_from_device(self, val):
        return super()._receive_from_device(bool(val))

class ZwaveNumericSensor(NumericSensor):
    gateway_class = ZwaveGatewayHandler
    config_form = ZwaveNumericSensorConfigForm


class ZwaveSwitch(Switch):
    gateway_class = ZwaveGatewayHandler
    config_form = BasicZwaveComponentConfigForm

    def _receive_from_device(self, val):
        return super()._receive_from_device(bool(val))


class ZwaveDimmer(Dimmer):
    gateway_class = ZwaveGatewayHandler
    config_form = ZwaveKnobComponentConfigForm

    def _send_to_device(self, value):
        conf = self.component.config

        com_amplitude = conf.get('max', 1.0) - conf.get('min', 0.0)
        float_value = (value - conf.get('min', 0.0)) / com_amplitude

        zwave_amplitude = conf.get('zwave_max', 99.0) - conf.get('zwave_min', 0.0)
        set_val = float_value * zwave_amplitude + conf.get('zwave_min', 0.0)

        return super()._send_to_device(set_val)

    def _receive_from_device(self, val):
        conf = self.component.config

        zwave_amplitude = conf.get('zwave_max', 99.0) - conf.get('zwave_min', 0.0)
        float_value = (val - conf.get('zwave_min', 0.0)) / zwave_amplitude

        com_amplitude = conf.get('max', 99.0) - conf.get('min', 0.0)
        set_val = float_value * com_amplitude + conf.get('min', 0.0)

        return super()._receive_from_device(set_val)


class ZwaveRGBWLight(RGBWLight):
    gateway_class = ZwaveGatewayHandler
    config_form = RGBLightComponentConfigForm

    def _receive_from_device(self, val):
        # TODO: need to addapt to map type RGBWLight value.
        return super()._receive_from_device(val)
