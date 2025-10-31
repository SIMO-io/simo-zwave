from simo.core.controllers import (
    BinarySensor, NumericSensor,
    Switch, Dimmer, RGBWLight, Button
)
from .gateways import ZwaveGatewayHandler
from .forms import (
    BasicZwaveComponentConfigForm, ZwaveKnobComponentConfigForm,
    RGBLightComponentConfigForm, ZwaveNumericSensorConfigForm,
    ZwaveSwitchConfigForm
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
    config_form = ZwaveSwitchConfigForm

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


class ZwaveButton(Button):
    gateway_class = ZwaveGatewayHandler
    config_form = BasicZwaveComponentConfigForm

    def _receive_from_device(self, val):
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
                    return super()._receive_from_device(v)
            elif isinstance(val, str):
                v = mapping_str.get(val) or val.lower()
                # accept already-normalized values too
                return super()._receive_from_device(v)
        except Exception:
            pass
        # Fallback: ignore unknowns
        return
