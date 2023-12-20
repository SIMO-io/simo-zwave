from simo.core.events import BaseMqttAnnouncement


class ZwaveControllerCommand(BaseMqttAnnouncement):
    TOPIC = 'SIMO/zwave_ctrl_c'

    def __init__(self, gateway_id, command, *args, **kwargs):
        self.data = {
            'gateway_id': gateway_id, 'command': command,
            'args': args, 'kwargs': kwargs
        }


