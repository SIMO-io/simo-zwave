import os
from random import randint
from dynamic_preferences.preferences import Section
from dynamic_preferences.types import BooleanPreference, StringPreference
from dynamic_preferences.registries import global_preferences_registry
from django.conf import settings


zwave = Section('zwave')


@global_preferences_registry.register
class OZwaveConfigPathCommunity(StringPreference):
    section = zwave
    name = 'ozwave_config_path_community'
    default = os.path.join(settings.VAR_DIR, 'ozwave_config', 'community')
    required = True


@global_preferences_registry.register
class OZwaveConfigPathUser(StringPreference):
    section = zwave
    name = 'ozwave_config_path_user'
    default = os.path.join(settings.VAR_DIR, 'ozwave_config', 'user')
    required = True


@global_preferences_registry.register
class OZwaveNetworkKey(StringPreference):
    section = zwave
    name = 'netkey'
    default = ', '.join([str(hex(randint(0, 255))) for i in range(16)])
    required = True
    help_text = 'DO NOT CHANGE THIS! Unless you know what you are doing!'

