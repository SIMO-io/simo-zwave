import os
import tempfile
import logging
import sys
import time
import shutil
import zipfile
from urllib.request import urlopen
from dynamic_preferences.registries import global_preferences_registry as gpr
from simo.core.models import Gateway
from .gateways import ZwaveGatewayHandler



def get_latest_ozw_library():
    stopped_gateways = []
    for gateway in Gateway.objects.filter(type=ZwaveGatewayHandler.uid):
        if gateway.status == 'running':
            gateway.stop()
            stopped_gateways.append(gateway)

    if stopped_gateways:
        if len(stopped_gateways) == 1:
            print("Stopping running gateway")
        else:
            print("Stopping running gateways")
        # give some time for gateways to stop
        time.sleep(2)

    configs_path = gpr.manager()['zwave__ozwave_config_path_community']

    dest = tempfile.mkdtemp()
    dest_file = os.path.join(dest, 'open-zwave.zip')
    print("Getting latest configs from OpenZwave Github project")
    try:
        req = urlopen(
            'https://codeload.github.com/OpenZWave/open-zwave/zip/master')
        with open(dest_file, 'wb') as f:
            f.write(req.read())
        zip_ref = zipfile.ZipFile(dest_file, 'r')
        zip_ref.extractall(dest)
        zip_ref.close()
    except Exception:
        print("Can't get zip from github. Cancelling.")
        try:
            shutil.rmtree(dest)
        except Exception:
            pass
        return "Can't get zip from github. Cancelling."

    if os.path.isdir(configs_path):
        # Try to remove old config
        try:
            shutil.rmtree(configs_path)
        except Exception:
            print("Can't remove old config directory")
            return "Can't remove old config directory"

    try:
        shutil.copytree(os.path.join(dest, 'open-zwave-master', 'config'),
                        configs_path)
    except Exception:
        print("Can't copy to %s", configs_path)
        return "Can't copy to %s", configs_path

    try:
        with open(os.path.join(configs_path,
                               'pyozw_config.version'), 'w') as f:
            f.write(time.strftime("%Y-%m-%d %H:%M"))
    except Exception:
        msg = "Can't update %s" % os.path.join(
            configs_path, 'pyozw_config.version'
        )
        print(msg)
        return msg

    try:
        with open(os.path.join(configs_path, '__init__.py'),
                  'a') as f:
            f.write(
                "#This file is part of **python-openzwave** project https://github.com/OpenZWave/python-openzwave.")
    except Exception:
        msg = "Can't create %s" % os.path.join(configs_path, '__init__.py')
        return msg

    shutil.rmtree(dest)

    if stopped_gateways:
        if len(stopped_gateways) == 1:
            print("Starting gateway")
        else:
            print("Starting gateways")
        for gateway in stopped_gateways:
            gateway.start()
