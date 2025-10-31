=================================
Z‑Wave JS integration for SIMO.io
=================================

Local Z‑Wave control on a SIMO.io hub using Z‑Wave JS (via Z‑Wave JS UI). This app adds a
``Z‑Wave JS`` gateway and Z‑Wave‑aware controllers so you can include/exclude devices from Django Admin
and map device values to SIMO.io components (switches, dimmers, sensors, RGBW lights, buttons) for a clean, app‑native UI.

What you get (at a glance)
--------------------------

* Gateway type: ``Z‑Wave JS`` (auto‑created after restart).
* Uses the Z‑Wave JS Server WebSocket API (loopback only) — no MQTT required.
* Optional local Z‑Wave JS UI access on port 8091, LAN‑only, time‑boxed for 12h (toggle on the gateway form).
* Device management in Django Admin:
  - Start inclusion (“Add”) and exclusion (“Remove nodes”) with live updates.
  - Per‑node values (read/write) listed inline; pick values for components.
  - Actions: remove/replace failed node.
* Component types:
  - ``Switch`` / ``Binary sensor``
  - ``Dimmer`` (maps device range to 0–100)
  - ``Numeric sensor`` (temperature, power, etc.)
  - ``RGBW light``
  - ``Button`` (maps Central Scene: click, double/triple/quad/quintuple‑click, hold, up)

Requirements
------------

* SIMO.io hub (Python >= 3.12).
* USB Z‑Wave controller attached to the hub (use ``/dev/serial/by-id/...`` with a short USB extension).
* Z‑Wave JS UI installed on the hub and configured to run the Z‑Wave JS driver and WS server.

Install Z‑Wave JS UI (Snap)
---------------------------

.. code-block:: bash

   snap install zwave-js-ui
   snap connect zwave-js-ui:raw-usb
   snap connect zwave-js-ui:hardware-observe
   snap start --enable zwave-js-ui
   # Verify status
   snap services zwave-js-ui
   # Tail logs if needed
   snap logs zwave-js-ui --follow

Z‑Wave JS UI access (from SIMO Admin)
-------------------------------------

1) Go to SIMO.io Django Admin → Gateways → Z‑Wave JS.

2) Enable “Expose Z‑Wave JS UI on LAN for 12 hours”. This opens UFW for port 8091 to your LAN and shows the URL.

3) Open the shown URL (e.g., ``http://<hub-ip>:8091``). Default credentials: ``admin / zwave``.

Then in the Z‑Wave JS UI, set:

* Settings → Z‑Wave → Serial Port: select ``/dev/serial/by-id/...`` for your stick.
* Settings → Z‑Wave → Security Keys: define S0 and S2 keys; keep a backup.
* Settings → Home Assistant → WS Server: enable, Host ``127.0.0.1``, Port ``3000`` (loopback only).
* Settings → MQTT Gateway: disable (not required by SIMO.io).

Install this integration
------------------------

1) Install package on the hub

.. code-block:: bash

   workon simo-hub
   pip install simo-zwave

2) Enable the app in ``/etc/SIMO/settings.py``

.. code-block:: python

   from simo.settings import *  # platform defaults
   INSTALLED_APPS += ['simo_zwave']

3) Apply migrations and restart services

.. code-block:: bash

   cd /etc/SIMO/hub
   python manage.py migrate
   supervisorctl restart all

Gateway & Local UI access
-------------------------

After restart, a ``Z‑Wave JS`` gateway is auto‑created. Open it in Django Admin:

* To access the Z‑Wave JS UI from your LAN, enable “Expose Z‑Wave JS UI on LAN for 12 hours”. It opens UFW for port 8091, shows the URL, and auto‑closes after 12h. Default ZUI credentials: ``admin / zwave``.
* The Z‑Wave JS WebSocket API stays bound to ``127.0.0.1:3000`` and is never exposed.

Inclusion / Exclusion (Django Admin)
------------------------------------

* Include: Django Admin → “Zwave nodes” → “Add”. Put the device in inclusion mode; new nodes appear live.
* Exclude: Django Admin → “Zwave nodes” → “Remove nodes”. Put the device in exclusion mode; removed nodes are listed.
* Failed nodes: Use actions on the node list to remove/replace failed devices.

Create components (SIMO app / Admin)
------------------------------------

Create components the usual way and select the ``Z‑Wave JS`` gateway:

* Choose the controller type (Switch, Dimmer, Binary/Numeric Sensor, RGBW light, Button).
* Select the ``Zwave item`` to bind (a node value imported by the gateway). For Buttons, point to the Central Scene “event”.
* For Dimmers, set UI ``min/max``; device range mapping is handled internally.
* Save — the component value updates live. Battery levels propagate to ``Component.battery_level``.

Migration from OpenZWave
------------------------

Upgrading from older ``simo-zwave`` based on OpenZWave requires no re‑inclusion:

* Keep the same USB stick and network keys; configure Z‑Wave JS UI as above.
* On first run, the gateway imports nodes/values from Z‑Wave JS and updates existing rows where possible
  (prefers matching by name/label on the same node). Existing components continue to work without changes.

Troubleshooting
---------------

* No values appearing: Confirm Z‑Wave JS UI is running, serial port correct, and devices finished interview. Wake battery devices.
* Inclusion/exclusion not starting: Ensure the gateway is running and use the Admin pages as described.
* Central Scene: Button supports 'click', 'double‑click', 'triple‑click', 'quadruple‑click', 'quintuple‑click', 'hold', 'up'.
* Port security: WS (3000) is loopback‑only. UI (8091) is closed by default and can be temporarily opened from the gateway form.

Upgrade
-------

.. code-block:: bash

   workon simo-hub
   pip install --upgrade simo-zwave
   python manage.py migrate
   supervisorctl restart all


License
-------

© Copyright by SIMO LT, UAB. Lithuania.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see `<https://www.gnu.org/licenses/>`_.
