=================================
Z‑Wave (OpenZWave) integration for SIMO.io
=================================

Local Z‑Wave control on a SIMO.io hub using OpenZWave. This app adds a
``Zwave`` gateway and Z‑Wave‑aware controllers so you can include/exclude
nodes from Django Admin and map device values to SIMO.io components
switches, dimmers, sensors, RGBW lights — for a clean, app‑native UI.

What you get (at a glance)
--------------------------

* Gateway type: ``Zwave`` (auto‑created with defaults after restart).
* Device management in Django Admin:
  - Include nodes (“Add”) and exclude nodes (“Remove nodes”) with live
    progress; pending commands auto‑cancel after timeout.
  - Per‑node values (read/write) listed inline; name values you intend to
    map to components; change values where writable.
  - Actions: kill node, request network update, send node information,
    check if node failed.
* Component types (select per value):
  - ``Switch`` and ``Binary sensor`` (Bool/Button values → on/off).
  - ``Dimmer`` (numeric values with min/max mapping to 0–100).
  - ``Numeric sensor`` (temperatures, power, etc.; preserves units).
  - ``RGBW light`` (basic color control; optional white channel).
* OpenZWave devices library updater (Admin button), local MQTT bridge,
  and periodic node health updates.

Requirements
------------

* SIMO.io core ``>= 3.1.7`` (installed on your hub).
* Python ``>= 3.12``.
* USB Z‑Wave controller attached to the hub (e.g., ``/dev/ttyACM0`` or
  ``/dev/ttyUSB0``) with appropriate OS permissions.
* The hub can create and write to its OpenZWave config paths
  (created automatically on first run).
* A Z‑Wave network key is generated automatically and stored in the hub’s
  dynamic settings; keep it stable unless you intentionally re‑key devices.

Install on a SIMO.io hub
------------------------

1. SSH to your hub and activate the hub’s Python environment
   (for example ``workon simo-hub``).

   .. code-block:: bash

      workon simo-hub

2. Install the package.

   .. code-block:: bash

      pip install simo-zwave

3. Enable the app in Django settings (``/etc/SIMO/settings.py``).

   .. code-block:: python

      # /etc/SIMO/settings.py
      from simo.settings import *  # keep platform defaults

      INSTALLED_APPS += [
          'simo_zwave',
      ]

4. Apply migrations from this app.

   .. code-block:: bash

      cd /etc/SIMO/hub

      python manage.py migrate

5. Restart SIMO services so the new app and gateway type load.

   .. code-block:: bash

      supervisorctl restart all

After restart: gateway, device path, logs
-----------------------------------------

After installation and restart, a ``Zwave`` gateway is created
automatically with default settings. Open it in Django Admin to confirm
the USB device path (defaults to ``/dev/ttyACM0``). Adjust if your stick
appears as another path (for example ``/dev/ttyUSB0``). The gateway page
also shows helpful logs.

Include new Z‑Wave devices (Admin)
----------------------------------

1. Go to Django Admin → ``Zwave nodes`` → “Add” (top right).
2. If you have more than one Z‑Wave gateway, pick the target gateway.
3. The page starts inclusion and shows live updates. Put each device into
   inclusion mode; discovered nodes appear on the page.
4. Click “Finish!” to cancel inclusion and mark new nodes as configured.

Exclude devices (Admin)
-----------------------

Open the “Remove nodes” page under ``Zwave nodes`` in Django Admin. Start
exclusion, put devices into remove mode, then click “Finish!” to cancel.

Prepare values for components (Admin)
-------------------------------------

Open a node in Admin. Its values are listed inline:

* Name the values you plan to use in components. Only values with a name
  and ``genre = User`` appear in component forms.
* For writable values, you can set a new value; “pending” indicates a
  value that will be sent to the device.

Add Z‑Wave components (SIMO.io app)
-----------------------------------

1. Components → Add New → Component.
2. Select Gateway: ``Zwave``.
3. Select Component type: choose based on the target value:
   - ``Switch`` or ``Binary sensor`` for Bool/Button values.
   - ``Dimmer`` for percentage/level values (0–99, 0–255, etc.).
   - ``Numeric sensor`` for scalar readings (°C, W, %, …).
   - ``RGBW light`` for color‑capable devices.
4. Complete the form:
   - ``Zwave item``: pick a named value from the device.
   - For ``Dimmer``: set display ``min/max`` and Z‑Wave ``zwave_min``/``zwave_max``
     to map the device range to 0–100.
   - For ``Switch``: optionally set ``slaves`` (other components to follow).
   - Usual component fields (name, room, category, icon).
5. Save. The component reflects live value and stays in sync with the node.

OpenZWave library updates (Admin)
---------------------------------

The gateway form exposes an “Update OpenZWave devices library” button.
Click to download the latest device configs from the OpenZWave project.
Gateways stop briefly during update and auto‑resume afterward.

Troubleshooting
---------------

* Cannot start gateway / no USB device:
  - Verify the ``device`` path in the gateway (e.g., ``/dev/ttyACM0``).
  - Check OS permissions for the SIMO process user (e.g., dialout group).
* Inclusion/exclusion stalls: Click “Finish!” to cancel, then retry.
* Value not appearing in component form: Ensure the Node Value is named
  and has ``genre = User``; wait for the next update.
* Odd levels on dimmers: Adjust ``zwave_min``/``zwave_max`` to match the
  device’s native range; set display ``min/max`` for your preferred scale.
* Missing device parameters: Update the OpenZWave library from Admin.

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
