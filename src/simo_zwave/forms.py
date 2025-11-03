from django import forms
from dal import forward
from django.utils.translation import gettext_lazy as _
from simo.core.utils.validators import validate_slaves
from simo.core.utils.form_widgets import AdminReadonlyFieldWidget, EmptyFieldWidget
from django.utils.safestring import mark_safe
from simo.core.forms import BaseGatewayForm, BaseComponentForm, NumericSensorForm
from simo.core.models import Gateway
from simo.core.models import Component
from simo.core.form_fields import (
    Select2ModelChoiceField, Select2ListChoiceField,
    Select2ModelMultipleChoiceField
)


class _ClickableUrlWidget(forms.Widget):
    def render(self, name, value, attrs=None, renderer=None):
        try:
            from simo.core.utils.helpers import get_self_ip
            ip = get_self_ip()
            url = f'http://{ip}:8091'
            return mark_safe(f'<a href="{url}" target="_blank" rel="noopener">{url}</a>')
        except Exception:
            return '—'


class ZwaveGatewayForm(BaseGatewayForm):
    expose_ui = forms.BooleanField(
        label=_('Expose Z-Wave JS UI on LAN for 12 hours'), required=False
    )
    ui_url = forms.CharField(
        label=_('Local Z-Wave JS UI URL'), required=False,
        widget=_ClickableUrlWidget()
    )
    ui_expires = forms.CharField(
        label=_('UI access expires at'), required=False,
        widget=AdminReadonlyFieldWidget()
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # fill readonly fields from gateway config
        cfg = self.instance.config or {}
        # UI URL best-effort compute
        # Widget renders the link dynamically; no need to set initial
        expires_at = cfg.get('ui_expires_at')
        if expires_at:
            try:
                import datetime
                ts = datetime.datetime.fromtimestamp(expires_at)
                self.fields['ui_expires'].initial = ts.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass
        # reflect current exposure status
        self.fields['expose_ui'].initial = bool(cfg.get('ui_open', False))
        # Avoid persisting helper/display fields in config
        if hasattr(self, 'config_fields'):
            for helper_field in ('ui_url', 'expose_ui'):
                if helper_field in self.config_fields:
                    self.config_fields.remove(helper_field)

    def save(self, commit=True):
        obj = super().save(commit=False)
        cfg = obj.config or {}
        requested = self.cleaned_data.get('expose_ui', False)
        was_open = bool(cfg.get('ui_open', False))
        # ensure URL is present
        if not cfg.get('ui_url'):
            cfg['ui_url'] = self._compute_ui_url()
        if requested and not was_open:
            self._ufw_allow_8091_lan()
            cfg['ui_open'] = True
            import time
            cfg['ui_expires_at'] = time.time() + 12 * 3600
        elif not requested and was_open:
            self._ufw_deny_8091_lan()
            cfg['ui_open'] = False
            cfg.pop('ui_expires_at', None)
        obj.config = cfg
        if commit:
            obj.save()
        return obj

    def _compute_ui_url(self):
        try:
            from simo.core.utils.helpers import get_self_ip
            ip = get_self_ip()
            url = f'http://{ip}:8091'
            return f'<a href="{url}" target="_blank" rel="noopener">{url}</a>'
        except Exception:
            return ''

    def _ufw_allow_8091_lan(self):
        try:
            import subprocess, ipaddress, socket, fcntl, struct
            # Allow from RFC1918 ranges by default; hubs are LAN only
            for cidr in ('192.168.0.0/16', '10.0.0.0/8', '172.16.0.0/12'):
                subprocess.run(['ufw', 'allow', 'from', cidr, 'to', 'any', 'port', '8091'], check=False)
        except Exception:
            pass

    def _ufw_deny_8091_lan(self):
        try:
            import subprocess
            # delete rules for default private ranges
            for cidr in ('192.168.0.0/16', '10.0.0.0/8', '172.16.0.0/12'):
                subprocess.run(['ufw', 'delete', 'allow', 'from', cidr, 'to', 'any', 'port', '8091'], check=False)
        except Exception:
            pass



class ZwaveGatewaySelectForm(forms.Form):
    gateway = forms.ModelChoiceField(
        queryset=Gateway.objects.filter(type__startswith='simo_zwave')
    )



class ZwaveNumericSensorConfigForm(NumericSensorForm):
    pass


class ZwaveSwitchConfigForm(BaseComponentForm):
    slaves = Select2ModelMultipleChoiceField(
        queryset=Component.objects.filter(
            base_type__in=(
                'dimmer', 'switch', 'blinds', 'script'
            )
        ),
        url='autocomplete-component',
        forward=[
            forward.Const(['dimmer', 'switch', 'blinds', 'script'], 'base_type')
        ], required=False
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk and 'slaves' in self.fields:
            self.fields['slaves'].initial = self.instance.slaves.all()

    def clean_slaves(self):
        if 'slaves' not in self.cleaned_data:
            return
        if not self.cleaned_data['slaves'] or not self.instance:
            return self.cleaned_data['slaves']
        return validate_slaves(self.cleaned_data['slaves'], self.instance)

    def save(self, commit=True):
        obj = super().save(commit=commit)
        if commit and 'slaves' in self.cleaned_data:
            obj.slaves.set(self.cleaned_data['slaves'])
        return obj


class ZwaveKnobComponentConfigForm(BaseComponentForm):
    min = forms.FloatField(
        initial=0, help_text="Minimum component value."
    )
    max = forms.FloatField(
        initial=100, help_text="Maximum component value."
    )
    zwave_min = forms.FloatField(
        initial=0, help_text="Minimum value expected by Zwave node."
    )
    zwave_max = forms.FloatField(
        initial=99, help_text="Maximum value expected by Zwave node."
    )
    slaves = Select2ModelMultipleChoiceField(
        queryset=Component.objects.filter(
            base_type__in=('dimmer',),
        ),
        url='autocomplete-component',
        forward=(forward.Const(['dimmer', ], 'base_type'),),
        required=False
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk and 'slaves' in self.fields:
            self.fields['slaves'].initial = self.instance.slaves.all()

    def clean_slaves(self):
        if not self.cleaned_data['slaves'] or not self.instance:
            return self.cleaned_data['slaves']
        return validate_slaves(self.cleaned_data['slaves'], self.instance)

    def save(self, commit=True):
        obj = super().save(commit=commit)
        if commit and 'slaves' in self.cleaned_data:
            obj.slaves.set(self.cleaned_data['slaves'])
        return obj

class RGBLightComponentConfigForm(BaseComponentForm):
    has_white = forms.BooleanField(
        label=_("Has WHITE color channel"), required=False,
    )
