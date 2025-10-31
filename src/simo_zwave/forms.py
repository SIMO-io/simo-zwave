from django import forms
from django.db.models import Q
from dal import forward
from django.utils.translation import gettext_lazy as _
from simo.core.utils.validators import validate_slaves
from simo.core.utils.form_widgets import AdminReadonlyFieldWidget, EmptyFieldWidget
from simo.core.forms import BaseGatewayForm, BaseComponentForm, NumericSensorForm
from simo.core.models import Gateway
from simo.core.events import GatewayObjectCommand
from simo.core.models import Component
from simo.core.form_fields import (
    Select2ModelChoiceField, Select2ListChoiceField,
    Select2ModelMultipleChoiceField
)
from .models import ZwaveNode, NodeValue
from .widgets import AdminNodeValueValueWidget, AdminNodeValueSelectWidget


class ZwaveGatewayForm(BaseGatewayForm):
    expose_ui = forms.BooleanField(
        label=_('Expose Z-Wave JS UI on LAN for 12 hours'), required=False
    )
    ui_url = forms.CharField(
        label=_('Local Z-Wave JS UI URL'), required=False,
        widget=AdminReadonlyFieldWidget()
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
        computed = cfg.get('ui_url') or self._compute_ui_url()
        # Set both field initial and form initial to ensure widget gets a value
        self.fields['ui_url'].initial = computed
        self.initial['ui_url'] = computed
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


class AdminNodeValueInlineForm(forms.ModelForm):
    value = forms.CharField(required=False)

    class Meta:
        model = NodeValue
        fields = 'label', 'value', 'units', 'genre',

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.org_val = self.instance.value
        if self.instance.is_read_only:
            self.fields['value'].widget = AdminReadonlyFieldWidget()
        else:
            if self.instance.type in ('Button', 'Bool'):
                self.fields['value'].widget = AdminNodeValueSelectWidget(
                    self.instance.value_new,
                    choices=[('True', 'True'), ('False', 'False')]
                )
            elif self.instance.type == 'List' and self.instance.value_choices:
                self.fields['value'].widget = AdminNodeValueSelectWidget(
                    self.instance.value_new,
                    choices=[('%s' % c, c) for c in self.instance.value_choices],
                    attrs={'style': 'width:200px'}
                )
            else:
                self.fields['value'].widget = AdminNodeValueValueWidget(
                    self.instance.value_new
                )


        if self.instance.genre != 'User':
            self.fields['name'].widget = EmptyFieldWidget()


    def clean_value(self):

        val = self.cleaned_data['value']

        if self.instance.type in ('Button', 'Bool'):
            if val == 'True':
                val = True
            elif val == 'False':
                val = False
            else:
                raise forms.ValidationError("Must be True or False")


        elif self.instance.type in ('Byte', 'Int', 'Short'):
            try:
                val = int(val)
            except:
                raise forms.ValidationError("Must be a number")

            if self.instance.type == 'Byte':
                if val < 0 or val > 255:
                    raise forms.ValidationError(
                        "Must be a number between 0 and 255"
                    )

            if self.instance.type == 'Short':
                try:
                    val = int(val)
                except:
                    raise forms.ValidationError(
                        "Must be an integer between -32768 and 32768"
                    )
                else:
                    if int(val) > 32768 or int(val) < -32768:
                        raise forms.ValidationError(
                            "Must be an integer between -32768 and 32768"
                        )

        elif self.instance.type == 'Decimal':
            try:
                val = float(val)
            except:
                raise forms.ValidationError("Must be a decimal number")

        return val

    def save(self, commit=True):
        self.instance.value = self.org_val
        set_val = False
        if not self.instance.is_read_only \
        and self.cleaned_data['value'] != self.org_val:
            self.instance.value_new = self.cleaned_data['value']
            set_val = True
        obj = super().save(commit=commit)
        if set_val:
            for gateway in Gateway.objects.filter(type__startswith='simo_zwave'):
                GatewayObjectCommand(
                    gateway, self.instance,
                    set_val=self.cleaned_data['value']
                ).publish()
        return obj


class ZwaveGatewaySelectForm(forms.Form):
    gateway = forms.ModelChoiceField(
        queryset=Gateway.objects.filter(type__startswith='simo_zwave')
    )



class BasicZwaveComponentConfigForm(BaseComponentForm):
    zwave_item = forms.ModelChoiceField(queryset=NodeValue.objects.all())

    node_value = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        qs = NodeValue.objects.filter(
            node__gateway=self.gateway, name__isnull=False,
        ).filter(
            Q(component=None) | Q(component=self.instance)
        ).order_by('node_id', 'id')
        self.fields['zwave_item'].queryset = qs


    def clean_zwave_val(self):
        if self.cleaned_data['zwave_item'].component in (
            None, self.instance
        ):
            return self.cleaned_data['zwave_item']

        raise forms.ValidationError(
            'Zwave val already has other component assigned to it'
        )

    def save(self, commit=True):
        self.instance.value = self.cleaned_data['zwave_item'].value
        org_zwave_item = None
        if not self.instance.pk:
            self.instance.value_units = self.cleaned_data['zwave_item'].units
        else:
            org_zwave_item = NodeValue.objects.get(
                pk=self.instance.config.get('zwave_item')
            )
        obj = super().save(commit=commit)
        self.cleaned_data['zwave_item'].component = obj
        self.cleaned_data['zwave_item'].save()
        if org_zwave_item \
        and org_zwave_item.pk != self.cleaned_data['zwave_item'].pk:
            org_zwave_item.component = None
            org_zwave_item.save()
        return obj


class ZwaveNumericSensorConfigForm(BasicZwaveComponentConfigForm, NumericSensorForm):
    pass


class ZwaveSwitchConfigForm(BasicZwaveComponentConfigForm):
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


class ZwaveKnobComponentConfigForm(BasicZwaveComponentConfigForm):
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

class RGBLightComponentConfigForm(BasicZwaveComponentConfigForm):
    has_white = forms.BooleanField(
        label=_("Has WHITE color channel"), required=False,
    )
