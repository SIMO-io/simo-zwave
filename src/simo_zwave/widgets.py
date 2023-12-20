from django import forms


class AdminNodeValueValueWidget(forms.TextInput):

    def __init__(self, value_new, *args, **kwargs):
        self.value_new = value_new
        super().__init__(*args, **kwargs)

    def render(self, name, value, attrs=None, renderer=None):
        org = super().render(name, value, attrs, renderer)
        if self.value_new != value:
            return org + '<br>&nbsp;&nbsp;<strong>%s</strong> - pending' % self.value_new
        return org


class AdminNodeValueSelectWidget(forms.Select):

    def __init__(self, value_new, *args, **kwargs):
        self.value_new = value_new
        super().__init__(*args, **kwargs)

    def render(self, name, value, attrs=None, renderer=None):
        org = super().render(name, value, attrs, renderer)
        if self.value_new != value:
            return org + '<br>&nbsp;&nbsp;<strong>%s</strong> - pending' % self.value_new
        return org