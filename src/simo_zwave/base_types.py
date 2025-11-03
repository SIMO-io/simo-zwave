from django.utils.translation import gettext_lazy as _
from simo.core.base_types import BaseComponentType


class ZwaveDeviceType(BaseComponentType):
    slug = 'zwave-device'
    name = _("Z-Wave Device")
    description = _("Generic Z-Wave device placeholder used to start pairing.")
    purpose = _("Select to begin Z-Wave inclusion/adoption; actual components will be created based on the node.")


def _export_base_types_dict():
    import inspect as _inspect
    mapping = {}
    for _name, _obj in globals().items():
        if _inspect.isclass(_obj) and issubclass(_obj, BaseComponentType) \
                and _obj is not BaseComponentType and getattr(_obj, 'slug', None):
            mapping[_obj.slug] = _obj.name
    return mapping


BASE_TYPES = _export_base_types_dict()

