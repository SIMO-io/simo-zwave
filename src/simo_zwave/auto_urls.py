from django.urls import re_path
from .views import update_zwave_library

urlpatterns = [
    re_path(
        r"^update-library/$",
        update_zwave_library, name='update-zwave-library'
    ),
]
