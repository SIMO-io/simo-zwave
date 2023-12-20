from django.conf.urls import include, url
from .views import update_zwave_library

urlpatterns = [
    url(
        r"^update-library/$",
        update_zwave_library, name='update-zwave-library'
    ),
]
