from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden, HttpResponse
from django.shortcuts import redirect
from django.contrib import messages
from .utils import get_latest_ozw_library


@login_required
def update_zwave_library(request):
    if not request.user.is_superuser:
        return HttpResponseForbidden()
    try:
        error = get_latest_ozw_library()
    except Exception as e:
        messages.error(request, str(e))
    else:
        if error:
            messages.error(request, str(error))
        else:
            messages.success(request, "Open OZW devices library updated!")
    if request.META.get('HTTP_REFERER'):
        return redirect(request.META.get('HTTP_REFERER'))
    return HttpResponse('Updated!')

