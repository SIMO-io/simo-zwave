import time
from django.utils.translation import gettext_lazy as _
from django.urls import reverse_lazy, path
from django.contrib import admin
from django.contrib import messages
from django.views.generic import TemplateView
from django.shortcuts import render, redirect
from django.utils.safestring import mark_safe
from simo.core.events import GatewayObjectCommand
from simo.core.models import Gateway
from .models import ZwaveNode, NodeValue
from .forms import AdminNodeValueInlineForm, ZwaveGatewaySelectForm




class NodeValueInline(admin.TabularInline):
    form = AdminNodeValueInlineForm
    model = NodeValue
    extra = 0
    fields = (
        'index', 'label',  'type', 'value', 'units',
        'name', 'component_display'
    )
    readonly_fields = (
        'index', 'label', 'units', 'type', 'component_display', 'type'
    )
    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request, obj):
        return False

    def component_display(self, obj):
        if obj.component:
            return mark_safe('<div style="min-width: 200px"><a href="%s"><strong>%s</strong>%s >></a></div>' % (
                obj.component.get_admin_url(),
                obj.component.name,
                ' in %s' % obj.component.zone.name
                if obj.component.zone else ''
            ))
        return ''

    component_display.short_description = _("component")



@admin.register(ZwaveNode)
class ZwaveNodeAdmin(admin.ModelAdmin):
    change_list_template = ''
    list_display = '__str__', 'battery_level', 'alive'
    readonly_fields = (
        'node_id', 'product_name', 'product_type', 'battery_level',
        'stats', 'alive',
    )
    inlines = NodeValueInline,
    actions = [
        'kill_node', 'request_network_update', 'send_node_information',
        'has_node_failed'
    ]

    # def has_delete_permission(self, request, obj=None):
    #     if obj and not obj.alive:
    #         return True
    #     return False

    def response_post_save_change(self, request, obj):
        # Let's give some time for these values to be applied
        # in case zwave node is active and directly reachable
        time.sleep(1)
        obj = ZwaveNode.objects.get(pk=obj.pk)
        return self._response_post_save(request, obj)

    def get_urls(self):
        urls = super().get_urls()
        return [
            path('remove-nodes/', self.remove_nodes)
        ] + urls

    def add_view(self, request, form_url="", extra_context=None):
        ctx = {"opts": self.model._meta, "media": self.media}

        from .gateways import ZwaveGatewayHandler
        zwave_gateways = Gateway.objects.filter(type=ZwaveGatewayHandler.uid)

        if zwave_gateways.count() == 0:
            messages.error(request, "Please create Zwave gateway first!")
            return redirect(reverse_lazy('admin:core_gateway_add'))

        if zwave_gateways.count() == 1:
            request.session['add_nodes_gateway_pk'] = zwave_gateways.first().pk

        if not request.session.get('add_nodes_gateway_pk'):

            ctx['form'] = ZwaveGatewaySelectForm()
            if request.method == 'POST':
                form = ZwaveGatewaySelectForm(request.POST)
                if form.is_valid():
                    request.session['add_nodes_gateway_pk'] = \
                        form.cleaned_data['gateway'].pk
                else:
                    return render(request, 'zwave_node_choose_gateway.html',
                                  ctx)
            else:
                return render(request, 'zwave_node_choose_gateway.html', ctx)


        if request.session.get('add_nodes_gateway_pk'):
            ctx['gateway'] = Gateway.objects.get(
                pk=request.session['add_nodes_gateway_pk']
            )
            if 'new_nodes_only' not in request.GET:
                GatewayObjectCommand(
                    ctx['gateway'], zwave_command='add_node'
                ).publish()
            ctx['gateway'].config['last_controller_command'] = time.time()
            ctx['gateway'].save()
            ctx['zwave_nodes'] = ZwaveNode.objects.filter(
                gateway=ctx['gateway'], is_new=True
            )

            if request.method == 'POST' and 'finish' in request.POST:
                GatewayObjectCommand(
                    ctx['gateway'], zwave_command='cancel_command'
                ).publish()
                request.session.pop('add_nodes_gateway_pk')
                ctx['new_nodes'] = ZwaveNode.objects.filter(
                    gateway=ctx['gateway'], is_new=True
                ).update(is_new=False)
                return redirect(
                    reverse_lazy('admin:zwave_zwavenode_changelist')
                )
        if 'new_nodes_only' in request.GET:
            return render(request, 'zwave_nodes.html', ctx)
        return render(request, 'add_zwave_node.html', ctx )


    def remove_nodes(self, request):
        ctx = {"opts": self.model._meta, "media": self.media}
        from .gateways import ZwaveGatewayHandler

        zwave_gateways = Gateway.objects.filter(type=ZwaveGatewayHandler.uid)

        if zwave_gateways.count() == 0:
            return redirect(reverse_lazy('admin:zwave_zwavenode_changelist'))

        if zwave_gateways.count() == 1:
            request.session['remove_nodes_gateway_pk'] = zwave_gateways.first().pk

        if not request.session.get('remove_nodes_gateway_pk'):

            ctx['form'] = ZwaveGatewaySelectForm()
            if request.method == 'POST':
                form = ZwaveGatewaySelectForm(request.POST)
                if form.is_valid():
                    request.session['remove_nodes_gateway_pk'] = \
                        form.cleaned_data['gateway'].pk
                else:
                    return render(request, 'zwave_node_choose_gateway.html',
                                  ctx)
            else:
                return render(request, 'zwave_node_choose_gateway.html', ctx)

        if request.session.get('remove_nodes_gateway_pk'):
            ctx['gateway'] = Gateway.objects.get(
                pk=request.session['remove_nodes_gateway_pk']
            )
            if 'removed_nodes_only' not in request.GET:
                GatewayObjectCommand(
                    ctx['gateway'], zwave_command='remove_node'
                ).publish()
                request.session['org_nodes_list'] = [
                    str(n) for n in
                    ZwaveNode.objects.filter(gateway=ctx['gateway'])
                ]
            ctx['gateway'].config['last_controller_command'] = time.time()
            ctx['gateway'].save()
            current_nodes = [
                str(n) for n in ZwaveNode.objects.filter(gateway=ctx['gateway'])
            ]
            ctx['zwave_nodes'] = [
                n for n in request.session.get('org_nodes_list')
                if n not in current_nodes
            ]

            if request.method == 'POST' and 'finish' in request.POST:
                GatewayObjectCommand(
                    ctx['gateway'], zwave_command='cancel_command'
                ).publish()
                request.session.pop('remove_nodes_gateway_pk')
                request.session.pop('org_nodes_list')
                return redirect(
                    reverse_lazy('admin:zwave_zwavenode_changelist')
                )
        if 'removed_nodes_only' in request.GET:
            return render(request, 'zwave_nodes.html', ctx)
        return render(request, 'remove_zwave_node.html', ctx )

    def kill_node(self, request, queryset):
        for node in queryset:
            node.kill_node()

    def request_network_update(self, request, queryset):
        for node in queryset:
            node.request_network_update()


    def send_node_information(self, request, queryset):
        for node in queryset:
            node.send_node_information()


    def has_node_failed(self, request, queryset):
        for node in queryset:
            node.has_node_failed()
