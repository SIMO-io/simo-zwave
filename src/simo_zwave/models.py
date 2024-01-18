import sys
from django.db import models
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from simo.core.models import Gateway, Component
from simo.core.events import GatewayObjectCommand




class ZwaveNode(models.Model):
    name = models.CharField(
        max_length=200, blank=True, default=''
    )
    gateway = models.ForeignKey(
        Gateway, on_delete=models.CASCADE, related_name='zwave_nodes'
    )
    node_id = models.PositiveIntegerField(
        primary_key=True, unique=True, editable=False
    )
    product_name = models.CharField(max_length=200, editable=False)
    product_type = models.CharField(max_length=200, editable=False)
    battery_level = models.PositiveIntegerField(null=True, editable=False)
    alive = models.BooleanField(default=True)
    stats = models.JSONField(default=dict)
    is_new = models.BooleanField(default=True, editable=False)

    def __str__(self):
        return '[%d] %s' % (
            self.node_id, self.name if self.name else self.product_name
        )

    def kill_node(self):
        print("kill node %s" % str(self.node_id))
        GatewayObjectCommand(
            self.gateway, zwave_command='remove_failed_node', node_id=self.node_id
        ).publish()

    def request_network_update(self):
        GatewayObjectCommand(
            self.gateway, zwave_command='request_network_update', node_id=self.node_id
        ).publish()

    def send_node_information(self):
        GatewayObjectCommand(
            self.gateway, zwave_command='send_node_information', node_id=self.node_id
        ).publish()

    def has_node_failed(self):
        GatewayObjectCommand(
            self.gateway, zwave_command='has_node_failed', node_id=self.node_id
        ).publish()


class NodeValue(models.Model):
    node = models.ForeignKey(
        ZwaveNode, on_delete=models.CASCADE, related_name='node_values'
    )
    genre = models.CharField(max_length=100, db_index=True, null=True)
    value_id = models.BigIntegerField()
    index = models.PositiveIntegerField(
        null=True, blank=True, editable=False, db_index=True
    )
    label = models.CharField(max_length=200)
    is_read_only = models.BooleanField()
    type = models.CharField(max_length=100)
    units = models.CharField(max_length=100, blank=True)
    value_choices = models.JSONField(default=list, editable=False)
    value = models.JSONField(null=True, blank=True)
    value_new = models.JSONField(null=True, blank=True)
    name = models.CharField(
        max_length=100, null=True, blank=True,
        help_text="Give it a name to allow use in components."
    )
    component = models.ForeignKey(
        Component, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='zwave_node_val'
    )

    class Meta:
        unique_together = 'node', 'value_id'
        ordering = '-genre', 'index',

    def __str__(self):
        return '%s | [%d] %s' % (
            str(self.node), self.id, self.name if self.name else self.label
        )

@receiver(post_save, sender=NodeValue)
def set_component_value_of_node_value(sender, instance, created, *args, **kwargs):
    '''Set component value to zwave node value on component creation'''
    if created:
        return
    org = NodeValue.objects.get(pk=instance.pk)
    if instance.component and org.component != instance.component:
        instance.component.set(instance.value)



@receiver(post_delete, sender=ZwaveNode)
def node_post_delete(sender, instance, *args, **kwargs):
    GatewayObjectCommand(
        instance.gateway, zwave_command='remove_failed_node', node_id=instance.node_id
    ).publish()


@receiver(post_save, sender=Gateway)
def update_zwave_library_on_new_zwave_gateway(
    sender, instance, created, *args, **kwargs
):
    if not created:
        return
    from .gateways import ZwaveGatewayHandler
    if instance.type == ZwaveGatewayHandler.uid and 'test' not in sys.argv:
        from .utils import get_latest_ozw_library
        get_latest_ozw_library()

