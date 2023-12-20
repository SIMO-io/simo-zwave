# Generated by Django 2.2.12 on 2021-09-24 11:57

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ZwaveNode',
            fields=[
                ('name', models.CharField(blank=True, default='', max_length=200)),
                ('node_id', models.PositiveIntegerField(editable=False, primary_key=True, serialize=False, unique=True)),
                ('product_name', models.CharField(editable=False, max_length=200)),
                ('product_type', models.CharField(editable=False, max_length=200)),
                ('battery_level', models.PositiveIntegerField(editable=False, null=True)),
                ('alive', models.BooleanField(default=True)),
                ('stats', models.JSONField(default=dict)),
                ('is_new', models.BooleanField(default=True, editable=False)),
                ('gateway', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='zwave_nodes', to='core.Gateway')),
            ],
        ),
        migrations.CreateModel(
            name='NodeValue',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('genre', models.CharField(db_index=True, max_length=100)),
                ('value_id', models.BigIntegerField()),
                ('index', models.PositiveIntegerField(blank=True, db_index=True, editable=False, null=True)),
                ('label', models.CharField(max_length=200)),
                ('is_read_only', models.BooleanField()),
                ('type', models.CharField(max_length=100)),
                ('units', models.CharField(blank=True, max_length=100)),
                ('value_choices', models.JSONField(default=list, editable=False)),
                ('value', models.JSONField(blank=True, null=True)),
                ('value_new', models.JSONField(blank=True, null=True)),
                ('name', models.CharField(blank=True, help_text='Give it a name to allow use in components.', max_length=100, null=True)),
                ('component', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='zwave_node_val', to='core.Component')),
                ('node', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='node_values', to='simo_zwave.ZwaveNode')),
            ],
            options={
                'ordering': ('-genre', 'index'),
                'unique_together': {('node', 'value_id')},
            },
        ),
    ]
