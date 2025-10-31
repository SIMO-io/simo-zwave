from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('simo_zwave', '0002_alter_nodevalue_genre'),
    ]

    operations = [
        migrations.AddField(
            model_name='nodevalue',
            name='command_class',
            field=models.IntegerField(blank=True, null=True, db_index=True),
        ),
        migrations.AddField(
            model_name='nodevalue',
            name='endpoint',
            field=models.IntegerField(blank=True, null=True, db_index=True),
        ),
        migrations.AddField(
            model_name='nodevalue',
            name='property',
            field=models.CharField(max_length=120, blank=True, null=True),
        ),
        migrations.AddField(
            model_name='nodevalue',
            name='property_key',
            field=models.CharField(max_length=120, blank=True, null=True),
        ),
    ]

