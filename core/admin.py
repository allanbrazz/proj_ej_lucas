from django.contrib import admin

from .models import ProcessingRun


@admin.register(ProcessingRun)
class ProcessingRunAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'nome',
        'origem',
        'status',
        'total_linhas',
        'total_colunas',
        'criado_em',
    )
    list_filter = ('status', 'origem', 'criado_em')
    search_fields = ('nome',)
    readonly_fields = ('criado_em', 'atualizado_em')
