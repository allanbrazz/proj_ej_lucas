from django.db import models


class ProcessingRun(models.Model):
    class Status(models.TextChoices):
        PENDING = 'pending', 'Pendente'
        SUCCESS = 'success', 'Sucesso'
        ERROR = 'error', 'Erro'

    nome = models.CharField(max_length=150)
    origem = models.CharField(max_length=50, default='server')
    parametros_busca = models.JSONField(default=dict, blank=True)
    arquivo = models.FileField(upload_to='uploads/%Y/%m/%d/', blank=True, null=True)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
    )

    total_linhas = models.PositiveIntegerField(default=0)
    total_colunas = models.PositiveIntegerField(default=0)
    colunas = models.JSONField(default=list, blank=True)
    resumo = models.JSONField(default=dict, blank=True)
    erro = models.TextField(blank=True)

    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-criado_em']
        verbose_name = 'Execução'
        verbose_name_plural = 'Execuções'

    def __str__(self) -> str:
        return f'{self.nome} ({self.get_status_display()})'
