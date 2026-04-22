from __future__ import annotations

from datetime import datetime
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from .forms import ServerSyncForm
from .models import ProcessingRun
from .services.adquisicion_datos import (
    carregar_execucao_dataframe,
    construir_historico_filtrado,
    obter_payload_tempo_real,
    processar_execucao_servidor,
)


@require_GET
def home(request, pk=None):
    form = ServerSyncForm()
    execucao_selecionada = None

    if pk is not None:
        execucao_selecionada = get_object_or_404(ProcessingRun, pk=pk)
    else:
        execucao_selecionada = ProcessingRun.objects.order_by('-criado_em').first()

    dashboard_data = {}
    if execucao_selecionada and isinstance(execucao_selecionada.resumo, dict):
        dashboard_data = execucao_selecionada.resumo.get('dashboard', {}) or {}

    return render(
        request,
        'core/home.html',
        {
            'form': form,
            'execucao_selecionada': execucao_selecionada,
            'ultimas_execucoes': ProcessingRun.objects.all()[:10],
            'dashboard_data': dashboard_data,
            'active_tab': request.GET.get('tab', 'historico'),
        },
    )


@require_POST
def sincronizar_servidor(request):
    form = ServerSyncForm(request.POST)
    if not form.is_valid():
        execucao_selecionada = ProcessingRun.objects.order_by('-criado_em').first()
        dashboard_data = {}
        if execucao_selecionada and isinstance(execucao_selecionada.resumo, dict):
            dashboard_data = execucao_selecionada.resumo.get('dashboard', {}) or {}
        return render(
            request,
            'core/home.html',
            {
                'form': form,
                'execucao_selecionada': execucao_selecionada,
                'ultimas_execucoes': ProcessingRun.objects.all()[:10],
                'dashboard_data': dashboard_data,
                'active_tab': 'historico',
            },
            status=400,
        )

    parametros_busca = {
        'filtro': form.cleaned_data.get('filtro') or '',
        'data_inicio': form.cleaned_data.get('data_inicio').isoformat() if form.cleaned_data.get('data_inicio') else '',
        'data_fim': form.cleaned_data.get('data_fim').isoformat() if form.cleaned_data.get('data_fim') else '',
    }

    nome = form.cleaned_data.get('nome') or f"Sincronização {timezone.localtime().strftime('%Y-%m-%d %H:%M:%S')}"

    execucao = ProcessingRun.objects.create(
        nome=nome,
        origem='ftp_legado',
        parametros_busca=parametros_busca,
        status=ProcessingRun.Status.PENDING,
    )
    processar_execucao_servidor(execucao)
    return redirect('execucao_detalhe', pk=execucao.pk)


@require_POST
def sincronizar_novamente(request, pk):
    original = get_object_or_404(ProcessingRun, pk=pk)
    nova_execucao = ProcessingRun.objects.create(
        nome=f'{original.nome} - refresh',
        origem=original.origem,
        parametros_busca=original.parametros_busca,
        status=ProcessingRun.Status.PENDING,
    )
    processar_execucao_servidor(nova_execucao)
    tab = request.POST.get('tab') or request.GET.get('tab') or 'historico'
    return redirect(f'/execucao/{nova_execucao.pk}/?tab={tab}')


@require_GET
def execucao_api(request, pk):
    execucao = get_object_or_404(ProcessingRun, pk=pk)
    resumo = execucao.resumo if isinstance(execucao.resumo, dict) else {}
    payload = {
        'id': execucao.pk,
        'nome': execucao.nome,
        'origem': execucao.origem,
        'parametros_busca': execucao.parametros_busca,
        'status': execucao.status,
        'status_label': execucao.get_status_display(),
        'total_linhas': execucao.total_linhas,
        'total_colunas': execucao.total_colunas,
        'colunas': execucao.colunas,
        'resumo': resumo,
        'dashboard': resumo.get('dashboard', {}),
        'arquivo_url': execucao.arquivo.url if execucao.arquivo else None,
        'erro': execucao.erro,
        'criado_em': execucao.criado_em.isoformat(),
        'atualizado_em': execucao.atualizado_em.isoformat(),
    }
    return JsonResponse(payload)


@require_GET
def execucao_historico_api(request, pk):
    execucao = get_object_or_404(ProcessingRun, pk=pk)
    df = carregar_execucao_dataframe(execucao)
    payload = construir_historico_filtrado(
        df,
        freq=request.GET.get('freq'),
        start=request.GET.get('start'),
        end=request.GET.get('end'),
    )
    payload['execution'] = {
        'id': execucao.pk,
        'nome': execucao.nome,
        'status': execucao.status,
        'status_label': execucao.get_status_display(),
    }
    return JsonResponse(payload)


@require_GET
def exportar_execucao_csv(request, pk):
    execucao = get_object_or_404(ProcessingRun, pk=pk)
    df = carregar_execucao_dataframe(execucao)
    start = request.GET.get('start')
    end = request.GET.get('end')

    if not df.empty:
        if start:
            try:
                df = df[df.index >= datetime.fromisoformat(start)]
            except Exception:
                pass
        if end:
            try:
                df = df[df.index <= datetime.fromisoformat(end)]
            except Exception:
                pass

    if df.empty:
        raise Http404('Não há dados para exportar nessa janela.')

    csv_bytes = df.reset_index().rename(columns={df.index.name or 'index': 'timestamp'}).to_csv(index=False).encode('utf-8')
    response = HttpResponse(csv_bytes, content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="execucao_{execucao.pk}_filtrada.csv"'
    return response


@require_GET
def tempo_real_api(request):
    return JsonResponse(obter_payload_tempo_real())
