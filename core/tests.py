from django.test import TestCase
from django.urls import reverse

from .models import ProcessingRun


class CoreViewsTestCase(TestCase):
    def test_home_status_code(self):
        response = self.client.get(reverse('core:home'))
        self.assertEqual(response.status_code, 200)

    def test_sincronizacao_demo(self):
        response = self.client.post(
            reverse('core:sincronizar_servidor'),
            data={
                'nome': 'Teste sincronizacao',
                'filtro': 'demo',
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ProcessingRun.objects.count(), 1)
        execucao = ProcessingRun.objects.first()
        self.assertEqual(execucao.status, ProcessingRun.Status.SUCCESS)
        self.assertGreater(execucao.total_linhas, 0)

    def test_execucao_api(self):
        execucao = ProcessingRun.objects.create(
            nome='Execucao API',
            origem='server',
            status=ProcessingRun.Status.SUCCESS,
            total_linhas=10,
            total_colunas=2,
            colunas=['a', 'b'],
            resumo={'preview': []},
        )
        response = self.client.get(reverse('core:execucao_api', args=[execucao.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['id'], execucao.pk)
