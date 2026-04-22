from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path

from core import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.home, name='home'),
    path('sincronizar/', views.sincronizar_servidor, name='sincronizar_servidor'),
    path('execucao/<int:pk>/', views.home, name='execucao_detalhe'),
    path('execucao/<int:pk>/refresh/', views.sincronizar_novamente, name='sincronizar_novamente'),
    path('execucao/<int:pk>/export/csv/', views.exportar_execucao_csv, name='exportar_execucao_csv'),
    path('api/execucao/<int:pk>/', views.execucao_api, name='execucao_api'),
    path('api/execucao/<int:pk>/historico/', views.execucao_historico_api, name='execucao_historico_api'),
    path('api/tempo-real/', views.tempo_real_api, name='tempo_real_api'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
