# fraud/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('predict/', views.FraudPredictionAPIView.as_view(), name='fraud-predict'),
    path('health/', views.ModelHealthAPIView.as_view(), name='fraud-health'),
]