from django.urls import path

from controller_app import views


urlpatterns = [
    path("", views.index),
]
