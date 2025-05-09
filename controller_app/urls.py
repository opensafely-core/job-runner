from django.urls import path

from controller_app import views


urlpatterns = [
    path("", views.index),
    path("<str:backend>/tasks/", views.active_tasks, name="active_tasks"),
    path("<str:backend>/task/update/", views.update_task, name="update_task"),
]
