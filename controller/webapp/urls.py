from django.urls import path

from controller.webapp.views import task_views


urlpatterns = [
    path("", task_views.index),
    path("<str:backend>/tasks/", task_views.active_tasks, name="active_tasks"),
    path("<str:backend>/task/update/", task_views.update_task, name="update_task"),
]
