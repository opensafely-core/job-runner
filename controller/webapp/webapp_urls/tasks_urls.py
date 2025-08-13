from django.urls import path

from controller.webapp.views import task_views


urlpatterns = [
    # health check
    path("", task_views.index),
    # task-related endpoints, called by Agents
    path("<str:backend>/tasks/", task_views.active_tasks, name="active_tasks"),
    path("<str:backend>/task/update/", task_views.update_task, name="update_task"),
]
