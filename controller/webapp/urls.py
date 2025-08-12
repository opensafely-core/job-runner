from django.urls import path

from controller.webapp.views import rap_views, task_views


urlpatterns = [
    # health check
    path("", task_views.index),
    # task-related endpoints, called by Agents
    path("<str:backend>/tasks/", task_views.active_tasks, name="active_tasks"),
    path("<str:backend>/task/update/", task_views.update_task, name="update_task"),
    # RAP API endpoints
    # backend status for a specific backend
    path("api-docs/", rap_views.api_docs),
    path("api_spec.json", rap_views.api_spec),
    # backend status for a all backends (allowed by token)
    path("backend/status/", rap_views.backends_status, name="backends_status"),
    # Cancel one or more actions
    path("rap/cancel/", rap_views.cancel, name="cancel"),
]
