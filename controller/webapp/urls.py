from django.urls import include, path

from controller.webapp.webapp_urls import rap_urls, tasks_urls


urlpatterns = [
    # health check
    # task-related endpoints, called by Agents
    path("", include(tasks_urls)),
    # RAP API endpoints
    path("controller/v1/", include(rap_urls)),
]
