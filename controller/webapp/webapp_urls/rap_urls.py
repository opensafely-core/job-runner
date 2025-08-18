from django.urls import path

from controller.webapp.views import rap_views


urlpatterns = [
    # RAP API endpoints
    # Documentation
    path("api-docs/", rap_views.api_docs),
    path("api_spec.json", rap_views.api_spec),
    # Backend status for all backends (that are allowed for client's token)
    path("backend/status/", rap_views.backends_status, name="backends_status"),
    # Cancel one or more actions for a single job request
    path("rap/cancel/", rap_views.cancel, name="cancel"),
]
