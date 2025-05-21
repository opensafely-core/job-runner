import logging

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from opentelemetry import trace

from jobrunner.controller.task_api import get_active_tasks, handle_task_update
from jobrunner.schema import AgentTask
from jobrunner.tracing import set_span_attributes


log = logging.getLogger(__name__)


def trace_attributes(**attrs):
    span = trace.get_current_span()
    set_span_attributes(span, attrs)


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response


def active_tasks(request, backend):
    trace_attributes(backend=backend)
    active_tasks = [
        AgentTask.from_task(task).asdict() for task in get_active_tasks(backend)
    ]
    return JsonResponse({"tasks": active_tasks})


@require_POST
def update_task(request, backend):
    update_task_info = request.POST
    task_id = update_task_info["task_id"]
    stage = update_task_info["stage"]
    # If the agent posts an empty results dict, it won't be present in the POST data
    results = update_task_info.get("results", {})
    complete = update_task_info["complete"]
    timestamp_ns = update_task_info["timestamp_ns"]

    trace_attributes(backend=backend, task_id=task_id)

    try:
        handle_task_update(
            task_id=task_id,
            stage=stage,
            results=results,
            complete=complete,
            timestamp_ns=timestamp_ns,
        )
    except Exception:
        log.exception("Error updating task")
        return JsonResponse({"error": "Error updating task"}, status=500)

    return HttpResponse(status=204)
