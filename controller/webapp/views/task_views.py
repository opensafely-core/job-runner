import json
import logging

from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from common.schema import AgentTask
from controller.queries import set_flag
from controller.task_api import get_active_tasks, handle_task_update
from controller.webapp.views.auth import require_backend_authentication
from controller.webapp.views.tracing import trace_attributes


log = logging.getLogger(__name__)


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response


@require_backend_authentication
def active_tasks(request, backend):
    trace_attributes(backend=backend)
    active_tasks = [
        AgentTask.from_task(task).asdict() for task in get_active_tasks(backend)
    ]
    # register that this backend has been in contact
    set_flag("last-seen-at", value=timezone.now().isoformat(), backend=backend)
    return JsonResponse({"tasks": active_tasks})


@require_backend_authentication
@require_POST
@csrf_exempt
def update_task(request, backend):
    update_task_info = json.loads(request.POST.get("payload"))

    task_id = update_task_info["task_id"]
    stage = update_task_info["stage"]
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

    return JsonResponse({"response": "Update successful"}, status=200)
