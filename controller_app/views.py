import json
import logging
from functools import wraps

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from jobrunner.config import common as common_config
from jobrunner.config import controller as config
from jobrunner.controller.task_api import get_active_tasks, handle_task_update
from jobrunner.schema import AgentTask


log = logging.getLogger(__name__)


def require_backend_authentication(view_fn):
    """Ensure a valid authentication token was received"""

    @wraps(view_fn)
    def wrapped_view(request, backend):
        if backend not in common_config.BACKENDS:
            return JsonResponse(
                {"error": "Not found", "details": f"Backend '{backend}' not found"},
                status=404,
            )
        token = request.headers.get("Authorization")
        error = None
        if not token:
            error = "No token provided"
        elif token != config.JOB_SERVER_TOKENS[backend]:
            error = f"Invalid token for backend '{backend}'"

        if error:
            return JsonResponse({"error": "Unauthorized", "details": error}, status=401)

        return view_fn(request, backend)

    return wrapped_view


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response


@require_backend_authentication
def active_tasks(request, backend):
    active_tasks = [
        AgentTask.from_task(task).asdict() for task in get_active_tasks(backend)
    ]
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

    try:
        handle_task_update(
            task_id=task_id, stage=stage, results=results, complete=complete
        )
    except Exception:
        log.exception("Error updating task")
        return JsonResponse({"error": "Error updating task"}, status=500)

    return JsonResponse({"response": "Update successful"}, status=200)
