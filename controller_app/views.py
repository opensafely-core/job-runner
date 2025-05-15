import json
import logging

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from jobrunner.controller.task_api import get_active_tasks, handle_task_update
from jobrunner.schema import AgentTask


log = logging.getLogger(__name__)


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response


def active_tasks(request, backend):
    active_tasks = [
        AgentTask.from_task(task).asdict() for task in get_active_tasks(backend)
    ]
    return JsonResponse({"tasks": active_tasks})


@require_POST
@csrf_exempt
def update_task(request, backend):
    update_task_info = json.loads(request.POST.get("payload"))

    task_id = update_task_info["task_id"]
    stage = update_task_info["stage"]
    # If the agent posts an empty results dict, it won't be present in the POST data

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
