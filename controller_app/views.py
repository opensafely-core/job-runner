from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from jobrunner.controller.task_api import get_active_tasks, handle_task_update


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response


def active_tasks(request, backend):
    active_tasks = [task.asdict() for task in get_active_tasks(backend)]
    return JsonResponse({"tasks": active_tasks})


@require_POST
def update_task(request, backend):
    update_task_info = request.POST

    task_id = update_task_info["task_id"]
    stage = update_task_info["stage"]
    results = update_task_info["results"]
    complete = update_task_info["complete"]

    handle_task_update(task_id=task_id, stage=stage, results=results, complete=complete)

    return HttpResponse(status=204)
