from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from jobrunner.controller.task_api import get_active_tasks


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response


def active_tasks(request, backend):
    active_tasks = [task.asdict() for task in get_active_tasks(backend)]
    return JsonResponse({"tasks": active_tasks})
