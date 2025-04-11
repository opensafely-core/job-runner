from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt


@csrf_exempt
def index(request):
    response = JsonResponse({"method": request.method})
    return response
