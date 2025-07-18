import time

from django.core.management.base import BaseCommand
from django.db.models import F, Window
from django.db.models.functions import FirstValue

from controller.queries import calculate_workspace_state
from controller.webapp.models import Job


def time_func(func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


def calculate_workspace_state_qs(backend, workspace):
    included_qs = Job.objects.filter(
        workspace=workspace, backend=backend, cancelled=False
    ).exclude(action="__error__")
    ret = included_qs.annotate(
        latest_pk=Window(
            expression=FirstValue("pk"),
            partition_by=["action"],
            order_by=F("created_at").desc(),
        )
    ).filter(pk=F("latest_pk"))
    # breakpoint()
    return ret


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("backend")
        parser.add_argument("workspace")

    def handle(self, *args, **options):
        backend = options["backend"]
        workspace = options["workspace"]

        # Compare `calculate_workspace_state` performance.
        (all_jobs, old_jobs), old_time = time_func(
            calculate_workspace_state, backend, workspace
        )
        new_jobs, new_time = time_func(calculate_workspace_state_qs, backend, workspace)
        self.stdout.write(f"Original: {len(old_jobs)} jobs, {old_time:.4f} sec")
        self.stdout.write(f"Queryset: {len(new_jobs)} jobs, {new_time:.4f} sec")
        self.stdout.write(f"ratio: {old_time / new_time:.1f}")

        # Check we actually get same results.
        old_ids = {job.id for job in old_jobs}
        new_ids = {job.id for job in new_jobs}
        assert old_ids == new_ids
