from django.core.management.base import BaseCommand

from controller.cli import add_job


class Command(BaseCommand):
    """
    Development utility for creating and submitting a RAP without having a
    job-server
    """

    def add_arguments(self, parser):
        add_job.add_parser_args(parser)

    def handle(self, repo_url, actions, **options):
        backend = options["backend"]
        commit = options["commit"]
        branch = options["branch"]
        workspace = options["workspace"]
        database = options["database"]
        force_run_dependencies = options["force_run_dependencies"]

        add_job.main(
            repo_url,
            actions,
            backend,
            commit,
            branch,
            workspace,
            database,
            force_run_dependencies,
        )
