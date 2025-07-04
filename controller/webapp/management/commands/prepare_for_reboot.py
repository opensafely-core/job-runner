from django.core.management.base import BaseCommand

from jobrunner.cli.controller import prepare_for_reboot


class Command(BaseCommand):
    """
    Ops utility for killing all running jobs and reseting them to PENDING so they will be
    automatically re-run after a reboot.
    """

    def add_arguments(self, parser):
        prepare_for_reboot.add_parser_args(parser)

    def handle(self, **options):
        backend = options["backend"]
        status = options["status"]

        prepare_for_reboot.main(backend, status)
