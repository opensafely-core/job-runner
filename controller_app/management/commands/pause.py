from django.core.management.base import BaseCommand

from jobrunner.cli.controller import flags as flags_cli
from jobrunner.config import common as common_config


class Command(BaseCommand):
    """
    Start or stop accepting new jobs on a backend
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "action",
            type=str.lower,
            choices=("on", "off"),
        )
        parser.add_argument(
            "backend",
            type=str.lower,
            choices=common_config.BACKENDS,
        )

    def handle(self, action, backend, **options):
        if action == "on":
            flags = [("paused", "true")]
        else:
            flags = [("paused", None)]

        flags_cli.main(backend, "set", flags)
