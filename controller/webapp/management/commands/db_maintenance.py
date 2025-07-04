from django.core.management.base import BaseCommand

from common import config as common_config
from jobrunner.cli.controller import flags as flags_cli


class Command(BaseCommand):
    """
    Manually enable or disable database maintenance mode
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
            flags = [("mode", "db-maintenance"), ("manual-db-maintenance", "on")]
        else:
            flags = [("mode", None), ("manual-db-maintenance", None)]

        flags_cli.main(backend, "set", flags)
