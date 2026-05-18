from django.core.management.base import BaseCommand

from common import config as common_config
from controller.cli import flags as flags_cli


class Command(BaseCommand):
    """
    Manually enable or disable database maintenance mode.
    By default, checks for automatically detected maintenance mode (i.e. DBSTATUS tasks)
    continue to run during manual maintenance mode.
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
        parser.add_argument(
            "--disable-checks",
            action="store_true",
            help="Disable DB_STATUS task checks",
        )

    def handle(self, action, backend, **options):
        if action == "on":
            flags = [("mode", "db-maintenance"), ("manual-db-maintenance", "on")]
            if options["disable_checks"]:
                flags.append(("db-maintenance-checks-disabled", "true"))
            else:
                # If the disable-checks option isn't present, make sure that any previously
                # set flag has been cleared
                flags.append(("db-maintenance-checks-disabled", None))
        else:
            flags = [
                ("mode", None),
                ("manual-db-maintenance", None),
                ("db-maintenance-checks-disabled", None),
            ]

        flags_cli.main(backend, "set", flags)
