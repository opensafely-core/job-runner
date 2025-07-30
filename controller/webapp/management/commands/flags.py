from django.core.management.base import BaseCommand

from controller.cli import flags as flags_cli


class Command(BaseCommand):
    """
    Ops utility getting and setting db flags
    """

    def add_arguments(self, parser):
        flags_cli.add_parser_args(parser)

    def handle(self, action, flags, **options):
        backend = options["backend"]
        create = options["create"]

        flags_cli.main(backend, action, flags, create)
