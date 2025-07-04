from django.core.management.base import BaseCommand

from controller.cli import migrate


class Command(BaseCommand):
    """
    Migrate the controller database.
    """

    def add_arguments(self, parser):
        migrate.add_parser_args(parser)

    def handle(self, **options):
        dbpath = options["dbpath"]
        migrate.main(dbpath)
