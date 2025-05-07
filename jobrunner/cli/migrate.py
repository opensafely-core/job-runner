import sys

from jobrunner.cli.controller import migrate


if __name__ == "__main__":
    migrate.run(sys.argv[1:])
