import sys

from jobrunner.cli.controller import flags


if __name__ == "__main__":
    flags.run(sys.argv[1:])
