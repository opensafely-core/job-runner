from jobrunner.config import common as common_config


def add_backend_argument(parser, helptext=None):
    helptext = helptext or "backend to run this command on"
    parser.add_argument(
        "--backend",
        type=str.lower,
        required=True,
        choices=common_config.BACKENDS,
        help=helptext,
    )
