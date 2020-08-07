import logging
import os

from tinynetrc import Netrc
from netrc import NetrcParseError


def getlogger(name):
    # Create a logger with a field for recording a unique job id, and a
    # `baselogger` adapter which fills this field with a hyphen, for use
    # when logging events not associated with jobs
    FORMAT = "%(asctime)-15s %(levelname)-10s  %(job_id)-10s %(message)s"
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger


def get_auth():
    return (os.environ["QUEUE_USER"], os.environ["QUEUE_PASS"])


def set_auth():
    """Set HTTP auth (used by `requests`)
    """
    netrc_path = os.path.join(os.path.expanduser("~"), ".netrc")
    try:
        netrc = Netrc()
    except (NetrcParseError, FileNotFoundError):
        with open(netrc_path, "w") as f:
            f.write("")
        netrc = Netrc()

    if netrc["github.com"]["password"]:
        login = netrc["github.com"]["login"]
        password = netrc["github.com"]["password"]
    else:
        password = os.environ["PRIVATE_REPO_ACCESS_TOKEN"]
        login = "doesntmatter"
        netrc["github.com"] = {
            "login": login,
            "password": password,
        }
        netrc.save()
    return (login, password)


def safe_join(startdir, path):
    requested_path = os.path.normpath(os.path.join(startdir, path))
    startdir = str(startdir)  # Normalise from PosixPath
    assert (
        os.path.commonprefix([requested_path, startdir]) == startdir
    ), f"Invalid requested path {requested_path}, not in {startdir}"
    return requested_path
