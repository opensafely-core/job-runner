import logging
import os


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


def safe_join(startdir, path):
    requested_path = os.path.normpath(os.path.join(startdir, path))
    startdir = str(startdir)  # Normalise from PosixPath
    assert (
        os.path.commonprefix([requested_path, startdir]) == startdir
    ), f"Invalid requested path {requested_path}, not in {startdir}"
    return requested_path
