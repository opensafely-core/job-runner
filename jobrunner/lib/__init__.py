import secrets
from contextlib import contextmanager


@contextmanager
def atomic_writer(dest):
    """Return a safe temp file on the same filesystem to write to

    On success, the tmp file is renamed to the original target atomically.
    If the write fails, ensure the tmp file is deleted
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + f".{secrets.token_hex(8)}.tmp")
    try:
        yield tmp
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    else:
        tmp.replace(dest)
