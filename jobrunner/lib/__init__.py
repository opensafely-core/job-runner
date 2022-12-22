import functools
import secrets
import warnings
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


def warn_assertions(reraise=True):
    """Helper decorator to emit any failed assertions as warnings.

    reraise=False will not raise the assertion error further.

    Designed to be used to ensure tests fail in dev, but are ignored in prod.
    Note that the AssertionError is still raised, so will need catching, just
    like any other error.  We don't know what to return, so re-raising the
    exception is the only option.
    """

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except AssertionError as exc:
                warnings.warn(str(exc))
                if reraise:
                    raise

        return wrapper

    return decorator
