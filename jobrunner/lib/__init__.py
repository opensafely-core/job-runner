import functools
import secrets
import warnings
from contextlib import contextmanager
from datetime import datetime


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


def datestr_to_ns_timestamp(datestr):
    """Parses a datestring with nanoseconds in it into integer ns timestamp.

    Stdlib datetime cannot natively parse nanoseconds, so we use it to parse
    the date and handle timezones, and then handle the ns ourselves.
    """
    # truncate to ms
    iso = datestr[:26]

    if datestr[26:29].isdigit():
        # we have nanoseconds
        ns = int(datestr[26:29])
        tz = datestr[29:].strip()
    else:
        ns = 0
        tz = datestr[26:].strip()

    if tz:
        # datetime.fromisoformat can't handle the Z in python < 3.11
        if tz == "Z":
            iso += "+00:00"
        # it also requires a : for timezones before 3.11
        elif ":" not in tz:
            iso += tz[0:3] + ":" + tz[3:5]
        else:
            iso += tz

    try:
        ts = int(datetime.fromisoformat(iso).timestamp() * 1e9)
    except ValueError:
        return None

    # re add the ns component
    ts += ns

    return ts


def ns_timestamp_to_datetime(timestamp_ns):
    """Debugging helper function to make ns timestamps human readable.

    We do lose 3 levels of precision, as datetime can only handle microseconds,
    but for human comparison that doesn't matter.
    """
    return datetime.fromtimestamp(timestamp_ns / 1e9)


def warn_assertions(f):
    """Helper decorator to catch assertions errors and emit as warnings.

    In dev, this will cause tests to fail, and log output in prod.

    Returns None, as that's the only thing it can reasonably do.  As such, it
    can only be used to decorate functions that also return None, and it emits
    a warning for that too.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            rvalue = f(*args, **kwargs)
            if rvalue is not None:
                raise AssertionError(
                    "warn_assertions can only be used on functions that return None:"
                    "{f.__name__} return {type(rvalue)}"
                )
        except AssertionError as exc:
            # convert exception to warning
            warnings.warn(str(exc))

        return None

    return wrapper
