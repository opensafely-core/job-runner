from ruamel.yaml import YAML, error


class YAMLError(Exception):
    pass


def parse_yaml(file_contents, name="yaml file"):
    """
    Parse a YAML file supplied as bytes into a dictionary

    Args:
        file_contents: file contents as bytes
        name: optional name of the file for producing more readable error
            messages

    Returns:
        parsed contents as a dictionary

    Raises:
        YAMLError
    """
    try:
        # We're using the pure-Python version here as we don't care about speed
        # and this gives better error messages (and consistent behaviour
        # cross-platform)
        return YAML(typ="safe", pure=True).load(file_contents)
    # ruamel doesn't have a nice exception hierarchy so we have to catch these
    # four separate base classes
    except (
        error.YAMLError,
        error.YAMLStreamError,
        error.YAMLWarning,
        error.YAMLFutureWarning,
    ) as exc:
        exc = make_yaml_error_more_helpful(exc, name)
        raise YAMLError(f"{type(exc).__name__} {exc}")


def make_yaml_error_more_helpful(exc, name):
    """
    ruamel produces quite helpful error messages but they refer to the file as
    `<byte_string>` (which will be confusing for users) and they also include
    notes and warnings to developers about API changes. This function attempts
    to fix these issues, but just returns the exception unchanged if anything
    goes wrong.
    """
    try:
        try:
            exc.context_mark.name = name
        except AttributeError:
            pass
        try:
            exc.problem_mark.name = name
        except AttributeError:
            pass
        exc.note = ""
        exc.warn = ""
    except Exception:
        pass
    return exc
