"""
Utility functions for interacting with Docker
"""
import json
import os
import re
import subprocess

from jobrunner import config
from jobrunner.lib import atomic_writer
from jobrunner.lib.subprocess_utils import subprocess_run


# Docker requires a container in order to interact with volumes, but it doesn't
# much matter what it is for our purposes as long as it has `sh` and `find`
MANAGEMENT_CONTAINER_IMAGE = f"{config.DOCKER_REGISTRY}/busybox"

# This path is pretty arbitrary: it sets where we mount volumes inside their
# management containers (which are used for copying files in and out), but this
# is independent of where the volumes get mounted inside other containers to
# which they may get attached
VOLUME_MOUNT_POINT = "/workspace"

# Apply this label (Docker-speak for "tag") to all containers and volumes we
# create for easier management and test cleanup
LABEL = "job-runner"


# Shelling out to the Docker client on Windows sometimes hangs indefinitely
# (for no obvious reason that we can determine). To prevent this locking up the
# entire process we use a large-ish default timeout on almost all calls to
# Docker. (Timeout value is in seconds.)
DEFAULT_TIMEOUT = 5 * 60


class DockerPullError(Exception):
    pass


class DockerAuthError(DockerPullError):
    pass


class DockerTimeoutError(Exception):
    pass


class DockerDiskSpaceError(Exception):
    pass


def add_docker_labels(cmd, labels):
    """Add labels to a docker cmd."""
    if not labels:
        return
    for name, value in labels.items():
        cmd.extend(["--label", f"{name}={value}"])


def docker(docker_args, timeout=DEFAULT_TIMEOUT, **kwargs):
    args = ["docker"] + docker_args
    try:
        return subprocess_run(args, timeout=timeout, **kwargs)
    except subprocess.TimeoutExpired as e:
        raise DockerTimeoutError from e
    except subprocess.CalledProcessError as e:
        output = e.stderr
        if output is None:
            output = e.stdout
        if isinstance(output, bytes):
            output = output.decode("utf8", "ignore")
        if (
            output is not None
            and e.returncode == 1
            and "Error response from daemon:" in output
            and ": no space left on device" in output
        ):
            raise DockerDiskSpaceError from e
        else:
            raise


def create_volume(volume_name, labels=None):
    """
    Creates the named volume and also creates (but does not start) a "manager"
    container which we can use to copy files in and out of the volume. Note
    that in order to interact with the volume a container with that volume
    mounted must exist, but it doesn't need to be running.
    """
    cmd = ["volume", "create", "--label", LABEL, "--name", volume_name]
    add_docker_labels(cmd, labels)
    docker(cmd, check=True, capture_output=True)
    # Run a basic container that mounts this image.  Having the volume mounted
    # allows us to copy from/to it, and having the container running protects
    # it from rogue `docker container prune` commands.
    try:
        run(
            manager_name(volume_name),
            [MANAGEMENT_CONTAINER_IMAGE, "sh"],
            volume=(volume_name, VOLUME_MOUNT_POINT),
            label=LABEL,
            labels=labels,
            extra_args=[
                "--interactive",
                "--restart=unless-stopped",
            ],
        )
    except subprocess.CalledProcessError as e:
        # If a volume and its manager already exist we don't want to throw an
        # error. `docker volume create` is naturally idempotent, but we have to
        # handle this manually here.
        if e.returncode != 125 or b"is already in use by container" not in e.stderr:
            raise


def volume_exists(volume_name):
    """Does the given volume exist?"""
    try:
        docker(["volume", "inspect", volume_name], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        return False
    else:
        return True


def delete_volume(volume_name):
    """
    Deletes the named volume and its manager container
    """
    try:
        docker(
            ["rm", "--force", manager_name(volume_name)],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Ignore error if container has already been removed
        if e.returncode != 1 or b"No such container" not in e.stderr:
            raise
    try:
        docker(
            [
                "volume",
                "rm",
                volume_name,
            ],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Ignore error if container has already been removed
        if e.returncode != 1 or b"No such volume" not in e.stderr:
            raise


def copy_to_volume(volume_name, source, dest, timeout=None):
    """
    Copy the contents of `directory` to the root of the named volume

    As this command can potentially take a long time with large files it does
    not, by default, have any timeout.
    """
    if source.is_dir():
        # Ensure the *contents* of the directory are copied, rather than the
        # directory itself. See:
        # https://docs.docker.com/engine/reference/commandline/cp/#extended-description
        source = str(source).rstrip("/") + "/."
    docker(
        [
            "cp",
            "--follow-link",
            source,
            f"{manager_name(volume_name)}:{VOLUME_MOUNT_POINT}/{dest}",
        ],
        check=True,
        capture_output=True,
        timeout=timeout,
    )


def copy_from_volume(volume_name, source, dest, timeout=None):
    """
    Copy the contents of `source` from the root of the named volume to `dest`
    on local disk

    As this command can potentially take a long time with large files it does
    not, by default, have any timeout.
    """
    with atomic_writer(dest) as tmp:
        docker(
            [
                "cp",
                "--follow-link",
                f"{manager_name(volume_name)}:{VOLUME_MOUNT_POINT}/{source}",
                tmp,
            ],
            check=True,
            capture_output=True,
            timeout=timeout,
        )


def glob_volume_files(volume_name, glob_patterns):
    """
    Accept a list of glob patterns and return a dict mapping each pattern to a
    list of all the files in `volume_name` which match

    Accepting multiple patterns like this allow us to avoid multiple round
    trips through Docker when we need to match several different patterns.
    """
    # Guard against the easy mistake of passing a single string pattern, rather
    # than a list of patterns
    assert not isinstance(glob_patterns, str)
    # Build a `find` command
    args = ["find", VOLUME_MOUNT_POINT, "-type", "f", "("]
    # We need to use regex matching rather than `-path` because find's
    # wildcards are too liberal and match across path separators (e.g
    # "foo/*.py" matches Python files in all sub-directories of "foo" rather
    # than just the top level)
    for pattern in glob_patterns:
        args.extend(
            ["-regex", _glob_pattern_to_regex(f"{VOLUME_MOUNT_POINT}/{pattern}"), "-o"]
        )
    # Replace final OR flag with a closing bracket
    args[-1] = ")"
    response = docker(
        ["container", "exec", manager_name(volume_name)] + args,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    # Remove the volume path prefix from the results
    chars_to_strip = len(VOLUME_MOUNT_POINT) + 1
    files = [f[chars_to_strip:] for f in response.stdout.splitlines()]
    files = sorted(files)
    matches = {}
    for pattern in glob_patterns:
        regex = re.compile(_glob_pattern_to_regex(pattern))
        matches[pattern] = [f for f in files if regex.match(f)]
    return matches


def _glob_pattern_to_regex(glob_pattern):
    """
    Convert a shell glob pattern (where the wildcard does not match the "/"
    character) into a regular expression
    """
    literals = glob_pattern.split("*")
    return "[^/]*".join(map(re.escape, literals))


def find_newer_files(volume_name, reference_file):
    """
    Return all files in volume newer than the reference file
    """
    args = [
        "find",
        VOLUME_MOUNT_POINT,
        "-type",
        "f",
        "-newer",
        f"{VOLUME_MOUNT_POINT}/{reference_file}",
    ]
    response = docker(
        ["container", "exec", manager_name(volume_name)] + args,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    # Remove the volume path prefix from the results
    chars_to_strip = len(VOLUME_MOUNT_POINT) + 1
    files = [f[chars_to_strip:] for f in response.stdout.splitlines()]
    return sorted(files)


def manager_name(volume_name):
    return f"{volume_name}-manager"


def container_exists(name):
    return bool(container_inspect(name, "ID", none_if_not_exists=True))


def container_is_running(name):
    return container_inspect(name, "State.Running", none_if_not_exists=True) or False


def container_inspect(name, key="", none_if_not_exists=False):
    """
    Retrieves metadata about the named container. By default will return
    everything but `key` can be a dotted path to a specific piece of metadata.

    Optionally returns None if the container does not exist

    See: https://docs.docker.com/engine/reference/commandline/inspect/
    """
    try:
        response = docker(
            ["container", "inspect", "--format", "{{json .%s}}" % key, name],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        if (
            none_if_not_exists
            and e.returncode == 1
            and b"No such container" in e.stderr
        ):
            return
        else:
            raise
    return json.loads(response.stdout)


def run(
    name,
    args,
    volume=None,
    env=None,
    allow_network_access=False,
    label=None,
    labels=None,
    extra_args=None,
):
    run_args = ["run", "--init", "--detach", "--label", LABEL, "--name", name]
    if extra_args is not None:
        run_args.extend(extra_args)

    if not allow_network_access:
        run_args.extend(["--network", "none"])
    if volume:
        run_args.extend(["--volume", f"{volume[0]}:{volume[1]}"])
    # These lables are in addition to the default LABEL which is always applied
    # Single unary label
    if label is not None:
        run_args.extend(["--label", label])
    if labels:
        add_docker_labels(run_args, labels)
    # To avoid leaking the values into the command line arguments we set them
    # in the evnironment and tell Docker to fetch them from there
    if env is None:
        env = {}
    for key, value in env.items():
        run_args.extend(["--env", key])
    ps = docker(
        run_args + args, check=True, capture_output=True, env=dict(os.environ, **env)
    )
    return ps


def image_exists_locally(image_name_and_version):
    try:
        docker(
            ["image", "inspect", "--format", "ok", image_name_and_version],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        if e.returncode == 1 and b"No such image" in e.stderr:
            return False
        raise


def delete_container(name):
    try:
        docker(
            ["container", "rm", "--force", name],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Ignore error if container has already been removed
        if e.returncode != 1 or b"No such container" not in e.stderr:
            raise


def kill(name):
    try:
        docker(
            ["container", "kill", name],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Ignore error if container has already been killed or removed
        if e.returncode != 1 or (
            b"No such container" not in e.stderr and b"is not running" not in e.stderr
        ):
            raise


def write_logs_to_file(container_name, filename):
    with open(filename, "wb") as f:
        # the --tail 100000 acts as an upper bound for large log outputs. It
        # ensures that streaming the logs via docker does not take too long and
        # cause the command to timeout. Some logs is better than none.
        docker(
            ["container", "logs", "--timestamps", "--tail", "100000", container_name],
            check=True,
            stdout=f,
            stderr=subprocess.STDOUT,
        )


def pull(image, quiet=False):
    try:
        docker(
            ["pull", image, *(["--quiet"] if quiet else [])],
            check=True,
            encoding="utf-8",
            # When not running "quiet" we don't capture stdout so that progress
            # gets shown in the terminal
            stdout=subprocess.PIPE if quiet else None,
            stderr=subprocess.PIPE,
            timeout=None,
        )
    except subprocess.CalledProcessError as e:
        message = e.stderr.strip()
        if message.endswith(": unauthorized"):
            raise DockerAuthError(message)
        else:
            raise DockerPullError(message)
