import re
import subprocess


# Docker requires a container in order to interact with volumes, but it doesn't
# much matter what it is for our purposes as long as it has `sh` and `find`.
# We're using the job-runner image here because we know that will already exist
# in the production environment but it's a bit heavyweight for this. Something
# like "busybox" would be ideal really.
MANAGEMENT_CONTAINER_IMAGE = "docker.opensafely.org/job-runner"

# This path is pretty arbitrary: it sets where we mount volumes inside their
# management containers (which are used for copying files in and out), but this
# is independent of where the volumes get mounted inside other containers to
# which they may get attached.
VOLUME_MOUNT_POINT = "/workspace"


def create_volume(volume_name):
    """
    Creates the named volume and also creates (but does not start) a "manager"
    container which we can use to copy files in and out of the volume
    """
    subprocess.run(
        ["docker", "volume", "create", "--name", volume_name],
        check=True,
        capture_output=True,
    )
    try:
        subprocess.run(
            [
                "docker",
                "container",
                "create",
                "--name",
                manager_name(volume_name),
                "--volume",
                f"{volume_name}:{VOLUME_MOUNT_POINT}",
                "--entrypoint",
                "sh",
                "--interactive",
                MANAGEMENT_CONTAINER_IMAGE,
            ],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # If a volume and its manager already exist we don't want to throw an
        # error. `docker volume create` is naturally idempotent, but we have to
        # handle this manually here.
        if e.returncode != 1 or b"is already in use by container" not in e.stderr:
            raise


def delete_volume(volume_name):
    """
    Deletes the named volume and its manager container
    """
    try:
        subprocess.run(
            ["docker", "container", "rm", "--force", manager_name(volume_name)],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Ignore error if container has already been removed
        if e.returncode != 1 or b"No such container" not in e.stderr:
            raise
    try:
        subprocess.run(
            ["docker", "volume", "rm", volume_name,], check=True, capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Ignore error if container has already been removed
        if e.returncode != 1 or b"No such volume" not in e.stderr:
            raise


def copy_to_volume(volume_name, directory):
    """
    Copy the contents of `directory` to the root of the named volume
    """
    subprocess.run(
        [
            "docker",
            "cp",
            # Ensure the *contents* of the directory are copied, rather than
            # the directory itself. See:
            # https://docs.docker.com/engine/reference/commandline/cp/#extended-description
            f"{str(directory).rstrip('/')}/.",
            f"{manager_name(volume_name)}:{VOLUME_MOUNT_POINT}",
        ],
        check=True,
        capture_output=True,
    )


def glob_volume_files(volume_name, glob_patterns):
    """
    Return a list of all files in `volume_name` whose paths match any of
    `glob_patterns`
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
    # We can't use `exec` unless the container is running, even though it won't
    # actually do anything other than sit waiting for input. This will get
    # stopped when we `--force rm` the container while removing the volume.
    subprocess.run(
        ["docker", "container", "start", manager_name(volume_name)],
        check=True,
        capture_output=True,
    )
    response = subprocess.run(
        ["docker", "container", "exec", manager_name(volume_name)] + args,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    matches = response.stdout.splitlines()
    # Remove the volume path prefix from the results
    chars_to_strip = len(VOLUME_MOUNT_POINT) + 1
    return [match[chars_to_strip:] for match in matches]


def _glob_pattern_to_regex(glob_pattern):
    """
    Convert a shell glob pattern (where the wildcard does not match the "/"
    character) into a regular expression
    """
    literals = glob_pattern.split("*")
    return "[^/]*".join(map(re.escape, literals))


def manager_name(volume_name):
    return f"{volume_name}-manager"
