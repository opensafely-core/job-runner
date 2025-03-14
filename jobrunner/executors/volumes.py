import importlib
import logging
import os
import shutil
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

from jobrunner import config
from jobrunner.lib import atomic_writer, docker


logger = logging.getLogger(__name__)


def copy_file(source, dest, follow_symlinks=True):
    """Efficient atomic copy.

    shutil.copy uses sendfile on linux, so should be fast.
    """
    # ensure path
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with atomic_writer(dest) as tmp:
        shutil.copy(source, tmp, follow_symlinks=follow_symlinks)

    return dest.stat().st_size


def docker_volume_name(job):
    return f"os-volume-{job.id}"


class DockerVolumeAPI:
    # don't run with UIDs for now. We maybe be able to support this in future.
    requires_root = True
    supported_platforms = ("linux", "win32", "darwin")
    volume_type = "volume"  # https://docs.docker.com/engine/storage/volumes/

    def volume_name(job):
        return docker_volume_name(job)

    def create_volume(job, labels=None):
        docker.create_volume(docker_volume_name(job))

    def volume_exists(job):
        return docker.volume_exists(docker_volume_name(job))

    def copy_to_volume(job, src, dst, timeout=None):
        docker.copy_to_volume(docker_volume_name(job), src, dst, timeout)

    def copy_from_volume(job, src, dst, timeout=None):
        return docker.copy_from_volume(docker_volume_name(job), src, dst, timeout)

    def delete_volume(job):
        docker.delete_volume(docker_volume_name(job))

    def write_timestamp(job, path, timeout=None):
        try:
            f = tempfile.NamedTemporaryFile(delete=False)
            f.close()
            p = Path(f.name)
            p.write_text(str(time.time_ns()))
            docker.copy_to_volume(docker_volume_name(job), p, path, timeout)
        finally:
            try:
                os.remove(f.name)
            except Exception:
                pass

    def read_timestamp(job, path, timeout=None):
        return docker.read_timestamp(docker_volume_name(job), path, timeout)

    def glob_volume_files(job):
        return docker.glob_volume_files(docker_volume_name(job), job.output_spec.keys())

    def find_newer_files(job, path):
        return docker.find_newer_files(docker_volume_name(job), path)


def host_volume_path(job, create=True):
    path = config.HIGH_PRIVACY_VOLUME_DIR / job.id
    if create:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise Exception(f"Could not create {path.parent} due to permissions error")
    return path


class BindMountVolumeAPI:
    # Only works running jobs with uid:gid
    requires_root = False
    supported_platforms = ("linux",)
    volume_type = "bind"  # https://docs.docker.com/engine/storage/bind-mounts/

    def volume_name(job):
        """Return the absolute path to the volume directory.

        In case we're running inside a docker container, make sure the path is
        relative to the *hosts* POV, not the container.
        """
        local_path = host_volume_path(job)
        if config.DOCKER_HOST_VOLUME_DIR is None:
            return local_path

        return config.DOCKER_HOST_VOLUME_DIR / local_path.relative_to(
            config.HIGH_PRIVACY_VOLUME_DIR
        )

    def create_volume(job, labels=None):
        """Create a volume dir.

        This can be called when the dir exists from a previous call to
        create_volume (e.g. retry_job or db maintainence mode), but the files
        didn't get properly written so its ok if it already exists - we'll
        re-copy all the files in that case.
        """
        host_volume_path(job).mkdir(exist_ok=True)

    def volume_exists(job):
        # create=False means this won't raise if we're not configured
        # to use BindMountVolumeAPI
        return host_volume_path(job, create=False).exists()

    def copy_to_volume(job, src, dst, timeout=None):
        # We don't respect the timeout.
        volume = host_volume_path(job)
        if src.is_dir():
            shutil.copytree(
                src,
                volume / dst,
                symlinks=True,
                copy_function=copy_file,
                dirs_exist_ok=True,
            )
        else:
            copy_file(src, volume / dst)

    def copy_from_volume(job, src, dst, timeout=None):
        # this is only used to copy final outputs/logs.
        path = host_volume_path(job) / src
        return copy_file(path, dst)

    def delete_volume(job):
        failed_files = {}

        # if we logged each file error directly, it would spam the logs, so we collect them
        def onerror(function, path, excinfo):
            failed_files[Path(path)] = str(excinfo[1])

        path = host_volume_path(job)
        try:
            shutil.rmtree(str(path), onerror=onerror)

            if failed_files:
                relative_paths = [str(p.relative_to(path)) for p in failed_files]
                logger.error(
                    f"could not remove {len(failed_files)} files from {path}: {','.join(relative_paths)}"
                )
        except Exception:
            logger.exception(f"Failed to cleanup job volume {path}")

    def write_timestamp(job, path, timeout=None):
        (host_volume_path(job) / path).write_text(str(time.time_ns()))

    def read_timestamp(job, path, timeout=None):
        abs_path = host_volume_path(job) / path
        if not abs_path.exists():
            return None
        try:
            contents = abs_path.read_text()
            if contents:
                return int(contents)
            else:
                # linux host filesystem provides untruncated timestamps
                stat = abs_path.stat()
                return int(stat.st_ctime * 1e9)

        except Exception:
            logger.exception("Failed to read timestamp from volume file {abs_path}")
            return None

    def glob_volume_files(job):
        volume = host_volume_path(job)

        found = defaultdict(list)

        for pattern in job.output_spec.keys():
            for match in volume.glob(pattern):
                if match.is_file():
                    found[pattern].append(str(match.relative_to(volume)))

        return found

    def find_newer_files(job, reference):
        volume = host_volume_path(job)
        ref_time = (volume / reference).stat().st_mtime
        found = []
        for f in volume.glob("**/*"):
            if f.is_file() and f.stat().st_mtime > ref_time:
                found.append(str(f.relative_to(volume)))

        return found


def default_volume_api():
    module_name, cls = config.LOCAL_VOLUME_API.split(":", 1)
    module = importlib.import_module(module_name)
    api = getattr(module, cls)
    if sys.platform not in api.supported_platforms:
        raise Exception(
            f"LOCAL_VOLUME_API={config.LOCAL_VOLUME_API} is not supported on this machine ({sys.platform})"
        )

    return api


DEFAULT_VOLUME_API = default_volume_api()


def get_volume_api(job):
    for api in [BindMountVolumeAPI, DockerVolumeAPI]:
        if api.volume_exists(job):
            return api

    return DEFAULT_VOLUME_API
