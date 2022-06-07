import importlib
import shutil
from collections import defaultdict
from pathlib import Path

from jobrunner import config
from jobrunner.lib import atomic_writer, docker


def copy_file(source, dest, follow_symlinks=True):
    """Efficient atomic copy.

    shutil.copy uses sendfile on linux, so should be fast.
    """
    # ensure path
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with atomic_writer(dest) as tmp:
        shutil.copy(source, tmp, follow_symlinks=follow_symlinks)


def docker_volume_name(job):
    return f"os-volume-{job.id}"


class DockerVolumeAPI:
    def volume_name(job):
        return docker_volume_name(job)

    def create_volume(job, labels=None):
        docker.create_volume(docker_volume_name(job))

    def volume_exists(job):
        return docker.volume_exists(docker_volume_name(job))

    def copy_to_volume(job, src, dst, timeout=None):
        docker.copy_to_volume(docker_volume_name(job), src, dst, timeout)

    def copy_from_volume(job, src, dst, timeout=None):
        docker.copy_from_volume(docker_volume_name(job), src, dst, timeout)

    def delete_volume(job):
        docker.delete_volume(docker_volume_name(job))

    def touch_file(job, path, timeout=None):
        docker.touch_file(docker_volume_name(job), path, timeout)

    def glob_volume_files(job):
        return docker.glob_volume_files(docker_volume_name(job), job.output_spec.keys())

    def find_newer_files(job, path):
        return docker.find_newer_files(docker_volume_name(job), path)


def host_volume_path(job):
    path = config.HIGH_PRIVACY_VOLUME_DIR / job.id
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class BindMountVolumeAPI:
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
        host_volume_path(job).mkdir()

    def volume_exists(job):
        return host_volume_path(job).exists()

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
        copy_file(path, dst)

    def delete_volume(job):
        shutil.rmtree(host_volume_path(job), ignore_errors=True)

    def touch_file(job, path, timeout=None):
        (host_volume_path(job) / path).touch()

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


def get_volume_api():
    module_name, cls = config.LOCAL_VOLUME_API.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, cls)
