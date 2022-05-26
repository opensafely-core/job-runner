from jobrunner.lib import docker


def volume_name(job):
    return f"os-volume-{job.id}"


class DockerVolumeAPI:
    def volume_name(job):
        return volume_name(job)

    def create_volume(job, labels=None):
        docker.create_volume(volume_name(job))

    def volume_exists(job):
        return docker.volume_exists(volume_name(job))

    def copy_to_volume(job, src, dst, timeout=None):
        docker.copy_to_volume(volume_name(job), src, dst, timeout)

    def copy_from_volume(job, src, dst, timeout=None):
        docker.copy_from_volume(volume_name(job), src, dst, timeout)

    def delete_volume(job):
        docker.delete_volume(volume_name(job))

    def touch_file(job, path, timeout=None):
        docker.touch_file(volume_name(job), path, timeout)

    def glob_volume_files(job):
        return docker.glob_volume_files(volume_name(job), job.output_spec.keys())

    def find_newer_files(job, path):
        return docker.find_newer_files(volume_name(job), path)


volume_api = DockerVolumeAPI
