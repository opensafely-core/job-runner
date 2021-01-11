"""
Run project.yaml actions locally

This creates and runs jobs in a way that's fairly close to what happens in
production, but with the key difference that rather than specifying a repo URL
and a commit we just supply a workspace directory and code is copied into a
Docker volume directly from there. In the past we've had an issue whereby
broken actions work locally by accident because the right output files happen
to exist anyway even though the action doesn't specify that it depends on them.
To try to avoid this, when copying code into a volume we ignore any files which
match any of the output patterns in the project. We then copy in just the
explicit dependencies of the action.

This is achieved by setting a LOCAL_RUN_MODE flag in the config which, in two
key places, tells the code not to talk to git but do something else instead.

Other than that, everything else runs entirely as it would in production. A
temporary database and log directory is created for each run and then thrown
away afterwards.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import random
import shlex
import shutil
import string
import subprocess
import sys
import tempfile
import textwrap

from .run import main as run_main, JobError
from . import config
from . import docker
from .database import find_where
from .manage_jobs import METADATA_DIR
from .models import JobRequest, Job, State, StatusCode
from .create_or_update_jobs import (
    create_jobs,
    ProjectValidationError,
    JobRequestError,
    NothingToDoError,
    RUN_ALL_COMMAND,
)
from .log_utils import configure_logging
from .subprocess_utils import subprocess_run
from .string_utils import tabulate


# First paragraph of docstring
DESCRIPTION = __doc__.partition("\n\n")[0]
# local run logging format
LOCAL_RUN_FORMAT = "{action}{message}"


def add_arguments(parser):
    parser.add_argument("actions", nargs="*", help="Name of project action to run")
    parser.add_argument(
        "-f",
        "--force-run-dependencies",
        help="Re-run from scratch without using existing outputs",
        action="store_true",
    )
    parser.add_argument(
        "--project-dir",
        help="Project directory (default: current directory)",
        default=".",
    )
    # This only really intended for use in CI. When running locally users
    # generally want to know about failures as soon as they happen whereas in
    # CI you want to run as many actions as possible.
    parser.add_argument(
        "--continue-on-error",
        help="Don't stop on first failed action",
        action="store_true",
    )
    return parser


def main(project_dir, actions, force_run_dependencies=False, continue_on_error=False):
    if not docker_preflight_check():
        return False

    project_dir = Path(project_dir).resolve()
    temp_log_dir = project_dir / METADATA_DIR / ".logs"
    # Generate unique docker label to use for all volumes and containers we
    # create during this run in order to make cleanup easy
    docker_label = "job-runner-local-{}".format(
        "".join(random.choices(string.ascii_uppercase, k=8))
    )

    try:
        success_flag = create_and_run_jobs(
            project_dir,
            actions,
            force_run_dependencies=force_run_dependencies,
            continue_on_error=continue_on_error,
            temp_log_dir=temp_log_dir,
            docker_label=docker_label,
        )
    except KeyboardInterrupt:
        print("\nKilled by user")
        print("Cleaning up Docker containers and volumes ...")
        success_flag = False
    finally:
        delete_docker_entities("container", docker_label, ignore_errors=True)
        delete_docker_entities("volume", docker_label, ignore_errors=True)
        shutil.rmtree(temp_log_dir, ignore_errors=True)
    return success_flag


def create_and_run_jobs(
    project_dir,
    actions,
    force_run_dependencies,
    continue_on_error,
    temp_log_dir,
    docker_label,
):
    # Configure
    docker.LABEL = docker_label
    config.LOCAL_RUN_MODE = True
    config.HIGH_PRIVACY_WORKSPACES_DIR = project_dir.parent
    # Append a random value so that multiple runs in the same process will each
    # get their own unique in-memory database. This is only really relevant
    # during testing.
    config.DATABASE_FILE = f":memory:{random.randrange(sys.maxsize)}"
    config.JOB_LOG_DIR = temp_log_dir
    config.BACKEND = "expectations"
    config.USING_DUMMY_DATA_BACKEND = True

    # None of the below should be used when running locally
    config.WORK_DIR = None
    config.TMP_DIR = None
    config.GIT_REPO_DIR = None
    config.HIGH_PRIVACY_STORAGE_BASE = None
    config.MEDIUM_PRIVACY_STORAGE_BASE = None
    config.MEDIUM_PRIVACY_WORKSPACES_DIR = None

    # Create job_request and jobs
    job_request = JobRequest(
        id="local",
        repo_url=str(project_dir),
        commit="none",
        requested_actions=actions,
        workspace=project_dir.name,
        database_name="dummy",
        force_run_dependencies=force_run_dependencies,
        # The default behaviour of refusing to run if a dependency has failed
        # makes for an awkward workflow when iterating in development
        force_run_failed=True,
        branch="",
        original={"created_by": os.environ.get("USERNAME")},
    )
    try:
        create_jobs(job_request)
    except NothingToDoError:
        print("=> All actions already completed successfully")
        print("   Use -f option to force everything to re-run")
        return True
    except (ProjectValidationError, JobRequestError) as e:
        print(f"=> {type(e).__name__}")
        print(textwrap.indent(str(e), "   "))
        if hasattr(e, "valid_actions"):
            print("\n   Valid action names are:")
            for action in e.valid_actions:
                if action != RUN_ALL_COMMAND:
                    print(f"     {action}")
                else:
                    print(f"     {action} (runs all actions in project)")
        return False

    jobs = find_where(Job)

    for image in get_docker_images(jobs):
        is_stata = "stata-mp" in image

        if is_stata and config.STATA_LICENSE is None:
            config.STATA_LICENSE = get_stata_license()
            if config.STATA_LICENSE is None:
                # TODO switch this to failing when the stata image requires it
                print("WARNING: no STATA_LICENSE found")

        if not docker.image_exists_locally(image):
            print(f"Fetching missing docker image: docker pull {image}")
            try:
                # We want to be chatty when running in the console so users can
                # see progress and quiet in CI so we don't spam the logs with
                # layer download noise
                docker.pull(image, quiet=not sys.stdout.isatty())
            except docker.DockerPullError as e:
                success = False
                if is_stata:
                    # best effort retry hack
                    success = temporary_stata_workaround(image)
                if not success:
                    print("Failed with error:")
                    print(e)
                    return False

    action_names = [job.action for job in jobs]
    print(f"\nRunning actions: {', '.join(action_names)}\n")

    configure_logging(
        fmt=LOCAL_RUN_FORMAT,
        # None of these status messages are particularly useful in local run
        # mode, and they can generate a lot of clutter in large dependency
        # trees
        status_codes_to_ignore=[
            StatusCode.WAITING_ON_DEPENDENCIES,
            StatusCode.DEPENDENCY_FAILED,
            StatusCode.WAITING_ON_WORKERS,
        ],
        # All the other output we produce goes to stdout and it's a bit
        # confusing if the log messages end up on a separate stream
        stream=sys.stdout,
    )

    # Run everything
    try:
        run_main(exit_when_done=True, raise_on_failure=not continue_on_error)
    except (JobError, KeyboardInterrupt):
        pass

    final_jobs = find_where(Job, state__in=[State.FAILED, State.SUCCEEDED])
    # Always show failed jobs last, otherwise show in order run
    final_jobs.sort(
        key=lambda job: (
            1 if job.state == State.FAILED else 0,
            job.started_at or 0,
        )
    )

    # Pretty print details of each action
    print()
    if not final_jobs:
        print("=> No jobs completed")
    for job in final_jobs:
        log_file = f"{METADATA_DIR}/{job.action}.log"
        # If a job fails we don't want to clutter the output with its failed
        # dependants.
        if (
            job.state == State.FAILED
            and job.status_code == StatusCode.DEPENDENCY_FAILED
        ):
            continue
        print(f"=> {job.action}")
        print(textwrap.indent(job.status_message, "   "))
        # Where a job failed because expected outputs weren't found we show a
        # list of other outputs which were generated
        if job.unmatched_outputs:
            print(
                "\n   Did you mean to match one of these files instead?\n    - ", end=""
            )
            print("\n    - ".join(job.unmatched_outputs))
        print()
        print(f"   log file: {log_file}")
        # Display matched outputs
        print("   outputs:")
        outputs = sorted(job.outputs.items()) if job.outputs else []
        print(tabulate(outputs, separator="  - ", indent=5, empty="(no outputs)"))
        # If a job exited with an error code then try to display the end of the
        # log output in case that makes the problem immediately obvious
        if job.status_code == StatusCode.NONZERO_EXIT:
            logs, truncated = get_log_file_snippet(project_dir / log_file, max_lines=32)
            if logs:
                print(f"\n   logs{' (truncated)' if truncated else ''}:\n")
                print(textwrap.indent(logs, "     "))
        print()

    success_flag = all(job.state == State.SUCCEEDED for job in final_jobs)
    return success_flag


# Copied from test/conftest.py to avoid a more complex dependency tree
def delete_docker_entities(entity, label, ignore_errors=False):
    ls_args = [
        "docker",
        entity,
        "ls",
        "--all" if entity == "container" else None,
        "--filter",
        f"label={label}",
        "--quiet",
    ]
    ls_args = list(filter(None, ls_args))
    response = subprocess_run(
        ls_args, capture_output=True, encoding="ascii", check=not ignore_errors
    )
    ids = response.stdout.split()
    if ids and response.returncode == 0:
        rm_args = ["docker", entity, "rm", "--force"] + ids
        subprocess_run(rm_args, capture_output=True, check=not ignore_errors)


def get_docker_images(jobs):
    docker_images = {shlex.split(job.run_command)[0] for job in jobs}
    full_docker_images = {
        f"{config.DOCKER_REGISTRY}/{image}" for image in docker_images
    }
    # We always need this image to work with volumes
    full_docker_images.add(docker.MANAGEMENT_CONTAINER_IMAGE)
    return full_docker_images


def get_log_file_snippet(log_file, max_lines):
    try:
        contents = Path(log_file).read_text()
    except Exception:
        contents = ""
    # As docker logs are timestamp-prefixed the first blank line marks the end
    # of the docker logs and the start of our "trailer"
    docker_logs = contents.partition("\n\n")[0]
    # Strip off timestamp
    log_lines = [line[31:] for line in docker_logs.splitlines()]
    if len(log_lines) > max_lines:
        log_lines = log_lines[-max_lines:]
        truncated = True
    else:
        truncated = False
    return "\n".join(log_lines).strip(), truncated


def temporary_stata_workaround(image):
    """
    This is a temporary workaround for the fact that the current crop of Github
    Actions have credentials for the old docker.opensafely.org registry but not
    for ghcr.io. These are only needed for the one private image we have
    (Stata) so we detect when we are in this situation and pull from the old
    registry instead.

    We'll shortly be updating the Github Actions to use an entirely new set of
    commands in any case, so this will hopefully be a short-lived hack.
    """
    if not os.environ.get("GITHUB_WORKFLOW"):
        return False

    docker_config = os.environ.get("DOCKER_CONFIG", os.path.expanduser("~/.docker"))
    config_path = Path(docker_config) / "config.json"
    try:
        config = json.loads(config_path.read_text())
        auths = config["auths"]
    except Exception:
        return False

    if "ghcr.io" in auths or "docker.opensafely.org" not in auths:
        return False

    print("Applying Docker authentication workaround...")
    alt_image = image.replace("ghcr.io/opensafely/", "docker.opensafely.org/")

    try:
        docker.pull(alt_image, quiet=True)
        print(f"Retagging '{alt_image}' as '{image}'")
        subprocess_run(["docker", "tag", alt_image, image])
    except Exception:
        return False

    return True


def get_stata_license(repo=config.STATA_LICENSE_REPO):
    """Load a stata license from local cache or remote repo."""
    cached = Path(f"{tempfile.gettempdir()}/opensafely-stata.lic")

    def git_clone(repo_url, cwd):
        cmd = ["git", "clone", "--depth=1", repo_url, "repo"]
        # GIT_TERMINAL_PROMPT=0 means it will fail if it requires auth. This
        # alows us to retry with an ssh url on linux/mac, as they would
        # generally prompt given an https url.
        result = subprocess_run(
            cmd,
            cwd=cwd,
            capture_output=True,
            env=dict(os.environ, GIT_TERMINAL_PROMPT="0"),
        )
        return result.returncode == 0

    if not cached.exists():
        try:
            tmp = tempfile.TemporaryDirectory(suffix="opensafely")
            success = git_clone(repo, tmp.name)
            # http urls usually won't work for linux/mac clients, so try ssh
            if not success and repo.startswith("https://"):
                git_clone(
                    repo.replace("https://", "git+ssh://git@"),
                    tmp.name,
                )
            shutil.copyfile(f"{tmp.name}/repo/stata.lic", cached)
        except Exception:
            return None
        finally:
            # py3.7 on windows can't clean up TemporaryDirectory with git's read only
            # files in them, so just don't bother.
            if platform.system() != "Windows" or sys.version_info[:2] > (3, 7):
                tmp.cleanup()

    return cached.read_text()


def docker_preflight_check():
    try:
        subprocess_run(["docker", "info"], check=True, capture_output=True)
    except FileNotFoundError:
        print("Could not find command: docker")
        print("\nTo use the `run` command you must have Docker installed, see:")
        print("https://docs.docker.com/get-docker/")
        return False
    except subprocess.CalledProcessError:
        print("There was an error running: docker info")
        print("\nIt looks like you have Docker installed but have not started it.")
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser = add_arguments(parser)
    args = parser.parse_args()
    success = main(**vars(args))
    sys.exit(0 if success else 1)
