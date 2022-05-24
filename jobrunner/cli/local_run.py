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

The job creation logic is also slighty different here (compare the
`create_job_request_and_jobs` function below with the `create_jobs` function in
`jobrunner.create_or_update_jobs`) as things like validating the repo URL don't
apply locally.

Other than that, everything else runs entirely as it would in production. A
temporary database and log directory is created for each run and then thrown
away afterwards.
"""
import argparse
import getpass
import os
import random
import shlex
import shutil
import string
import subprocess
import sys
import tempfile
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

from jobrunner import config, executors
from jobrunner.create_or_update_jobs import (
    RUN_ALL_COMMAND,
    JobRequestError,
    NothingToDoError,
    ProjectValidationError,
    assert_new_jobs_created,
    get_new_jobs_to_run,
    insert_into_database,
    parse_and_validate_project_file,
)
from jobrunner.executors.local import METADATA_DIR
from jobrunner.lib import database, docker
from jobrunner.lib.database import find_where
from jobrunner.lib.log_utils import configure_logging
from jobrunner.lib.string_utils import tabulate
from jobrunner.lib.subprocess_utils import subprocess_run
from jobrunner.models import Job, JobRequest, State, StatusCode, random_id
from jobrunner.project import UnknownActionError, get_all_actions
from jobrunner.queries import calculate_workspace_state
from jobrunner.reusable_actions import (
    ReusableActionError,
    resolve_reusable_action_references,
)
from jobrunner.run import main as run_main


# First paragraph of docstring
DESCRIPTION = __doc__.partition("\n\n")[0]

# local run logging format
LOCAL_RUN_FORMAT = "{action}{message}"


# Super-crude support for colourised/formatted output inside Github Actions. It
# would be good to support formatted output in the CLI more generally, but we
# should use a decent library for that to handle the various cross-platform
# issues.
class ANSI:
    Reset = "\u001b[0m"
    Bold = "\u001b[1m"
    Grey = "\u001b[38;5;248m"


def add_arguments(parser):
    parser.add_argument("actions", nargs="*", help="Name of project action to run")
    parser.add_argument(
        "-f",
        "--force-run-dependencies",
        help="Force the dependencies of the action to run, whether or not their outputs exist",
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
    # This particularly useful in CI.
    parser.add_argument(
        "--timestamps",
        help="Include timestamps in output",
        action="store_true",
    )
    parser.add_argument(
        "--debug",
        help="Leave docker containers and volumes in place for debugging",
        action="store_true",
    )
    parser.add_argument(
        "--format-output-for-github",
        help=(
            "Produce output in a format suitable for display inside a "
            "Github Actions Workflow"
        ),
        action="store_true",
    )
    return parser


def main(
    project_dir,
    actions,
    force_run_dependencies=False,
    continue_on_error=False,
    debug=False,
    timestamps=False,
    format_output_for_github=False,
):
    if not docker_preflight_check():
        return False

    project_dir = Path(project_dir).resolve()
    temp_dir = Path(tempfile.mkdtemp(prefix="opensafely_"))
    if not debug:
        # Generate unique docker label to use for all volumes and containers we
        # create during this run in order to make cleanup easy. We're using a
        # random string to prevent mutliple parallel runs from interferring by
        # trying to clean up each others jobs.
        docker_label = "job-runner-local-{}".format(
            "".join(random.choices(string.ascii_uppercase, k=8))
        )
    else:
        # In debug mode (where we don't automatically delete containers and
        # volumes) we use a consistent label to make manual clean up easier
        docker_label = "job-runner-debug"

    log_format = LOCAL_RUN_FORMAT
    if timestamps:
        log_format = "{asctime} " + LOCAL_RUN_FORMAT

    try:
        success_flag = create_and_run_jobs(
            project_dir,
            actions,
            force_run_dependencies=force_run_dependencies,
            continue_on_error=continue_on_error,
            temp_dir=temp_dir,
            docker_label=docker_label,
            clean_up_docker_objects=(not debug),
            log_format=log_format,
            format_output_for_github=format_output_for_github,
        )
    except KeyboardInterrupt:
        print("\nKilled by user")
        print("Cleaning up Docker containers and volumes ...")
        success_flag = False
    finally:
        if not debug:
            delete_docker_entities("container", docker_label, ignore_errors=True)
            delete_docker_entities("volume", docker_label, ignore_errors=True)
            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            containers = find_docker_entities("container", docker_label)
            volumes = find_docker_entities("volume", docker_label)
            print(f"\n{'-' * 48}")
            print("\nRunning in --debug mode so not cleaning up. To clean up run:\n")
            for container in containers:
                print(f"  docker container rm --force {container}")
            for volume in volumes:
                print(f"  docker volume rm --force {volume}")
            print(f"  rm -rf {temp_dir}")
    return success_flag


def create_and_run_jobs(
    project_dir,
    actions,
    force_run_dependencies,
    continue_on_error,
    temp_dir,
    docker_label,
    clean_up_docker_objects=True,
    log_format=LOCAL_RUN_FORMAT,
    format_output_for_github=False,
):
    # Fiddle with the configuration to suit what we need for running local jobs
    docker.LABEL = docker_label
    # It's more helpful in this context to have things consistent
    config.RANDOMISE_JOB_ORDER = False
    config.HIGH_PRIVACY_WORKSPACES_DIR = project_dir.parent
    config.DATABASE_FILE = project_dir / "metadata" / "db.sqlite"
    config.TMP_DIR = temp_dir
    config.JOB_LOG_DIR = temp_dir / "logs"
    config.BACKEND = "expectations"
    config.USING_DUMMY_DATA_BACKEND = True
    config.CLEAN_UP_DOCKER_OBJECTS = clean_up_docker_objects

    # We want to fetch any reusable actions code directly from Github so as to
    # avoid pushing unnecessary traffic through the proxy
    config.GIT_PROXY_DOMAIN = "github.com"
    # Rather than using the throwaway `temp_dir` to store git repos in we use a
    # consistent directory within the system tempdir. This means we don't have
    # to keep refetching commits and also avoids the complexity of deleting
    # git's read-only directories on Windows. We use the current username as a
    # crude means of scoping the directory to the user in order to avoid
    # potential permissions issues if multiple users share the same directory.
    config.GIT_REPO_DIR = Path(tempfile.gettempdir()).joinpath(
        f"opensafely_{getuser()}"
    )

    # None of the below should be used when running locally
    config.WORKDIR = None
    config.HIGH_PRIVACY_STORAGE_BASE = None
    config.MEDIUM_PRIVACY_STORAGE_BASE = None
    config.MEDIUM_PRIVACY_WORKSPACES_DIR = None

    configure_logging(
        fmt=log_format,
        # All the other output we produce goes to stdout and it's a bit
        # confusing if the log messages end up on a separate stream
        stream=sys.stdout,
        # Filter out log messages in the local run context
        extra_filter=filter_log_messages,
    )

    # Any jobs that are running or pending must be left over from a previous run that was aborted either by an
    # unexpected and unhandled exception or by the researcher abruptly terminating the process. We can't reasonably
    # recover them (and the researcher may not want to -- maybe that's why they terminated), so we mark them as
    # cancelled. This causes the rest of the system to effectively ignore them.
    #
    # We do this here at the beginning rather than trying to catch these cases when the process exits because the
    # latter couldn't ever completely guarantee to catch every possible termination case correctly.
    database.update_where(
        Job,
        {"cancelled": True, "state": State.FAILED},
        state__in=[State.RUNNING, State.PENDING],
    )

    try:
        job_request, jobs = create_job_request_and_jobs(
            project_dir, actions, force_run_dependencies
        )
    except NothingToDoError:
        print("=> All actions already completed successfully")
        print("   Use -f option to force everything to re-run")
        return True
    except (ProjectValidationError, ReusableActionError, JobRequestError) as e:
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

    docker_images = get_docker_images(jobs)

    uses_stata = any(
        i.startswith(f"{config.DOCKER_REGISTRY}/stata-mp:") for i in docker_images
    )
    if uses_stata and config.STATA_LICENSE is None:
        config.STATA_LICENSE = get_stata_license()
        if config.STATA_LICENSE is None:
            print(
                "The docker image 'stata-mp' requires a license to function.\n"
                "\n"
                "If you are a member of OpenSAFELY we should have been able to fetch\n"
                "the license automatically, so something has gone wrong. Please open\n"
                "a new discussion here so we can help:\n"
                "  https://github.com/opensafely/documentation/discussions\n"
                "\n"
                "If you are not a member of OpenSAFELY you will have to provide your\n"
                "own license. See the dicussion here for pointers:\n"
                " https://github.com/opensafely/documentation/discussions/299"
            )
            return False

    for image in docker_images:
        if not docker.image_exists_locally(image):
            print(f"Fetching missing docker image: docker pull {image}")
            try:
                # We want to be chatty when running in the console so users can
                # see progress and quiet in CI so we don't spam the logs with
                # layer download noise
                docker.pull(image, quiet=not sys.stdout.isatty())
            except docker.DockerPullError as e:
                print("Failed with error:")
                print(e)
                return False

    action_names = [job.action for job in jobs]
    print(f"\nRunning actions: {', '.join(action_names)}\n")

    # Wrap all the log output inside an expandable block when running inside
    # Github Actions
    if format_output_for_github:
        print(f"::group::Job Runner Logs {ANSI.Grey}(click to view){ANSI.Reset}")

    # Run everything
    exit_condition = (
        no_jobs_remaining if continue_on_error else job_failed_or_none_remaining
    )
    try:
        run_main(exit_callback=exit_condition)
    except KeyboardInterrupt:
        pass
    finally:
        if format_output_for_github:
            print("::endgroup::")

    final_jobs = find_where(
        Job, state__in=[State.FAILED, State.SUCCEEDED], job_request_id=job_request.id
    )
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
        if format_output_for_github:
            print(f"{ANSI.Bold}=> {job.action}{ANSI.Reset}")
        else:
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
        # Output the entire log file inside an expandable block when running
        # inside Github Actions
        if format_output_for_github:
            print(
                f"::group:: log file: {log_file} {ANSI.Grey}(click to view){ANSI.Reset}"
            )
            long_grey_line = ANSI.Grey + ("\u2015" * 80) + ANSI.Reset
            print(long_grey_line)
            print((project_dir / log_file).read_text())
            print(long_grey_line)
            print("::endgroup::")
        else:
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


def create_job_request_and_jobs(project_dir, actions, force_run_dependencies):
    job_request = JobRequest(
        id=random_id(),
        repo_url=str(project_dir),
        commit=None,
        requested_actions=actions,
        cancelled_actions=[],
        workspace=project_dir.name,
        database_name="dummy",
        force_run_dependencies=force_run_dependencies,
        # The default behaviour of refusing to run if a dependency has failed
        # makes for an awkward workflow when iterating in development
        force_run_failed=True,
        branch="",
        original={"created_by": getuser()},
    )

    project_file_path = project_dir / "project.yaml"
    if not project_file_path.exists():
        raise ProjectValidationError(f"No project.yaml file found in {project_dir}")
    # NOTE: Similar but non-identical logic is implemented for running jobs in
    # production in `jobrunner.create_or_update_jobs.create_jobs`. If you make
    # changes below then consider what, if any, the appropriate corresponding
    # changes might be for production jobs.
    project = parse_and_validate_project_file(project_file_path.read_bytes())
    latest_jobs = calculate_workspace_state(job_request.workspace)

    # On the server out-of-band deletion of an existing output is considered an error, so we ignore that case when
    # scheduling and allow jobs with missing dependencies to fail loudly when they are actually run. However for local
    # running we should allow researchers to delete outputs on disk and automatically rerun the actions that create
    # if they are needed. So here we check whether any files are missing for completed actions and, if so, treat them
    # as though they had not been run -- this will automatically trigger a rerun.
    latest_jobs_with_files_present = [
        job for job in latest_jobs if all_output_files_present(project_dir, job)
    ]

    try:
        if not actions:
            raise UnknownActionError("At least one action must be supplied")
        new_jobs = get_new_jobs_to_run(
            job_request, project, latest_jobs_with_files_present
        )
    except UnknownActionError as e:
        # Annotate the exception with a list of valid action names so we can
        # show them to the user
        e.valid_actions = [RUN_ALL_COMMAND] + get_all_actions(project)
        raise e
    assert_new_jobs_created(new_jobs, latest_jobs_with_files_present)
    resolve_reusable_action_references(new_jobs)
    insert_into_database(job_request, new_jobs)
    return job_request, new_jobs


def all_output_files_present(project_dir, job):
    return all(project_dir.joinpath(f).exists() for f in job.output_files)


def no_jobs_remaining(active_jobs):
    return len(active_jobs) == 0


def job_failed_or_none_remaining(active_jobs):
    if any(job.state == State.FAILED for job in active_jobs):
        return True
    return len(active_jobs) == 0


def filter_log_messages(record):
    """
    Not all log messages are useful in the local run context so to avoid noise
    and make things clearer for the user we filter them out here
    """
    # None of these status messages are particularly useful in local run
    # mode, and they can generate a lot of clutter in large dependency
    # trees
    if getattr(record, "status_code", None) in {
        StatusCode.WAITING_ON_DEPENDENCIES,
        StatusCode.DEPENDENCY_FAILED,
        StatusCode.WAITING_ON_WORKERS,
    }:
        return False

    # We sometimes log caught exceptions for debugging purposes in production,
    # but we don't want to show these to the user when running locally
    if getattr(record, "exc_info", None):
        return False

    # Executor state logging is pretty verbose and unlikely to be useful for local running
    if record.name == executors.logging.LOGGER_NAME:
        return False

    return True


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


def find_docker_entities(entity, label):
    """
    Return list of names of all docker entities (of specified type) matching
    `label`
    """
    response = subprocess_run(
        [
            "docker",
            entity,
            "ls",
            *(["--all"] if entity == "container" else []),
            "--filter",
            f"label={label}",
            "--format",
            "{{ .Names }}" if entity == "container" else "{{ .Name }}",
            "--quiet",
        ],
        capture_output=True,
        encoding="ascii",
    )
    return response.stdout.split()


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


def get_stata_license(repo=config.STATA_LICENSE_REPO):
    """Load a stata license from local cache or remote repo."""
    cached = Path(f"{tempfile.gettempdir()}/opensafely-stata.lic")
    license_timeout = timedelta(hours=2)

    def git_clone(repo_url, cwd):
        cmd = ["git", "clone", "--depth=1", repo_url, "repo"]
        # GIT_TERMINAL_PROMPT=0 means it will fail if it requires auth. This
        # allows us to retry with an ssh url on linux/mac, as they would
        # generally prompt given an https url.
        result = subprocess_run(
            cmd,
            cwd=cwd,
            capture_output=True,
            env=dict(os.environ, GIT_TERMINAL_PROMPT="0"),
        )
        return result.returncode == 0

    fetch = False
    if cached.exists():
        mtime = datetime.fromtimestamp(cached.stat().st_mtime)
        if datetime.utcnow() - mtime > license_timeout:
            fetch = True
    else:
        fetch = True

    if fetch:
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
            pass
        finally:
            tmp.cleanup()

    if cached.exists():
        # if the refresh failed for some reason, update the last time it was
        # used to now to avoid spamming github on every subsequent run
        t = datetime.utcnow().timestamp()
        os.utime(cached, (t, t))
        return cached.read_text()
    else:
        return None


def docker_preflight_check():
    try:
        subprocess_run(["docker", "info"], check=True, capture_output=True)
    except FileNotFoundError:
        print("Could not find application: docker")
        print("\nYou must have Docker installed to run this command, see:")
        print("https://docs.docker.com/get-docker/")
        return False
    except subprocess.CalledProcessError:
        print("There was an error running: docker info")
        print("\nIt looks like you have Docker installed but have not started it.")
        return False
    return True


def getuser():
    # `getpass.getuser()` can fail on Windows under certain circumstances. It's never
    # critical that we know the current username so we don't want to block execution if
    # this happens
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def run():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser = add_arguments(parser)
    args = parser.parse_args()
    success = main(**vars(args))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    run()
