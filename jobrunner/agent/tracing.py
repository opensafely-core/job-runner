import logging

from jobrunner.job_executor import JobDefinition
from jobrunner.schema import AgentTask
from jobrunner.tracing import set_span_attributes


logger = logging.getLogger(__name__)


OTEL_ATTR_TYPES = (bool, str, bytes, int, float)


def set_task_span_metadata(span, task: AgentTask, **attrs):
    """Set span metadata with everything we know about a task."""
    try:
        attributes = {}

        if attrs:
            attributes.update(attrs)
        attributes.update(trace_task_attributes(task))

        set_span_attributes(span, attributes)
    except Exception:
        # make sure trace failures do not error the task
        logger.exception(f"failed to trace task {task.id}")


def trace_task_attributes(task: AgentTask):
    """These attributes are added to every span in order to slice and dice by
    each as needed.
    Note that task definition is not set on the task trace; we assume that the
    definition contains task-type-specific info that will be set on the releavent
    task type (e.g. a RUNJOB task will set job metadata)
    """
    attrs = dict(
        backend=task.backend,
        task=task.id,
        task_type=task.type.name,
        # convert seconds to ns integer
        task_created_at=int(task.created_at * 1e9),
    )

    return attrs


def set_job_span_metadata(span, job: JobDefinition, **attrs):
    """Set span metadata with everything we know about a job."""
    try:
        attributes = {}

        if attrs:
            attributes.update(attrs)

        attributes.update(trace_job_attributes(job))

        set_span_attributes(span, attributes)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace job {job.id}")


def trace_job_attributes(job: JobDefinition):
    """These attributes are added to every span in order to slice and dice by
    each as needed.
    """
    repo_url = job.study.git_repo_url or ""
    commit = job.study.commit or ""

    attrs = dict(
        job=job.id,
        job_request=job.job_request_id,
        workspace=job.workspace,
        repo_url=repo_url,
        commit=commit,
        action=job.action,
        # convert seconds to ns integer
        job_created_at=int(job.created_at * 1e9),
        image=job.image,
        args=",".join(job.args or []),
        inputs=",".join(job.inputs or []),
        allow_database_access=job.allow_database_access,
        cpu_count=job.cpu_count,
        memory_limit=job.memory_limit,
    )

    return attrs


def set_job_results_metadata(span, results, attributes=None):
    try:
        attributes = attributes or {}

        if results:
            attributes.update(
                dict(
                    exit_code=results["exit_code"],
                    image_id=results["docker_image_id"],
                    outputs=len(results["outputs"]),
                    unmatched_patterns=len(results["unmatched_patterns"]),
                    unmatched_outputs=len(results["unmatched_outputs"]),
                    executor_message=results["status_message"],
                    action_version=results["action_version"],
                    action_revision=results["action_revision"],
                    action_created=results["action_created"],
                    base_revision=results["base_revision"],
                    base_created=results["base_created"],
                    cancelled=results["cancelled"],
                )
            )
            if "error" in results:
                attributes.update(error=results["error"])

        set_span_attributes(span, attributes)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(
            f"failed to trace job results for job {span.attributes.get('id')}"
        )
