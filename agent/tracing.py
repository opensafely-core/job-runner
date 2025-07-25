import logging

from common.job_executor import JobDefinition
from common.schema import AgentTask
from common.tracing import backwards_compatible_job_attrs, set_span_attributes


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
    """These attributes are added to every task span in order to slice and dice by
    each as needed.
    Note that task definition is not set on the task trace; we assume that the
    definition contains task-type-specific info that will be set on the relevant
    task type (e.g. a RUNJOB task will set job metadata)
    """
    attrs = dict(
        backend=task.backend,
        task=task.id,
        task_type=task.type.name,
        # convert seconds to ns integer
        task_created_at=int(task.created_at * 1e9),
        **task.attributes,
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

        # temporary backwards compatibility, can remove after a few months
        set_span_attributes(span, backwards_compatible_job_attrs(attributes))

    except Exception:
        # make sure trace failures do not error the job
        logger.exception(f"failed to trace job {job.id}")


def trace_job_attributes(job: JobDefinition):
    """These attributes are added to every task span in order to slice and dice by
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
        input_job_ids=",".join(job.input_job_ids or []),
        allow_database_access=job.allow_database_access,
        cpu_count=job.cpu_count,
        memory_limit=job.memory_limit,
    )

    return attrs


def set_job_results_metadata(span, results, attributes=None):
    attributes = attributes or {}
    try:
        attributes = trace_job_results_attributes(results, attributes)
        set_span_attributes(span, attributes)
    except Exception:
        # make sure trace failures do not error the job
        logger.exception(
            f"failed to trace job results for job {span.attributes.get('id')}"
        )


def trace_job_results_attributes(results, attributes):
    if results:
        attributes.update(
            dict(
                exit_code=results["exit_code"],
                image_id=results["docker_image_id"],
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
    return attributes
