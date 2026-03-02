import textwrap

from controller.config import parse_job_resource_weights
from controller.main import get_job_resource_weight
from controller.models import Job


def test_job_resource_weights(tmp_path):
    config_file_template = "config_{backend}.ini"
    config = textwrap.dedent(
        """
        [my-workspace]
        some_action = 2.5
        pattern[\\d]+ = 4
        """
    )
    config_file = tmp_path / "config_test.ini"
    config_file.write_text(config)
    weights = parse_job_resource_weights(str(tmp_path / config_file_template))
    job = Job(workspace="foo", action="bar", backend="test")
    assert get_job_resource_weight(job, weights=weights) == 1
    job = Job(workspace="my-workspace", action="some_action", backend="test")
    assert get_job_resource_weight(job, weights=weights) == 2.5
    other_backend_job = Job(
        workspace="my-workspace", action="some_action", backend="other"
    )
    assert get_job_resource_weight(other_backend_job, weights=weights) == 1
    job = Job(workspace="my-workspace", action="pattern315", backend="test")
    assert get_job_resource_weight(job, weights=weights) == 4
    other_backend_job = Job(
        workspace="my-workspace", action="pattern315", backend="other"
    )
    assert get_job_resource_weight(other_backend_job, weights=weights) == 1
    job = Job(workspace="my-workspace", action="pattern000no_match", backend="test")
    assert get_job_resource_weight(job, weights=weights) == 1


def test_job_resource_weights_defaults(tmp_path):
    config_file_template = "config_{backend}.ini"
    config = textwrap.dedent(
        """
        [default]
        my-action:v[\\d]+\\s+do-thing.+ = 3
        ehrql:v[\\d]+\\s+generate-measures.+ = 4.5

        [my-workspace]
        some_action = 2.5
        pattern[\\d]+ = 4
        """
    )
    config_file = tmp_path / "config_test.ini"
    config_file.write_text(config)
    weights = parse_job_resource_weights(str(tmp_path / config_file_template))

    # no entry in config for workspace, no match for defaults
    job = Job(workspace="foo", action="bar", run_command="", backend="test")
    assert get_job_resource_weight(job, weights=weights) == 1

    # no entry in config for workspace, matches a default run_command
    job = Job(
        workspace="foo",
        action="some_action",
        run_command="my-action:v1 do-thing --output foo",
        backend="test",
    )
    assert get_job_resource_weight(job, weights=weights) == 3

    # matches a workspace action AND matches a default run_command; workspace config wins
    job = Job(
        workspace="my-workspace",
        action="some_action",
        run_command="my-action:v1 do-thing --output foo",
        backend="test",
    )
    assert get_job_resource_weight(job, weights=weights) == 2.5

    # has an entry in config for workspace but doesn't match an action, does match a default run_command
    job = Job(
        workspace="my-workspace",
        action="generate_measures",
        run_command="ehrql:v1 generate-measures analysis/definition.py --output results.csv",
        backend="test",
    )
    assert get_job_resource_weight(job, weights=weights) == 4.5
