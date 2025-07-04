import textwrap

from controller.main import get_job_resource_weight
from controller.models import Job
from jobrunner.config.controller import parse_job_resource_weights


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
