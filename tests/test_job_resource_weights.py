import textwrap

from jobrunner.config import parse_job_resource_weights
from jobrunner.models import Job
from jobrunner.run import get_job_resource_weight


def test_job_resource_weights(tmp_path):
    config = textwrap.dedent(
        """
        [my-workspace]
        some_action = 2.5
        pattern[\\d]+ = 4
        """
    )
    config_file = tmp_path / "config.ini"
    config_file.write_text(config)
    weights = parse_job_resource_weights(config_file)
    job = Job(workspace="foo", action="bar")
    assert get_job_resource_weight(job, weights=weights) == 1
    job = Job(workspace="my-workspace", action="some_action")
    assert get_job_resource_weight(job, weights=weights) == 2.5
    job = Job(workspace="my-workspace", action="pattern315")
    assert get_job_resource_weight(job, weights=weights) == 4
    job = Job(workspace="my-workspace", action="pattern000no_match")
    assert get_job_resource_weight(job, weights=weights) == 1
