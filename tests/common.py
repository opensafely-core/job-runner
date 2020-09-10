import time

job_spec = {
    "url": "http://test.com/jobs/0/",
    "backend": "tpp",
    "repo": "myrepo",
    "pk": 1,
    "action_id": "generate_cohort",
    "force_run": False,
    "force_run_dependencies": False,
    "output_locations": [],
    "workspace": {
        "id": 1,
        "name": "workspace",
        "repo": "repo",
        "owner": "testowner",
        "branch": "mybranch",
        "db": "full",
    },
    "workspace_id": 1,
}

default_job = job_spec.copy()
default_job.update(
    {
        "status_code": None,
        "status_message": "",
        "created_at": None,
        "started_at": None,
        "completed_at": None,
    }
)


class TestJob:
    def __init__(self, job_spec):
        self.job_spec = job_spec

    def __repr__(self):
        return self.__class__.__name__


class WorkingJob(TestJob):
    def __call__(self):
        return [self.job_spec]


class SlowJob(TestJob):
    def __call__(self):
        time.sleep(1)
        return [self.job_spec]


class BrokenJob(TestJob):
    def __call__(self):
        raise KeyError


def test_job_list(job=None):
    if job is None:
        job = default_job.copy()
    return {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [job],
    }
