import time

job_spec = {
    "url": "http://test.com/jobs/0/",
    "repo": "myrepo",
    "branch": "mybranch",
    "backend": "tpp",
    "db": "full",
    "operation": "generate_cohort",
}

default_job = job_spec.copy()
default_job.update(
    {
        "status_code": None,
        "status_message": "",
        "outputs": [],
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
        return self.job_spec


class SlowJob(TestJob):
    def __call__(self):
        time.sleep(1)
        return self.job_spec


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
