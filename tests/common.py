import time

default_job = {
    "url": "http://test.com/jobs/0/",
    "repo": "myrepo",
    "tag": "mytag",
    "backend": "tpp",
    "db": "full",
    "started": False,
    "operation": "generate_cohort",
    "status_code": None,
    "status_message": "",
    "output_bucket": "output_bucket",
    "created_at": None,
    "started_at": None,
    "completed_at": None,
}


class TestJobRunner:
    def __init__(self, job):
        self.job = job

    def __repr__(self):
        return self.__class__.__name__


class WorkingJobRunner(TestJobRunner):
    def __call__(self):
        return self.job


class SlowJobRunner(TestJobRunner):
    def __call__(self):
        time.sleep(1)
        return self.job


class BrokenJobRunner(TestJobRunner):
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
