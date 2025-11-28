import logging

from django.core.management.base import BaseCommand

from controller.lib import database
from controller.models import Job
from controller.queries import get_saved_job_request


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Development utility for creating and submitting a RAP without having a
    job-server
    """

    def handle(self, **options):
        logger.info("Backpopulating Job from SavedJobRequest")
        # Find jobs where the attributes we're going to populate are all None; this will
        # avoid re-fetching jobs that we've already updated, or ones that have been run after
        # the new attributes were added. It will re-fetch jobs that have no SavedJobRequest at
        # all, but there are only 660 of those, and they'll be skipped in the update below.
        jobs = database.find_where(Job, project=None, orgs=None, branch=None, user=None)

        total_jobs_found = len(jobs)
        self.stdout.write(f"{total_jobs_found} jobs to update")
        updated = 0
        missing = 0
        for i, job in enumerate(jobs, start=1):
            saved_job_request = get_saved_job_request(job=job)
            # As of 2025-11-28, in the rap-controller prod db:
            # - 660 jobs have no saved job request
            # - 14 saved job requests have no created_by
            # - 39643 saved job requests have no project
            # - 39643 saved job requests have no orgs
            # - no saved job requests are missing workspace/branch
            if saved_job_request:
                project = saved_job_request.get("project", "unknown")
                orgs = saved_job_request.get("orgs", [])
                user = saved_job_request.get("created_by", "unknown")
                branch = saved_job_request.get("workspace", {}).get("branch", "unknown")

                # update_where runs with @ensure_transaction, and only updates these fields
                database.update_where(
                    Job,
                    dict(project=project, orgs=orgs, user=user, branch=branch),
                    id=job.id,
                )
                updated += 1
            else:
                missing += 1

            if i % 1000 == 0:  # pragma: no cover
                self.stdout.write(f"{i}/{total_jobs_found} processed")

        logging.info(
            "%d/%d jobs updated (%d missing saved job request)",
            total_jobs_found,
            updated,
            missing,
        )
