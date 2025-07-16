# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Flags(models.Model):
    pk = models.CompositePrimaryKey("id", "backend")
    id = models.TextField()
    value = models.TextField()
    backend = models.TextField()
    timestamp = models.TextField()  # This field type is a guess.

    class Meta:
        managed = False
        db_table = "flags"


class Job(models.Model):
    id = models.TextField(primary_key=True)
    job_request_id = models.TextField()
    state = models.TextField()
    repo_url = models.TextField()
    commit = models.TextField()
    workspace = models.TextField()
    database_name = models.TextField()
    action = models.TextField()
    action_repo_url = models.TextField()
    action_commit = models.TextField()
    requires_outputs_from = models.TextField()
    wait_for_job_ids = models.TextField()
    run_command = models.TextField()
    image_id = models.TextField()
    output_spec = models.TextField()
    outputs = models.TextField()
    unmatched_outputs = models.TextField()
    status_message = models.TextField()
    status_code = models.TextField()
    cancelled = models.BooleanField()
    created_at = models.IntegerField()
    updated_at = models.IntegerField()
    started_at = models.IntegerField()
    completed_at = models.IntegerField()
    trace_context = models.TextField()
    status_code_updated_at = models.IntegerField()
    level4_excluded_files = models.TextField()
    requires_db = models.BooleanField()
    backend = models.TextField()

    class Meta:
        managed = False
        db_table = "job"


class JobRequest(models.Model):
    id = models.TextField(
        primary_key=True,
    )
    original = models.TextField()

    class Meta:
        managed = False
        db_table = "job_request"


class Tasks(models.Model):
    id = models.TextField(
        primary_key=True,
    )
    backend = models.TextField()
    type = models.TextField()
    definition = models.TextField()
    active = models.BooleanField()
    created_at = models.IntegerField()
    finished_at = models.IntegerField()
    attributes = models.TextField()
    agent_stage = models.TextField()
    agent_complete = models.BooleanField()
    agent_results = models.TextField()
    agent_timestamp_ns = models.IntegerField()

    class Meta:
        managed = False
        db_table = "tasks"
