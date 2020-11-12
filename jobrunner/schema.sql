-- In one sense we don't need to store JobRequest at all because once the
-- relevant Jobs are created the job-runner has no further use for it. However
-- we want to be able to log the original JobRequest as metadata along with the
-- job outputs, so we store it here as blob of JSON.
CREATE TABLE job_request (
    id TEXT,
    original TEXT,

    PRIMARY KEY (id)
);

CREATE TABLE job (
    id TEXT,
    job_request_id TEXT,
    status TEXT,
    repo_url TEXT,
    "commit" TEXT,
    workspace TEXT,
    database_name TEXT,
    action TEXT,
    -- The below two fields are related but distinct: the first is a list of
    -- currently runnning job_ids which we must wait to complete before we can
    -- start. The second is a list of action_ids whose outputs must be copied
    -- into the job container.
    wait_for_job_ids TEXT,
    requires_outputs_from TEXT,
    run_command TEXT,
    output_spec TEXT,
    status_message TEXT,
    last_updated INT,

    PRIMARY KEY (id)
);

CREATE INDEX idx_job__job_request_id ON job (job_request_id);

CREATE INDEX idx_job__status ON job (status);
