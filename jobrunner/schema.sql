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
    created_at INT,
    updated_at INT,
    started_at INT,
    completed_at INT,

    PRIMARY KEY (id)
);

CREATE INDEX idx_job__job_request_id ON job (job_request_id);

-- Once jobs transition into a terminal state (failed or succeeded) they become
-- basically irrelevant from the application's point of view as it never needs
-- to query them. By creating an index only on non-terminal states we ensure
-- that it always stays relatively small even as the set of historical jobs
-- grows.
CREATE INDEX idx_job__status ON job (status) WHERE status NOT IN ('F', 'S');
