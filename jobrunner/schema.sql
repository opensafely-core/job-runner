-- See jobrunner/models.py for comments on the fields here

CREATE TABLE job_request (
    id TEXT,
    original TEXT,

    PRIMARY KEY (id)
);

CREATE TABLE job (
    id TEXT,
    job_request_id TEXT,
    state TEXT,
    repo_url TEXT,
    "commit" TEXT,
    workspace TEXT,
    database_name TEXT,
    action TEXT,
    requires_outputs_from TEXT,
    wait_for_job_ids TEXT,
    run_command TEXT,
    output_spec TEXT,
    outputs TEXT,
    unmatched_outputs TEXT,
    status_message TEXT,
    status_code TEXT,
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
CREATE INDEX idx_job__state ON job (state) WHERE state NOT IN ('failed', 'succeeded');
