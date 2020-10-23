-- In one sense we don't need to store JobRequest at all because once the
-- relevant Jobs are created the job-runner has no further use for it. However
-- we want to be able to log the original JobRequest as metadata along with the
-- job outputs, so we store it here as blob of JSON.
CREATE TABLE job_request (
    id TEXT,
    original_json TEXT,

    PRIMARY KEY (id)
);

CREATE TABLE job (
    id TEXT,
    job_request_id TEXT,
    status TEXT,
    repo_url TEXT,
    -- We call this "commit" elsewhere but thats not a great name for a
    -- database column
    sha TEXT,
    workspace TEXT,
    action TEXT,
    -- The below two fields are related but distinct: the first is a list of
    -- currently runnning job_ids which we must wait to complete before we can
    -- start. The second is a list of action_ids whos outputs must be copied
    -- into the job container.
    wait_for_job_ids_json TEXT,
    requires_outputs_from_json TEXT,
    run_command TEXT,
    output_spec_json TEXT,
    output_files_json TEXT,
    error_message TEXT,

    PRIMARY KEY (id)
);
