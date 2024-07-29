-- See `src/report/models.rs` for complete, up-to-date schema documentation.

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE source_file (
    -- This should be set to the hash of the `path` column so that we can
    -- distribute processing across multiple different hosts and they will
    -- all come up with the same ID.
    id INTEGER PRIMARY KEY,

    path VARCHAR NOT NULL
);

-- TODO: Allow distinguishing between raw reports within a single upload
-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE raw_upload (
    -- This should be set to a random 64-bit integer so that we can
    -- distribute processing across multiple different hosts and they will
    -- not fight over autoincrementing ID values.
    id INTEGER PRIMARY KEY,

    timestamp INTEGER,
    raw_upload_url VARCHAR,
    flags VARCHAR, -- JSON
    provider VARCHAR,
    build VARCHAR,
    name VARCHAR,
    job_name VARCHAR,
    ci_run_url VARCHAR,
    state VARCHAR,
    env VARCHAR,
    session_type VARCHAR,
    session_extras VARCHAR -- JSON,
);

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE context (
    -- This should be set to the hash of the `name` column so that we can
    -- distribute processing across multiple different hosts and they will
    -- all come up with the same ID.
    id INTEGER PRIMARY KEY,

    context_type VARCHAR NOT NULL,
    name VARCHAR NOT NULL
);

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE context_assoc (
    context_id INTEGER REFERENCES context(id) NOT NULL,

    raw_upload_id INTEGER NOT NULL,
    local_sample_id INTEGER,
    local_span_id INTEGER,

    -- TODO: Figure out how to re-enable these
--    FOREIGN KEY (raw_upload_id, local_sample_id) REFERENCES coverage_sample(raw_upload_id, local_sample_id),
--    FOREIGN KEY (raw_upload_id, local_span_id) REFERENCES span_data(raw_upload_id, local_span_id),

    PRIMARY KEY (context_id, raw_upload_id, local_sample_id, local_span_id)
);

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE coverage_sample (
    raw_upload_id INTEGER REFERENCES raw_upload(id) NOT NULL,

    -- This should be an application-managed auto-incremented integer.
    local_sample_id INTEGER NOT NULL,

    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    line_no INTEGER NOT NULL,

    coverage_type VARCHAR NOT NULL,
    hits INTEGER,
    hit_branches INTEGER,
    total_branches INTEGER,

    PRIMARY KEY (raw_upload_id, local_sample_id)
);

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE branches_data (
    raw_upload_id INTEGER REFERENCES raw_upload(id) NOT NULL,
    local_sample_id INTEGER NOT NULL,

    -- This should be an application-managed auto-incremented integer.
    local_branch_id INTEGER NOT NULL,

    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,

    hits INTEGER NOT NULL,
    branch_format VARCHAR NOT NULL,
    branch VARCHAR NOT NULL,

    FOREIGN KEY (raw_upload_id, local_sample_id) REFERENCES coverage_sample(raw_upload_id, local_sample_id),
    PRIMARY KEY (raw_upload_id, local_branch_id)
);

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE method_data (
    raw_upload_id INTEGER REFERENCES raw_upload(id) NOT NULL,
    local_sample_id INTEGER NOT NULL,

    -- This should be an application-managed auto-incremented integer.
    local_method_id INTEGER NOT NULL,

    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    line_no INTEGER,

    hit_branches INTEGER,
    total_branches INTEGER,
    hit_complexity_paths INTEGER,
    total_complexity INTEGER,

    FOREIGN KEY (raw_upload_id, local_sample_id) REFERENCES coverage_sample(raw_upload_id, local_sample_id),
    PRIMARY KEY (raw_upload_id, local_method_id)
);

-- TODO: Measure size/perf impact of making this table `WITHOUT ROWID`
CREATE TABLE span_data (
    raw_upload_id INTEGER REFERENCES raw_upload(id) NOT NULL,
    local_sample_id INTEGER,

    -- This should be an application-managed auto-incremented integer.
    local_span_id INTEGER NOT NULL,

    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,

    hits INTEGER NOT NULL,
    start_line INTEGER,
    start_col INTEGER,
    end_line INTEGER,
    end_col INTEGER,

    FOREIGN KEY (raw_upload_id, local_sample_id) REFERENCES coverage_sample(raw_upload_id, local_sample_id),
    PRIMARY KEY (raw_upload_id, local_span_id)
);
