CREATE TABLE source_file (
    id INTEGER PRIMARY KEY,
    path VARCHAR NOT NULL
) WITHOUT ROWID;

CREATE TABLE coverage_sample (
    id BLOB PRIMARY KEY,
    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    line_no INTEGER NOT NULL,
    coverage_type VARCHAR NOT NULL,
    hits INTEGER,
    hit_branches INTEGER,
    total_branches INTEGER
) WITHOUT ROWID;

CREATE TABLE branches_data (
    id BLOB PRIMARY KEY,
    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    sample_id BLOB REFERENCES coverage_sample(id) NOT NULL,
    hits INTEGER NOT NULL,
    branch_format VARCHAR NOT NULL,
    branch VARCHAR NOT NULL
) WITHOUT ROWID;

CREATE TABLE method_data (
    id BLOB PRIMARY KEY,
    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    sample_id BLOB REFERENCES coverage_sample(id),
    line_no INTEGER,
    hit_branches INTEGER,
    total_branches INTEGER,
    hit_complexity_paths INTEGER,
    total_complexity INTEGER
) WITHOUT ROWID;

CREATE TABLE span_data (
    id BLOB PRIMARY KEY,
    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    sample_id BLOB REFERENCES coverage_sample(id),
    hits INTEGER NOT NULL,
    start_line INTEGER,
    start_col INTEGER,
    end_line INTEGER,
    end_col INTEGER
) WITHOUT ROWID;

CREATE TABLE context (
    id INTEGER PRIMARY KEY,
    context_type VARCHAR NOT NULL,
    name VARCHAR NOT NULL
);

CREATE TABLE context_assoc (
    context_id INTEGER NOT NULL,
    sample_id BLOB,
    branch_id BLOB,
    method_id BLOB,
    span_id BLOB,
    PRIMARY KEY(context_id, sample_id)
) WITHOUT ROWID;

CREATE TABLE upload_details (
    context_id INTEGER REFERENCES context(id) NOT NULL,
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
