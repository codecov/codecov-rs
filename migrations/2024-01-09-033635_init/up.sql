CREATE TABLE source_file (
    id INTEGER PRIMARY KEY,
    path VARCHAR NOT NULL
);

CREATE TABLE line_status (
    id INTEGER PRIMARY KEY,
    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    line_no INTEGER NOT NULL,
    coverage_status INTEGER NOT NULL
);

CREATE TABLE branch_status (
    id INTEGER PRIMARY KEY,
    source_file_id INTEGER REFERENCES source_file(id) NOT NULL,
    start_line_no INTEGER NOT NULL,
    end_line_no INTEGER NOT NULL,
    coverage_status INTEGER NOT NULL
);

CREATE TABLE context_assoc (
    context_id INTEGER NOT NULL,
    line_id INTEGER,
    branch_id INTEGER,
    PRIMARY KEY(context_id, line_id, branch_id)
);

CREATE TABLE context (
    id INTEGER PRIMARY KEY,
    context_type VARCHAR NOT NULL,
    name VARCHAR NOT NULL
);
