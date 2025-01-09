# codecov-rs

![Actions](https://github.com/codecov/codecov-rs/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/codecov/codecov-rs/graph/badge.svg?token=IEGybruDEg)](https://codecov.io/gh/codecov/codecov-rs)

Library for processing code coverage reports.

Supported formats include:

- `codecov-rs`'s SQLite format described in `src/report/models.rs`
- Codecov's Python report implementation ("pyreport")

See `core/src/parsers` or the list of features in `core/Cargo.toml` for a complete list. All formats are converted to `codecov-rs`'s SQLite format ([inspired by `coverage.py`](https://coverage.readthedocs.io/en/latest/dbschema.html)) and converting back is generally not a goal (pyreport being the exception).

All details (e.g. SQLite schema, code interfaces) subject to breaking changes until further notice. In the future, we will at least use SQLite's [`schema_version` pragma](https://www.sqlite.org/pragma.html#pragma_schema_version) to attempt backwards compatibility.

## Developing

Set up your development environment:

- To work on the Python bindings, run `source .envrc` (or use `direnv`) to set up a virtual environment. Update development dependencies with `pip install -r python/requirements.dev.txt`
- Install lint hooks with `pip install pre-commit && pre-commit install`.
- Large sample test reports are checked in using [Git LFS](https://git-lfs.com/) in `test_utils/fixtures/**/large` directories (e.g. `test_utils/fixtures/pyreport/large`). Tests and benchmarks may reference them so installing it yourself is recommended.

`codecov-rs` aims to serve as effective documentation for every flavor of every format it supports. To that end, the following are greatly appreciated in submissions:

- Thorough doc comments (`///` / `/**`). For parsers, include snippets that show what inputs look like
- Granular, in-module unit tests
- Integration tests with real-world samples (that are safe to distribute; don't send us data from your private repo)

The `core/examples/` directory contains runnable commands for developers including:

- `parse_pyreport`: converts a given pyreport into a SQLite report
- `sql_to_pyreport`: converts a given SQLite report into a pyreport (report JSON + chunks file)

You can run an example with `cargo run --example <example> <arguments>`. Consider following suit for your own new feature.

### Repository structure

- `core/`: Rust crate with all of the core coverage-processing functionality
- `bindings/`: Rust crate with PyO3 bindings for `core/`
- `test_utils/`: Rust crate with utilities for Rust tests and sample data for any tests
  - `test_utils/fixtures`: Checked-in sampled data. Large samples are checked in with Git LFS
- `python/codecov_rs`: Python code using/typing the Rust crate in `bindings/`
- `python/tests`: Python tests

`Cargo.toml` in the root defines a Cargo workspace. `pyproject.toml` in the root defines our Python package. Development dependencies for the Python code are in `python/requirements.dev.txt`.

### Writing new parsers

**TBD: Design not settled**

New parsers should be optional via Cargo features. Adding them to the default featureset is fine.

Where possible, parsers should not load their entire input or output into RAM. On the input side, you can avoid that with a _streaming_ parser or by using `memmap2` to map the input file into virtual memory. SQLite makes it straightforward enough to stream outputs to the database.

Coverage formats really run the gamut so there's no one-size-fits-all framework we can use. Some options:

- [`quick_xml`](https://crates.io/crates/quick_xml), a streaming XML parser
- [`serde`](https://serde.rs/), a popular serialization/deserialization framework
  - `serde`'s docs illustrate [how one can write a streaming parser](https://serde.rs/stream-array.html)

Non-XML formats lack clean OOTB support for streaming so `codecov-rs` currently relies more on the mmap approach.

### Testing

Run tests with:

```
# Rust tests
$ cargo test

# Python tests
$ pytest
```

### Benchmarks

Run benchmarks with:

```
$ cargo bench --features testing
```
