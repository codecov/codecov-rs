from pathlib import Path
from tempfile import NamedTemporaryFile

from codecov_rs.report import SqliteReportBuilder

PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_fixture_path(path_from_root: str) -> str:
    return str(PROJECT_ROOT / path_from_root)


def test_from_pyreport():
    report_json_filepath = get_fixture_path(
        "test_utils/fixtures/pyreport/codecov-rs-reports-json-d2a9ba1.txt"
    )
    chunks_filepath = get_fixture_path(
        "test_utils/fixtures/pyreport/codecov-rs-chunks-d2a9ba1.txt"
    )

    # `NamedTemporaryFile` is finnicky on Windows:
    # https://docs.python.org/3/library/tempfile.html#tempfile.NamedTemporaryFile
    # `delete_on_close=False` is needed to allow sqlite to also open the file,
    # and `del report_builder` is needed to allow the context manager to delete
    # it.
    with NamedTemporaryFile(delete_on_close=False) as out_file:
        report_builder = SqliteReportBuilder.from_pyreport(
            report_json_filepath, chunks_filepath, out_file.name
        )
        assert report_builder.filepath() is not None
        del report_builder
