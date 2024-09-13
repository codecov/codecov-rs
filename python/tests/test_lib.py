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

    with NamedTemporaryFile() as out_file:
        report_builder = SqliteReportBuilder.from_pyreport(
            report_json_filepath, chunks_filepath, out_file.name
        )
        print(report_builder.filepath())
        assert report_builder.filepath() is not None
