use codecov_rs::{
    error::Result,
    parsers::{common::ReportBuilderCtx, pyreport::report_json},
    report::{models, Report, ReportBuilder},
};
use divan::Bencher;
use winnow::Parser as _;

fn main() {
    divan::main();
}

#[divan::bench]
fn simple_report() {
    let reports = &[
        r#"{"files": {"src/report.rs": [0, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}}}"#,
        r#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#,
        r#"{"files": {}, "sessions": {"0": {"j": "codecov-rs CI"}, "1": {"j": "codecov-rs CI 2"}}}"#,
        r#"{"files": {"src/report.rs": [0, {}, [], null], "src/report/models.rs": [1, {}, [], null]}, "sessions": {}}"#,
        r#"{"files": {}, "sessions": {}}"#,
    ];

    for input in reports {
        run_parsing(input);
    }
}

// parsing this is quite slow
#[divan::bench(sample_count = 10)]
fn complex_report(bencher: Bencher) {
    // this is a ~11M `report_json`
    let path =
        "./fixtures/pyreport/large/worker-c71ddfd4cb1753c7a540e5248c2beaa079fc3341-report_json.json";
    let Ok(report) = std::fs::read_to_string(path) else {
        println!("Failed to read test report");
        return;
    };

    if report.starts_with("version https://git-lfs.github.com/spec/v1\n") {
        println!("Sample report has not been pulled from Git LFS");
        return;
    }

    bencher.bench(|| run_parsing(&report));
}

fn run_parsing(input: &str) {
    let report_builder = TestReport::default();
    let mut stream = report_json::ReportOutputStream::<&str, TestReport, TestReport> {
        input,
        state: ReportBuilderCtx::new(report_builder),
    };
    report_json::parse_report_json
        .parse_next(&mut stream)
        .unwrap();
}

#[derive(Debug, Default)]
struct TestReport {
    files: Vec<models::SourceFile>,
    uploads: Vec<models::RawUpload>,
}

impl Report for TestReport {
    fn list_files(&self) -> Result<Vec<models::SourceFile>> {
        todo!()
    }

    fn list_contexts(&self) -> Result<Vec<models::Context>> {
        todo!()
    }

    fn list_coverage_samples(&self) -> Result<Vec<models::CoverageSample>> {
        todo!()
    }

    fn list_branches_for_sample(
        &self,
        _sample: &models::CoverageSample,
    ) -> Result<Vec<models::BranchesData>> {
        todo!()
    }

    fn get_method_for_sample(
        &self,
        _sample: &models::CoverageSample,
    ) -> Result<Option<models::MethodData>> {
        todo!()
    }

    fn list_spans_for_sample(
        &self,
        _sample: &models::CoverageSample,
    ) -> Result<Vec<models::SpanData>> {
        todo!()
    }

    fn list_contexts_for_sample(
        &self,
        _sample: &models::CoverageSample,
    ) -> Result<Vec<models::Context>> {
        todo!()
    }

    fn list_samples_for_file(
        &self,
        _file: &models::SourceFile,
    ) -> Result<Vec<models::CoverageSample>> {
        todo!()
    }

    fn list_raw_uploads(&self) -> Result<Vec<models::RawUpload>> {
        todo!()
    }

    fn merge(&mut self, _other: &Self) -> Result<()> {
        todo!()
    }

    fn totals(&self) -> Result<models::ReportTotals> {
        todo!()
    }
}

impl ReportBuilder<TestReport> for TestReport {
    fn insert_file(&mut self, path: String) -> Result<models::SourceFile> {
        let file = models::SourceFile {
            id: seahash::hash(path.as_bytes()) as i64,
            path,
        };
        self.files.push(file.clone());
        Ok(file)
    }

    fn insert_raw_upload(
        &mut self,
        mut upload_details: models::RawUpload,
    ) -> Result<models::RawUpload> {
        upload_details.id = self.uploads.len() as i64;
        self.uploads.push(upload_details.clone());
        Ok(upload_details)
    }

    fn insert_context(
        &mut self,
        _context_type: models::ContextType,
        _name: &str,
    ) -> Result<models::Context> {
        todo!()
    }

    fn insert_coverage_sample(
        &mut self,
        _sample: models::CoverageSample,
    ) -> Result<models::CoverageSample> {
        todo!()
    }

    fn multi_insert_coverage_sample(
        &mut self,
        _samples: Vec<&mut models::CoverageSample>,
    ) -> Result<()> {
        todo!()
    }

    fn insert_branches_data(
        &mut self,
        _branch: models::BranchesData,
    ) -> Result<models::BranchesData> {
        todo!()
    }

    fn multi_insert_branches_data(
        &mut self,
        _branches: Vec<&mut models::BranchesData>,
    ) -> Result<()> {
        todo!()
    }

    fn insert_method_data(&mut self, _method: models::MethodData) -> Result<models::MethodData> {
        todo!()
    }

    fn multi_insert_method_data(&mut self, _methods: Vec<&mut models::MethodData>) -> Result<()> {
        todo!()
    }

    fn insert_span_data(&mut self, _span: models::SpanData) -> Result<models::SpanData> {
        todo!()
    }

    fn multi_insert_span_data(&mut self, _spans: Vec<&mut models::SpanData>) -> Result<()> {
        todo!()
    }

    fn associate_context(&mut self, _assoc: models::ContextAssoc) -> Result<models::ContextAssoc> {
        todo!()
    }

    fn multi_associate_context(&mut self, _assocs: Vec<&mut models::ContextAssoc>) -> Result<()> {
        todo!()
    }

    fn build(self) -> Result<Self> {
        Ok(self)
    }
}
