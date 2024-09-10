use crate::{
    error,
    report::{
        models::{
            BranchesData, Context, ContextAssoc, CoverageSample, MethodData, RawUpload,
            ReportTotals, SourceFile, SpanData,
        },
        Report, ReportBuilder,
    },
};

#[derive(Default)]
pub struct TestReport {
    pub files: Vec<SourceFile>,
    pub uploads: Vec<RawUpload>,
    pub contexts: Vec<Context>,
    pub samples: Vec<CoverageSample>,
    pub assocs: Vec<ContextAssoc>,
    pub branches: Vec<BranchesData>,
    pub methods: Vec<MethodData>,
    pub spans: Vec<SpanData>,
}

#[derive(Default)]
pub struct TestReportBuilder {
    pub report: TestReport,
}

impl Report for TestReport {
    fn list_files(&self) -> error::Result<Vec<SourceFile>> {
        todo!()
    }

    fn list_contexts(&self) -> error::Result<Vec<Context>> {
        todo!()
    }

    fn list_coverage_samples(&self) -> error::Result<Vec<CoverageSample>> {
        todo!()
    }

    fn list_branches_for_sample(
        &self,
        _sample: &CoverageSample,
    ) -> error::Result<Vec<BranchesData>> {
        todo!()
    }

    fn get_method_for_sample(&self, _sample: &CoverageSample) -> error::Result<Option<MethodData>> {
        todo!()
    }

    fn list_spans_for_sample(&self, _sample: &CoverageSample) -> error::Result<Vec<SpanData>> {
        todo!()
    }

    fn list_contexts_for_sample(&self, _sample: &CoverageSample) -> error::Result<Vec<Context>> {
        todo!()
    }

    fn list_samples_for_file(&self, _file: &SourceFile) -> error::Result<Vec<CoverageSample>> {
        todo!()
    }

    fn list_raw_uploads(&self) -> error::Result<Vec<RawUpload>> {
        todo!()
    }

    fn merge(&mut self, _other: &Self) -> error::Result<()> {
        todo!()
    }

    fn totals(&self) -> error::Result<ReportTotals> {
        todo!()
    }
}

impl ReportBuilder<TestReport> for TestReportBuilder {
    fn insert_file(&mut self, path: &str) -> error::Result<SourceFile> {
        let file = SourceFile::new(path);
        self.report.files.push(file.clone());
        Ok(file)
    }

    fn insert_context(&mut self, name: &str) -> error::Result<Context> {
        let context = Context::new(name);
        self.report.contexts.push(context.clone());
        Ok(context)
    }

    fn insert_coverage_sample(&mut self, sample: CoverageSample) -> error::Result<CoverageSample> {
        self.report.samples.push(sample.clone());
        Ok(sample)
    }

    fn multi_insert_coverage_sample(
        &mut self,
        samples: Vec<&mut CoverageSample>,
    ) -> error::Result<()> {
        self.report
            .samples
            .extend(samples.into_iter().enumerate().map(|(i, m)| {
                m.local_sample_id = i as i64;
                m.clone()
            }));
        Ok(())
    }

    fn insert_branches_data(&mut self, branch: BranchesData) -> error::Result<BranchesData> {
        self.report.branches.push(branch.clone());
        Ok(branch)
    }

    fn multi_insert_branches_data(
        &mut self,
        branches: Vec<&mut BranchesData>,
    ) -> error::Result<()> {
        self.report
            .branches
            .extend(branches.into_iter().map(|m| m.clone()));
        Ok(())
    }

    fn insert_method_data(&mut self, method: MethodData) -> error::Result<MethodData> {
        self.report.methods.push(method.clone());
        Ok(method)
    }

    fn multi_insert_method_data(&mut self, methods: Vec<&mut MethodData>) -> error::Result<()> {
        self.report
            .methods
            .extend(methods.into_iter().map(|m| m.clone()));
        Ok(())
    }

    fn insert_span_data(&mut self, span: SpanData) -> error::Result<SpanData> {
        self.report.spans.push(span.clone());
        Ok(span)
    }

    fn multi_insert_span_data(&mut self, spans: Vec<&mut SpanData>) -> error::Result<()> {
        self.report
            .spans
            .extend(spans.into_iter().map(|s| s.clone()));
        Ok(())
    }

    fn associate_context(&mut self, assoc: ContextAssoc) -> error::Result<ContextAssoc> {
        self.report.assocs.push(assoc.clone());
        Ok(assoc)
    }

    fn multi_associate_context(&mut self, assocs: Vec<&mut ContextAssoc>) -> error::Result<()> {
        self.report
            .assocs
            .extend(assocs.into_iter().map(|m| m.clone()));
        Ok(())
    }

    fn insert_raw_upload(&mut self, mut upload_details: RawUpload) -> error::Result<RawUpload> {
        upload_details.id = self.report.uploads.len() as i64;
        self.report.uploads.push(upload_details.clone());
        Ok(upload_details)
    }

    fn build(self) -> error::Result<TestReport> {
        Ok(self.report)
    }
}
