use std::path::PathBuf;

use serde_json::json;

use crate::{
    error::Result,
    report::{models, sqlite::Insertable, ReportBuilder, SqliteReport, SqliteReportBuilder},
};

pub fn build_sample_report(path: PathBuf) -> Result<SqliteReport> {
    let mut builder = SqliteReportBuilder::open(path)?;
    let file_1 = builder.insert_file("src/report/report.rs")?;
    let file_2 = builder.insert_file("src/report/models.rs")?;

    let upload_1 = models::RawUpload {
        id: 5,
        timestamp: Some(123),
        raw_upload_url: Some("upload 1 url".to_string()),
        flags: Some(json!(["flag on upload 1"])),
        provider: Some("provider upload 1".to_string()),
        build: Some("build upload 1".to_string()),
        name: Some("name upload 1".to_string()),
        job_name: Some("job name upload 1".to_string()),
        ci_run_url: Some("ci run url upload 1".to_string()),
        state: Some("state upload 1".to_string()),
        env: Some("env upload 1".to_string()),
        session_type: Some("type upload 1".to_string()),
        session_extras: Some(json!({"k1": "v1"})),
        ..Default::default()
    };
    // Insert directly, not through report builder, because we don't want a random
    // ID
    upload_1.insert(&builder.conn)?;

    let upload_2 = models::RawUpload {
        id: 10,
        timestamp: Some(456),
        raw_upload_url: Some("upload 2 url".to_string()),
        flags: Some(json!(["flag on upload 2"])),
        provider: Some("provider upload 2".to_string()),
        build: Some("build upload 2".to_string()),
        name: Some("name upload 2".to_string()),
        job_name: Some("job name upload 2".to_string()),
        ci_run_url: Some("ci run url upload 2".to_string()),
        state: Some("state upload 2".to_string()),
        env: Some("env upload 2".to_string()),
        session_type: Some("type upload 2".to_string()),
        session_extras: Some(json!({"k2": "v2"})),
        ..Default::default()
    };
    // Insert directly, not through report builder, because we don't want a random
    // ID
    upload_2.insert(&builder.conn)?;

    let line_1 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_1.id,
        line_no: 1,
        coverage_type: models::CoverageType::Line,
        hits: Some(3),
        ..Default::default()
    })?;

    let line_2 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_2.id,
        line_no: 1,
        coverage_type: models::CoverageType::Line,
        hits: Some(4),
        ..Default::default()
    })?;
    let _line_3 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_2.id,
        source_file_id: file_2.id,
        line_no: 3,
        coverage_type: models::CoverageType::Line,
        hits: Some(0),
        ..Default::default()
    })?;

    let branch_sample_1 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_1.id,
        line_no: 3,
        coverage_type: models::CoverageType::Branch,
        hit_branches: Some(2),
        total_branches: Some(2),
        ..Default::default()
    })?;
    let _ = builder.insert_branches_data(models::BranchesData {
        raw_upload_id: upload_1.id,
        source_file_id: branch_sample_1.source_file_id,
        local_sample_id: branch_sample_1.local_sample_id,
        hits: 1,
        branch_format: models::BranchFormat::Condition,
        branch: "0:jump".to_string(),
        ..Default::default()
    })?;
    let _ = builder.insert_branches_data(models::BranchesData {
        raw_upload_id: upload_1.id,
        source_file_id: branch_sample_1.source_file_id,
        local_sample_id: branch_sample_1.local_sample_id,
        hits: 1,
        branch_format: models::BranchFormat::Condition,
        branch: "1".to_string(),
        ..Default::default()
    })?;

    let branch_sample_2 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_2.id,
        line_no: 6,
        coverage_type: models::CoverageType::Branch,
        hit_branches: Some(2),
        total_branches: Some(4),
        ..Default::default()
    })?;
    let _ = builder.insert_branches_data(models::BranchesData {
        raw_upload_id: upload_1.id,
        source_file_id: branch_sample_2.source_file_id,
        local_sample_id: branch_sample_2.local_sample_id,
        hits: 1,
        branch_format: models::BranchFormat::Condition,
        branch: "0:jump".to_string(),
        ..Default::default()
    })?;
    let _ = builder.insert_branches_data(models::BranchesData {
        raw_upload_id: upload_1.id,
        source_file_id: branch_sample_2.source_file_id,
        local_sample_id: branch_sample_2.local_sample_id,
        hits: 1,
        branch_format: models::BranchFormat::Condition,
        branch: "1".to_string(),
        ..Default::default()
    })?;
    let _ = builder.insert_branches_data(models::BranchesData {
        raw_upload_id: upload_1.id,
        source_file_id: branch_sample_2.source_file_id,
        local_sample_id: branch_sample_2.local_sample_id,
        hits: 0,
        branch_format: models::BranchFormat::Condition,
        branch: "2".to_string(),
        ..Default::default()
    })?;
    let _ = builder.insert_branches_data(models::BranchesData {
        raw_upload_id: upload_1.id,
        source_file_id: branch_sample_2.source_file_id,
        local_sample_id: branch_sample_2.local_sample_id,
        hits: 0,
        branch_format: models::BranchFormat::Condition,
        branch: "3".to_string(),
        ..Default::default()
    })?;

    let method_sample_1 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_1.id,
        line_no: 2,
        coverage_type: models::CoverageType::Method,
        hits: Some(2),
        ..Default::default()
    })?;
    let _ = builder.insert_method_data(models::MethodData {
        raw_upload_id: upload_1.id,
        source_file_id: method_sample_1.source_file_id,
        local_sample_id: method_sample_1.local_sample_id,
        line_no: Some(method_sample_1.line_no),
        hit_branches: Some(2),
        total_branches: Some(4),
        hit_complexity_paths: Some(2),
        total_complexity: Some(4),
        ..Default::default()
    })?;

    let method_sample_2 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_2.id,
        line_no: 2,
        coverage_type: models::CoverageType::Method,
        hits: Some(5),
        ..Default::default()
    })?;
    let _ = builder.insert_method_data(models::MethodData {
        raw_upload_id: upload_1.id,
        source_file_id: method_sample_2.source_file_id,
        local_sample_id: method_sample_2.local_sample_id,
        line_no: Some(method_sample_2.line_no),
        hit_branches: Some(2),
        total_branches: Some(4),
        ..Default::default()
    })?;

    let method_sample_3 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_2.id,
        source_file_id: file_2.id,
        line_no: 5,
        coverage_type: models::CoverageType::Method,
        hits: Some(0),
        ..Default::default()
    })?;
    let _ = builder.insert_method_data(models::MethodData {
        raw_upload_id: upload_2.id,
        source_file_id: method_sample_3.source_file_id,
        local_sample_id: method_sample_3.local_sample_id,
        line_no: Some(method_sample_3.line_no),
        hit_complexity_paths: Some(2),
        total_complexity: Some(4),
        ..Default::default()
    })?;

    let line_with_partial_1 = builder.insert_coverage_sample(models::CoverageSample {
        raw_upload_id: upload_1.id,
        source_file_id: file_1.id,
        line_no: 8,
        coverage_type: models::CoverageType::Line,
        hits: Some(3),
        ..Default::default()
    })?;
    let _ = builder.insert_span_data(models::SpanData {
        raw_upload_id: upload_1.id,
        source_file_id: line_with_partial_1.source_file_id,
        local_sample_id: Some(line_with_partial_1.local_sample_id),
        start_line: Some(line_with_partial_1.line_no),
        start_col: Some(3),
        end_line: Some(line_with_partial_1.line_no),
        end_col: None,
        hits: 3,
        ..Default::default()
    })?;

    let label_1 = builder.insert_context("test-case")?;
    let _ = builder.associate_context(models::ContextAssoc {
        context_id: label_1.id,
        raw_upload_id: upload_1.id,
        local_sample_id: Some(line_1.local_sample_id),
        ..Default::default()
    })?;
    let _ = builder.associate_context(models::ContextAssoc {
        context_id: label_1.id,
        raw_upload_id: upload_1.id,
        local_sample_id: Some(line_2.local_sample_id),
        ..Default::default()
    })?;

    let label_2 = builder.insert_context("test-case 2")?;
    let _ = builder.associate_context(models::ContextAssoc {
        context_id: label_2.id,
        raw_upload_id: upload_1.id,
        local_sample_id: Some(line_1.local_sample_id),
        ..Default::default()
    })?;
    let _ = builder.associate_context(models::ContextAssoc {
        context_id: label_2.id,
        raw_upload_id: upload_1.id,
        local_sample_id: Some(line_2.local_sample_id),
        ..Default::default()
    })?;
    let _ = builder.associate_context(models::ContextAssoc {
        context_id: label_2.id,
        raw_upload_id: upload_1.id,
        local_sample_id: Some(method_sample_1.local_sample_id),
        ..Default::default()
    })?;

    builder.build()
}
