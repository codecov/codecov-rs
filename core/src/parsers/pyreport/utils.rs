use super::chunks::ParseCtx;
use crate::{
    error::Result,
    report::{
        models,
        pyreport::types::{
            Complexity, CoverageDatapoint, LineSession, MissingBranch, Partial, PyreportCoverage,
            ReportLine,
        },
        Report, ReportBuilder,
    },
};

fn separate_pyreport_complexity(complexity: &Complexity) -> (Option<i64>, Option<i64>) {
    let (covered, total) = match complexity {
        Complexity::PathsTaken { covered, total } => (Some(*covered as i64), Some(*total as i64)),
        Complexity::Total(total) => (None, Some(*total as i64)),
    };
    (covered, total)
}

fn separate_pyreport_coverage(
    coverage: &PyreportCoverage,
) -> (Option<i64>, Option<i64>, Option<i64>) {
    let (hits, hit_branches, total_branches) = match coverage {
        PyreportCoverage::HitCount(hits) => (Some(*hits as i64), None, None),
        PyreportCoverage::BranchesTaken { covered, total } => {
            (None, Some(*covered as i64), Some(*total as i64))
        }
        // `PyreportCoverage::Partial()` should already have been transformed in this way, but just
        // in case
        PyreportCoverage::Partial() => (None, Some(1), Some(2)),
    };
    (hits, hit_branches, total_branches)
}

fn format_pyreport_branch(branch: &MissingBranch) -> (models::BranchFormat, String) {
    let (branch_format, branch_serialized) = match branch {
        MissingBranch::BlockAndBranch(block, branch) => (
            models::BranchFormat::BlockAndBranch,
            format!("{}:{}", block, branch),
        ),
        MissingBranch::Condition(index, Some(cond_type)) => (
            models::BranchFormat::Condition,
            format!("{}:{}", index, cond_type),
        ),
        MissingBranch::Condition(index, None) => {
            (models::BranchFormat::Condition, index.to_string())
        }
        MissingBranch::Line(line_no) => (models::BranchFormat::Line, line_no.to_string()),
    };
    (branch_format, branch_serialized)
}

/// Each [`LineSession`] corresponds to a single [`models::CoverageSample`].
/// Each [`CoverageSample`](models::CoverageSample) _may_ (but won't always)
/// have:
/// - multiple related [`models::BranchesData`] records, one for each specific
///   branch path we have data for
/// - a single related [`models::MethodData`] if the `LineSession` is for a
///   method and we have extra method-specific data
/// - multiple related [`models::SpanData`] records, if we have data indicating
///   different subspans within line are/aren't covered (Chunks files only
///   record single-line spans, which it calls "partials")
/// - multiple related [`models::ContextAssoc`] records to link the line with
///   [`models::Context`]s which, for a chunks file, come from the labels index
#[derive(Default, Debug, PartialEq)]
struct LineSessionModels {
    sample: models::CoverageSample,
    branches: Vec<models::BranchesData>,
    method: Option<models::MethodData>,
    partials: Vec<models::SpanData>,
    assocs: Vec<models::ContextAssoc>,
}

fn create_model_sets_for_line_session<R: Report, B: ReportBuilder<R>>(
    line_session: &LineSession,
    coverage_type: &models::CoverageType,
    line_no: i64,
    datapoint: Option<&CoverageDatapoint>,
    ctx: &mut ParseCtx<R, B>,
) -> LineSessionModels {
    let source_file_id = ctx.report_json_files[&ctx.chunk.index];
    let (hits, hit_branches, total_branches) = separate_pyreport_coverage(&line_session.coverage);
    let raw_upload_id = ctx.report_json_sessions[&line_session.session_id];

    // Each `LineSession` definitely gets a `CoverageSample`
    let sample = models::CoverageSample {
        source_file_id,
        raw_upload_id,
        line_no,
        coverage_type: *coverage_type,
        hits,
        hit_branches,
        total_branches,
        ..Default::default()
    };

    // Read the labels index to populate `assocs`
    let assocs: Vec<_> = datapoint
        .map_or(&vec![], |datapoint| &datapoint.labels)
        .iter()
        .map(|label| {
            let label_context_id = ctx.labels_index[label];
            models::ContextAssoc {
                context_id: label_context_id,
                raw_upload_id,
                ..Default::default()
            }
        })
        .collect();

    // Create `BranchesData` models, if there are any
    let branches = match &line_session.branches {
        Some(Some(missing_branches)) => missing_branches
            .iter()
            .map(|branch| {
                let (branch_format, branch_serialized) = format_pyreport_branch(branch);
                models::BranchesData {
                    source_file_id,
                    raw_upload_id,
                    hits: 0,
                    branch_format,
                    branch: branch_serialized,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>(),
        _ => vec![],
    };

    // Create a `MethodData` model, if we have data for it
    let method = match &line_session.complexity {
        Some(Some(complexity)) => {
            let (covered, total) = separate_pyreport_complexity(complexity);
            Some(models::MethodData {
                source_file_id,
                raw_upload_id,
                line_no: Some(line_no),
                hit_complexity_paths: covered,
                total_complexity: total,
                ..Default::default()
            })
        }
        _ => None,
    };

    // Create `SpanData` models, if we have data for single-line spans
    let partials = match &line_session.partials {
        Some(Some(partials)) => partials
            .iter()
            .map(
                |Partial {
                     start_col,
                     end_col,
                     coverage,
                 }| {
                    let hits = match coverage {
                        PyreportCoverage::HitCount(hits) => *hits as i64,
                        _ => 0,
                    };
                    models::SpanData {
                        source_file_id,
                        raw_upload_id,
                        hits,
                        start_line: Some(line_no),
                        start_col: start_col.map(|x| x as i64),
                        end_line: Some(line_no),
                        end_col: end_col.map(|x| x as i64),
                        ..Default::default()
                    }
                },
            )
            .collect::<Vec<_>>(),
        _ => vec![],
    };

    LineSessionModels {
        sample,
        branches,
        method,
        partials,
        assocs,
    }
}

fn create_model_sets_for_report_line<R: Report, B: ReportBuilder<R>>(
    report_line: &ReportLine,
    ctx: &mut ParseCtx<R, B>,
) -> Vec<LineSessionModels> {
    // A `ReportLine` is a collection of `LineSession`s, and each `LineSession` has
    // a set of models we need to insert for it. Build a list of those sets of
    // models.
    let mut line_session_models = vec![];
    for line_session in &report_line.sessions {
        // Datapoints are effectively `LineSession`-scoped, but they don't actually live
        // in the `LineSession`. Get the `CoverageDatapoint` for this
        // `LineSession` if there is one.
        let datapoint = if let Some(Some(datapoints)) = &report_line.datapoints {
            datapoints.get(&(line_session.session_id as u32))
        } else {
            None
        };
        line_session_models.push(create_model_sets_for_line_session(
            line_session,
            &report_line.coverage_type,
            report_line.line_no,
            datapoint,
            ctx,
        ));
    }
    line_session_models
}

/// Each [`ReportLine`] from a chunks file is comprised of a number of
/// [`LineSession`]s, and each [`LineSession`] corresponds to a number of
/// related models in our schema ([`LineSessionModels`]). This function builds
/// all of the models for a collection of [`ReportLine`]s and batch-inserts
/// them.
pub fn save_report_lines<R: Report, B: ReportBuilder<R>>(
    report_lines: &[ReportLine],
    ctx: &mut ParseCtx<R, B>,
) -> Result<()> {
    // Build a flat list of `LineSessionModels` structs for us to insert
    let mut models: Vec<LineSessionModels> = report_lines
        .iter()
        .flat_map(|line| create_model_sets_for_report_line(line, ctx))
        .collect::<Vec<LineSessionModels>>();

    // First, insert all of the `CoverageSample`s. Each of them will have an ID
    // assigned as a side-effect of this insertion. That lets us populate the
    // `local_sample_id` foreign key on all of the models associated with each
    // `CoverageSample`.
    ctx.db.report_builder.multi_insert_coverage_sample(
        models
            .iter_mut()
            .map(|LineSessionModels { sample, .. }| sample)
            .collect(),
    )?;

    // Populate `local_sample_id` and insert all of the context assocs for each
    // `LineSession` (if there are any)
    ctx.db.report_builder.multi_associate_context(
        models
            .iter_mut()
            .flat_map(|LineSessionModels { sample, assocs, .. }| {
                for assoc in assocs.iter_mut() {
                    assoc.local_sample_id = Some(sample.local_sample_id);
                }
                assocs
            })
            .collect(),
    )?;

    // Populate `local_sample_id` and insert all of the `BranchesData` records for
    // each `LineSession` (if there are any)
    ctx.db.report_builder.multi_insert_branches_data(
        models
            .iter_mut()
            .flat_map(
                |LineSessionModels {
                     sample, branches, ..
                 }| {
                    for branch in branches.iter_mut() {
                        branch.local_sample_id = sample.local_sample_id;
                    }
                    branches
                },
            )
            .collect(),
    )?;

    // Populate `local_sample_id` and insert the single `MethodData` record for each
    // `LineSession` (if there is one)
    ctx.db.report_builder.multi_insert_method_data(
        models
            .iter_mut()
            .filter_map(|LineSessionModels { sample, method, .. }| {
                // See https://github.com/rust-lang/rust-clippy/issues/13185
                #[allow(clippy::manual_inspect)]
                method.as_mut().map(|method| {
                    method.local_sample_id = sample.local_sample_id;
                    method
                })
            })
            .collect(),
    )?;

    // Populate `local_sample_id` and insert all of the `SpanData` records for each
    // `LineSession` (if there are any). In a chunks file, only spans that are
    // subsets of a single line are recorded.
    ctx.db.report_builder.multi_insert_span_data(
        models
            .iter_mut()
            .flat_map(
                |LineSessionModels {
                     sample, partials, ..
                 }| {
                    for span in partials.iter_mut() {
                        span.local_sample_id = Some(sample.local_sample_id);
                    }
                    partials
                },
            )
            .collect(),
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::test_utils::test_report::{TestReport, TestReportBuilder};

    struct Ctx {
        parse_ctx: ParseCtx<TestReport, TestReportBuilder>,
    }

    fn setup() -> Ctx {
        let report_builder = TestReportBuilder::default();
        let report_json_files = HashMap::from([(0, 123), (1, 456), (2, 789)]);
        let report_json_sessions = HashMap::from([(0, 123), (1, 456), (2, 789)]);

        let parse_ctx = ParseCtx::new(report_builder, report_json_files, report_json_sessions);

        Ctx { parse_ctx }
    }

    #[test]
    fn test_format_pyreport_branch() {
        let input = MissingBranch::BlockAndBranch(0, 1);
        let expected = (models::BranchFormat::BlockAndBranch, "0:1".to_string());
        assert_eq!(format_pyreport_branch(&input), expected);

        let input = MissingBranch::Condition(0, Some("jump".to_string()));
        let expected = (models::BranchFormat::Condition, "0:jump".to_string());
        assert_eq!(format_pyreport_branch(&input), expected);

        let input = MissingBranch::Condition(3, None);
        let expected = (models::BranchFormat::Condition, "3".to_string());
        assert_eq!(format_pyreport_branch(&input), expected);

        let input = MissingBranch::Line(13);
        let expected = (models::BranchFormat::Line, "13".to_string());
        assert_eq!(format_pyreport_branch(&input), expected);
    }

    #[test]
    fn test_separate_pyreport_complexity() {
        let input = Complexity::Total(13);
        let expected = (None, Some(13));
        assert_eq!(separate_pyreport_complexity(&input), expected);

        let input = Complexity::PathsTaken {
            covered: 3,
            total: 4,
        };
        let expected = (Some(3), Some(4));
        assert_eq!(separate_pyreport_complexity(&input), expected);
    }

    #[test]
    fn test_separate_pyreport_coverage() {
        let input = PyreportCoverage::HitCount(31);
        let expected = (Some(31), None, None);
        assert_eq!(separate_pyreport_coverage(&input), expected);

        let input = PyreportCoverage::BranchesTaken {
            covered: 3,
            total: 4,
        };
        let expected = (None, Some(3), Some(4));
        assert_eq!(separate_pyreport_coverage(&input), expected);

        let input = PyreportCoverage::Partial();
        let expected = (None, Some(1), Some(2));
        assert_eq!(separate_pyreport_coverage(&input), expected);
    }

    #[test]
    fn test_create_model_sets_for_line_session_simple_line() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.index = 0;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::HitCount(4),
            branches: None,
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Line;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hits: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_simple_line_with_datapoint() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.index = 0;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::HitCount(4),
            branches: None,
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Line;

        parse_ctx.labels_index = HashMap::from([
            ("test_label".to_string(), 50),
            ("test_label_2".to_string(), 51),
        ]);

        let datapoint = CoverageDatapoint {
            session_id: 0,
            _coverage: PyreportCoverage::HitCount(4),
            _coverage_type: None,
            labels: vec!["test_label".to_string(), "test_label_2".to_string()],
        };

        let line_session_models = create_model_sets_for_line_session(
            &input_session,
            &input_type,
            5,
            Some(&datapoint),
            parse_ctx,
        );

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hits: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                assocs: vec![
                    models::ContextAssoc {
                        raw_upload_id: 123,
                        context_id: 50,
                        ..Default::default()
                    },
                    models::ContextAssoc {
                        raw_upload_id: 123,
                        context_id: 51,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_line_with_partials() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::HitCount(4),
            branches: None,
            partials: Some(Some(vec![
                Partial {
                    start_col: None,
                    end_col: Some(10),
                    coverage: PyreportCoverage::HitCount(1),
                },
                Partial {
                    start_col: Some(15),
                    end_col: Some(20),
                    coverage: PyreportCoverage::HitCount(0),
                },
                Partial {
                    start_col: Some(25),
                    end_col: None,
                    coverage: PyreportCoverage::HitCount(1),
                },
            ])),
            complexity: None,
        };
        let input_type = models::CoverageType::Line;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    hits: Some(4),
                    coverage_type: input_type,
                    line_no: 5,
                    ..Default::default()
                },
                partials: vec![
                    models::SpanData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 1,
                        start_line: Some(5),
                        end_line: Some(5),
                        end_col: Some(10),
                        ..Default::default()
                    },
                    models::SpanData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        start_line: Some(5),
                        end_line: Some(5),
                        start_col: Some(15),
                        end_col: Some(20),
                        ..Default::default()
                    },
                    models::SpanData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 1,
                        start_line: Some(5),
                        end_line: Some(5),
                        start_col: Some(25),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_simple_method() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::HitCount(4),
            branches: None,
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Method;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hits: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_method_with_total_complexity() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::HitCount(4),
            branches: None,
            partials: None,
            complexity: Some(Some(Complexity::Total(13))),
        };
        let input_type = models::CoverageType::Method;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hits: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                method: Some(models::MethodData {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: Some(5),
                    total_complexity: Some(13),
                    ..Default::default()
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_method_with_split_complexity() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::HitCount(4),
            branches: None,
            partials: None,
            complexity: Some(Some(Complexity::PathsTaken {
                covered: 3,
                total: 4,
            })),
        };
        let input_type = models::CoverageType::Method;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hits: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                method: Some(models::MethodData {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: Some(5),
                    hit_complexity_paths: Some(3),
                    total_complexity: Some(4),
                    ..Default::default()
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_simple_branch() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::BranchesTaken {
                covered: 2,
                total: 4,
            },
            branches: None,
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Branch;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hit_branches: Some(2),
                    total_branches: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_branch_with_missing_branches_block_and_branch() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::BranchesTaken {
                covered: 2,
                total: 4,
            },
            branches: Some(Some(vec![
                MissingBranch::BlockAndBranch(0, 0),
                MissingBranch::BlockAndBranch(0, 1),
            ])),
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Branch;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hit_branches: Some(2),
                    total_branches: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                branches: vec![
                    models::BranchesData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        branch_format: models::BranchFormat::BlockAndBranch,
                        branch: "0:0".to_string(),
                        ..Default::default()
                    },
                    models::BranchesData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        branch_format: models::BranchFormat::BlockAndBranch,
                        branch: "0:1".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_branch_with_missing_branches_condition() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::BranchesTaken {
                covered: 2,
                total: 4,
            },
            branches: Some(Some(vec![
                MissingBranch::Condition(0, Some("jump".to_string())),
                MissingBranch::Condition(1, None),
            ])),
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Branch;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 5,
                    hit_branches: Some(2),
                    total_branches: Some(4),
                    coverage_type: input_type,
                    ..Default::default()
                },
                branches: vec![
                    models::BranchesData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        branch_format: models::BranchFormat::Condition,
                        branch: "0:jump".to_string(),
                        ..Default::default()
                    },
                    models::BranchesData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        branch_format: models::BranchFormat::Condition,
                        branch: "1".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_line_session_branch_with_missing_branches_line() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;

        let input_session = LineSession {
            session_id: 0,
            coverage: PyreportCoverage::BranchesTaken {
                covered: 2,
                total: 4,
            },
            branches: Some(Some(vec![MissingBranch::Line(26), MissingBranch::Line(27)])),
            partials: None,
            complexity: None,
        };
        let input_type = models::CoverageType::Branch;

        let line_session_models =
            create_model_sets_for_line_session(&input_session, &input_type, 5, None, parse_ctx);

        assert_eq!(
            line_session_models,
            LineSessionModels {
                sample: models::CoverageSample {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    hit_branches: Some(2),
                    total_branches: Some(4),
                    line_no: 5,
                    coverage_type: input_type,
                    ..Default::default()
                },
                branches: vec![
                    models::BranchesData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        branch_format: models::BranchFormat::Line,
                        branch: "26".to_string(),
                        ..Default::default()
                    },
                    models::BranchesData {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        hits: 0,
                        branch_format: models::BranchFormat::Line,
                        branch: "27".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_create_model_sets_for_report_line_line_no_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        parse_ctx.chunk.index = 0;
        let coverage_type = models::CoverageType::Line;
        let coverage = PyreportCoverage::HitCount(10);

        let sessions: Vec<_> = [0, 1, 2]
            .iter()
            .map(|i| LineSession {
                session_id: *i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: None,
            })
            .collect();

        let report_line = ReportLine {
            line_no: 1,
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: None,
            datapoints: None,
        };

        let model_sets = create_model_sets_for_report_line(&report_line, parse_ctx);
        assert_eq!(
            model_sets,
            vec![
                LineSessionModels {
                    sample: models::CoverageSample {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        line_no: 1,
                        hits: Some(10),
                        coverage_type,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                LineSessionModels {
                    sample: models::CoverageSample {
                        raw_upload_id: 456,
                        source_file_id: 123,
                        line_no: 1,
                        hits: Some(10),
                        coverage_type,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                LineSessionModels {
                    sample: models::CoverageSample {
                        raw_upload_id: 789,
                        source_file_id: 123,
                        line_no: 1,
                        hits: Some(10),
                        coverage_type,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_create_model_sets_for_report_line_line_with_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        parse_ctx.chunk.index = 0;
        let coverage_type = models::CoverageType::Line;
        let coverage = PyreportCoverage::HitCount(10);

        parse_ctx.labels_index = HashMap::from([
            ("test_label".to_string(), 50),
            ("test_label_2".to_string(), 51),
        ]);

        let sessions: Vec<_> = [0, 1, 2]
            .iter()
            .map(|i| LineSession {
                session_id: *i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: None,
            })
            .collect();

        let datapoints: HashMap<u32, CoverageDatapoint> = HashMap::from([
            (
                0,
                CoverageDatapoint {
                    session_id: 0,
                    _coverage: coverage.clone(),
                    _coverage_type: Some(coverage_type),
                    labels: vec!["test_label".to_string(), "test_label_2".to_string()],
                },
            ),
            (
                2,
                CoverageDatapoint {
                    session_id: 2,
                    _coverage: coverage.clone(),
                    _coverage_type: Some(coverage_type),
                    labels: vec!["test_label_2".to_string()],
                },
            ),
        ]);

        let report_line = ReportLine {
            line_no: 1,
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: None,
            datapoints: Some(Some(datapoints)),
        };

        let model_sets = create_model_sets_for_report_line(&report_line, parse_ctx);
        assert_eq!(
            model_sets,
            vec![
                LineSessionModels {
                    sample: models::CoverageSample {
                        raw_upload_id: 123,
                        source_file_id: 123,
                        line_no: 1,
                        hits: Some(10),
                        coverage_type,
                        ..Default::default()
                    },
                    assocs: vec![
                        models::ContextAssoc {
                            context_id: 50,
                            raw_upload_id: 123,
                            ..Default::default()
                        },
                        models::ContextAssoc {
                            context_id: 51,
                            raw_upload_id: 123,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                LineSessionModels {
                    sample: models::CoverageSample {
                        raw_upload_id: 456,
                        source_file_id: 123,
                        line_no: 1,
                        hits: Some(10),
                        coverage_type,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                LineSessionModels {
                    sample: models::CoverageSample {
                        raw_upload_id: 789,
                        source_file_id: 123,
                        line_no: 1,
                        hits: Some(10),
                        coverage_type,
                        ..Default::default()
                    },
                    assocs: vec![models::ContextAssoc {
                        context_id: 51,
                        raw_upload_id: 789,
                        ..Default::default()
                    },],
                    ..Default::default()
                },
            ]
        );
    }

    #[test]
    fn test_save_report_lines() {
        let mut test_ctx = setup();
        test_ctx.parse_ctx.labels_index = HashMap::from([
            ("test_label".to_string(), 50),
            ("test_label_2".to_string(), 51),
        ]);
        test_ctx.parse_ctx.chunk.current_line = 1;
        test_ctx.parse_ctx.chunk.index = 0;

        // Sample input: 1 line (2 sessions), 1 branch (1 session), 1 method (1 session)
        // BranchesData, SpanData, MethodData, and ContextAssoc will all get inserted
        let report_lines = vec![
            // ReportLine 1: a line with 2 sessions, 1 datapoint, 1 label
            ReportLine {
                line_no: 1,
                coverage: PyreportCoverage::HitCount(10),
                coverage_type: models::CoverageType::Line,
                sessions: vec![
                    LineSession {
                        session_id: 0,
                        coverage: PyreportCoverage::HitCount(10),
                        branches: None,
                        partials: None,
                        complexity: None,
                    },
                    LineSession {
                        session_id: 1,
                        coverage: PyreportCoverage::HitCount(10),
                        branches: None,
                        partials: Some(Some(vec![Partial {
                            start_col: None,
                            end_col: Some(10),
                            coverage: PyreportCoverage::HitCount(3),
                        }])),
                        complexity: None,
                    },
                ],
                _messages: None,
                _complexity: None,
                datapoints: Some(Some(HashMap::from([(
                    0,
                    CoverageDatapoint {
                        session_id: 0,
                        _coverage: PyreportCoverage::HitCount(10),
                        _coverage_type: None,
                        labels: vec!["test_label".to_string()],
                    },
                )]))),
            },
            // ReportLine 2: a branch with 1 session, 2 BranchesData rows, 1 datapoint, 1 label
            ReportLine {
                line_no: 2,
                coverage: PyreportCoverage::BranchesTaken {
                    covered: 2,
                    total: 4,
                },
                coverage_type: models::CoverageType::Branch,
                sessions: vec![LineSession {
                    session_id: 0,
                    coverage: PyreportCoverage::BranchesTaken {
                        covered: 2,
                        total: 4,
                    },
                    branches: Some(Some(vec![
                        MissingBranch::BlockAndBranch(0, 0),
                        MissingBranch::BlockAndBranch(0, 1),
                    ])),
                    partials: None,
                    complexity: None,
                }],
                _messages: None,
                _complexity: None,
                datapoints: Some(Some(HashMap::from([(
                    0,
                    CoverageDatapoint {
                        session_id: 0,
                        _coverage: PyreportCoverage::BranchesTaken {
                            covered: 2,
                            total: 4,
                        },
                        _coverage_type: None,
                        labels: vec!["test_label".to_string()],
                    },
                )]))),
            },
            // ReportLine 3: a method with complexity, 1 session, 1 datapoint, 1 label
            ReportLine {
                line_no: 3,
                coverage: PyreportCoverage::HitCount(3),
                coverage_type: models::CoverageType::Method,
                sessions: vec![LineSession {
                    session_id: 2,
                    coverage: PyreportCoverage::HitCount(3),
                    branches: None,
                    partials: None,
                    complexity: Some(Some(Complexity::Total(4))),
                }],
                _messages: None,
                _complexity: None,
                datapoints: Some(Some(HashMap::from([(
                    2,
                    CoverageDatapoint {
                        session_id: 2,
                        _coverage: PyreportCoverage::HitCount(3),
                        _coverage_type: None,
                        labels: vec!["test_label_2".to_string()],
                    },
                )]))),
            },
        ];

        // Now we actually run the function
        save_report_lines(&report_lines, &mut test_ctx.parse_ctx).unwrap();
        let report = test_ctx.parse_ctx.db.report_builder.build().unwrap();

        // Now we need to set up our mock expectations. There are a lot of them.
        // First thing that gets inserted is CoverageSample. We expect 4 of them,
        // one for each LineSession. Our first ReportLine has 2 sessions, and the
        // other two have 1 session each, so 4 total.
        assert_eq!(
            report.samples,
            &[
                models::CoverageSample {
                    local_sample_id: 0,
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 1,
                    coverage_type: models::CoverageType::Line,
                    hits: Some(10),
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 1,
                    raw_upload_id: 456,
                    source_file_id: 123,
                    line_no: 1,
                    coverage_type: models::CoverageType::Line,
                    hits: Some(10),
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 2,
                    raw_upload_id: 123,
                    source_file_id: 123,
                    line_no: 2,
                    coverage_type: models::CoverageType::Branch,
                    hit_branches: Some(2),
                    total_branches: Some(4),
                    ..Default::default()
                },
                models::CoverageSample {
                    local_sample_id: 3,
                    raw_upload_id: 789,
                    source_file_id: 123,
                    line_no: 3,
                    coverage_type: models::CoverageType::Method,
                    hits: Some(3),
                    ..Default::default()
                },
            ]
        );

        // Next thing to go is ContextAssoc. Only 3 LineSessions have a corresponding
        // CoverageDatapoint, and each CoverageDatapoint only has one label.
        // "test_label" is context_id==50 and "test_label_2" is context_id==51
        assert_eq!(
            report.assocs,
            &[
                models::ContextAssoc {
                    raw_upload_id: 123,
                    local_sample_id: Some(0),
                    context_id: 50,
                    ..Default::default()
                },
                models::ContextAssoc {
                    raw_upload_id: 123,
                    local_sample_id: Some(2),
                    context_id: 50,
                    ..Default::default()
                },
                models::ContextAssoc {
                    raw_upload_id: 789,
                    local_sample_id: Some(3),
                    context_id: 51,
                    ..Default::default()
                },
            ]
        );

        // Then we do BranchesData. Our branch ReportLine has a single LineSession, and
        // that LineSession has a `branches` field with two missing branches in
        // it.
        assert_eq!(
            report.branches,
            &[
                models::BranchesData {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    local_sample_id: 2,
                    hits: 0,
                    branch: "0:0".to_string(),
                    branch_format: models::BranchFormat::BlockAndBranch,
                    ..Default::default()
                },
                models::BranchesData {
                    raw_upload_id: 123,
                    source_file_id: 123,
                    local_sample_id: 2,
                    hits: 0,
                    branch: "0:1".to_string(),
                    branch_format: models::BranchFormat::BlockAndBranch,
                    ..Default::default()
                },
            ]
        );

        // Then we do MethodData. Our method ReportLine has a single session, and that
        // single session has its complexity field filled in.
        assert_eq!(
            report.methods,
            &[models::MethodData {
                raw_upload_id: 789,
                source_file_id: 123,
                local_sample_id: 3,
                line_no: Some(3),
                total_complexity: Some(4),
                ..Default::default()
            }]
        );

        // Then we do SpanData. Our first ReportLine has two sessions, one without any
        // partials and one with a single partial. So, we need to create a
        // single SpanData for that partial.
        assert_eq!(
            report.spans,
            &[models::SpanData {
                raw_upload_id: 456,
                source_file_id: 123,
                local_sample_id: Some(1),
                hits: 3,
                start_line: Some(1),
                end_line: Some(1),
                end_col: Some(10),
                ..Default::default()
            }]
        );
    }
}
