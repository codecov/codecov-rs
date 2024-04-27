use super::{
    Complexity, LineSession, MissingBranch, ParseCtx, Partial, PyreportCoverage, ReportLine,
};
use crate::{
    error::Result,
    report::{models, Report, ReportBuilder},
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

/// Each [`LineSession`] corresponds to one
/// [`crate::report::models::CoverageSample`]. It also sometimes contains data
/// that belongs in [`crate::report::models::BranchesData`],
/// [`crate::report::models::MethodData`], or
/// [`crate::report::models::SpanData`]. This function writes all of that data
/// to the output as well as associations with
/// [`crate::report::models::Context`]s.
fn save_line_session<R: Report, B: ReportBuilder<R>>(
    line_session: &LineSession,
    coverage_type: &models::CoverageType,
    ctx: &mut ParseCtx<R, B>,
) -> Result<models::CoverageSample> {
    let file_id = ctx.report_json_files[&ctx.chunk.index];

    // The chunks file crams three of our model fields into the same "coverage"
    // field. We have to separate them.
    let (hits, hit_branches, total_branches) = separate_pyreport_coverage(&line_session.coverage);

    // Insert the meat of the `LineSession` and get back a `CoverageSample`.
    let coverage_sample = ctx
        .db
        .report_builder
        .insert_coverage_sample(models::CoverageSample {
            source_file_id: file_id,
            line_no: ctx.chunk.current_line,
            coverage_type: *coverage_type,
            hits,
            hit_branches,
            total_branches,
            ..Default::default()
        })?;

    // We already created a `Context` for each session when processing the report
    // JSON. Get the `Context` ID for the current session and then associate it with
    // our new `CoverageSample`.
    let session_id = ctx.report_json_sessions[&line_session.session_id];
    let _ = ctx
        .db
        .report_builder
        .associate_context(models::ContextAssoc {
            context_id: session_id,
            sample_id: Some(coverage_sample.id),
            ..Default::default()
        })?;

    // Check for and insert any additional branches data that we have.
    if let Some(Some(missing_branches)) = &line_session.branches {
        for branch in missing_branches {
            let (branch_format, branch_serialized) = format_pyreport_branch(branch);
            let _ = ctx
                .db
                .report_builder
                .insert_branches_data(models::BranchesData {
                    source_file_id: file_id,
                    sample_id: coverage_sample.id,
                    hits: 0, // Chunks file only records missing branches
                    branch_format,
                    branch: branch_serialized,
                    ..Default::default()
                })?;
        }
    }

    // Check for and insert any additional method data we have.
    if let Some(Some(complexity)) = &line_session.complexity {
        let (covered, total) = separate_pyreport_complexity(complexity);
        let _ = ctx
            .db
            .report_builder
            .insert_method_data(models::MethodData {
                source_file_id: file_id,
                sample_id: Some(coverage_sample.id),
                line_no: Some(ctx.chunk.current_line),
                hit_complexity_paths: covered,
                total_complexity: total,
                ..Default::default()
            })?;
    }

    // Check for and insert any additional span data we have.
    if let Some(Some(partials)) = &line_session.partials {
        for Partial {
            start_col,
            end_col,
            coverage,
        } in partials
        {
            let hits = match coverage {
                PyreportCoverage::HitCount(hits) => *hits as i64,
                _ => 0,
            };
            ctx.db.report_builder.insert_span_data(models::SpanData {
                source_file_id: file_id,
                sample_id: Some(coverage_sample.id),
                hits,
                start_line: Some(ctx.chunk.current_line),
                start_col: start_col.map(|x| x as i64),
                end_line: Some(ctx.chunk.current_line),
                end_col: end_col.map(|x| x as i64),
                ..Default::default()
            })?;
        }
    }

    Ok(coverage_sample)
}

/// Parsing a chunks file is a separate matter from coercing chunks data into
/// our schema. This function encapsulates most of that logic. It does not
/// include populating `ctx.labels_index`.
pub fn save_report_line<R: Report, B: ReportBuilder<R>>(
    report_line: &ReportLine,
    ctx: &mut ParseCtx<R, B>,
) -> Result<()> {
    // Most of the data we save is at the `LineSession` level
    for line_session in &report_line.sessions {
        let coverage_sample = save_line_session(line_session, &report_line.coverage_type, ctx)?;

        // If we have datapoints, and one of those datapoints is for this `LineSession`,
        // get its `Context` ID and associate it with our new `CoverageSample`.
        if let Some(Some(datapoints)) = &report_line.datapoints {
            if let Some(datapoint) = datapoints.get(&(line_session.session_id as u32)) {
                for label in &datapoint.labels {
                    let context_id = ctx.labels_index[label];
                    let _ = ctx
                        .db
                        .report_builder
                        .associate_context(models::ContextAssoc {
                            context_id,
                            sample_id: Some(coverage_sample.id),
                            ..Default::default()
                        })?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mockall::predicate::*;

    use super::{super::CoverageDatapoint, *};
    use crate::report::{MockReport, MockReportBuilder};

    struct Ctx {
        parse_ctx: ParseCtx<MockReport, MockReportBuilder<MockReport>>,
        sequence: mockall::Sequence,
    }

    fn setup() -> Ctx {
        let report_builder = MockReportBuilder::new();
        let report_json_files = HashMap::from([(0, 0), (1, 1), (2, 2)]);
        let report_json_sessions = HashMap::from([(0, 0), (1, 1), (2, 2)]);

        let parse_ctx = ParseCtx::new(report_builder, report_json_files, report_json_sessions);

        Ctx {
            parse_ctx,
            sequence: mockall::Sequence::new(),
        }
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

    // This test template function relies on `separate_pyreport_coverage`,
    // `separate_pyreport_complexity`, and `format_pyreport_branch` being tested and
    // correct. It uses them to set up the appropriate expectations for any
    // `LineSession` you pass in.
    fn set_up_line_session_expectations(
        line_session: &LineSession,
        coverage_type: models::CoverageType,
        parse_ctx: &mut ParseCtx<MockReport, MockReportBuilder<MockReport>>,
        sequence: &mut mockall::Sequence,
    ) -> models::CoverageSample {
        let session_context_id = parse_ctx.report_json_sessions[&line_session.session_id];
        let source_file_id = parse_ctx.report_json_files[&parse_ctx.chunk.index];

        let (hits, hit_branches, total_branches) =
            separate_pyreport_coverage(&line_session.coverage);

        let inserted_coverage_sample = models::CoverageSample {
            id: uuid::Uuid::new_v4(),
            source_file_id,
            line_no: parse_ctx.chunk.current_line,
            coverage_type,
            hits,
            hit_branches,
            total_branches,
        };
        parse_ctx
            .db
            .report_builder
            .expect_insert_coverage_sample()
            .with(eq(models::CoverageSample {
                id: uuid::Uuid::nil(),
                ..inserted_coverage_sample
            }))
            .return_once(move |mut sample| {
                sample.id = inserted_coverage_sample.id;
                Ok(sample)
            })
            .times(1)
            .in_sequence(sequence);

        parse_ctx
            .db
            .report_builder
            .expect_associate_context()
            .with(eq(models::ContextAssoc {
                context_id: session_context_id,
                sample_id: Some(inserted_coverage_sample.id),
                ..Default::default()
            }))
            .returning(|assoc| Ok(assoc))
            .times(1)
            .in_sequence(sequence);

        if let Some(Some(missing_branches)) = &line_session.branches {
            for branch in missing_branches {
                let (branch_format, branch_serialized) = format_pyreport_branch(branch);
                parse_ctx
                    .db
                    .report_builder
                    .expect_insert_branches_data()
                    .with(eq(models::BranchesData {
                        source_file_id,
                        sample_id: inserted_coverage_sample.id,
                        hits: 0,
                        branch_format,
                        branch: branch_serialized,
                        ..Default::default()
                    }))
                    .return_once(move |mut branch| {
                        branch.id = uuid::Uuid::new_v4();
                        Ok(branch)
                    })
                    .times(1)
                    .in_sequence(sequence);
            }
        } else {
            parse_ctx
                .db
                .report_builder
                .expect_insert_branches_data()
                .times(0);
        }

        if let Some(Some(complexity)) = &line_session.complexity {
            let (covered, total) = separate_pyreport_complexity(complexity);
            parse_ctx
                .db
                .report_builder
                .expect_insert_method_data()
                .with(eq(models::MethodData {
                    source_file_id,
                    sample_id: Some(inserted_coverage_sample.id),
                    line_no: Some(inserted_coverage_sample.line_no),
                    hit_complexity_paths: covered,
                    total_complexity: total,
                    ..Default::default()
                }))
                .return_once(move |mut method| {
                    method.id = uuid::Uuid::new_v4();
                    Ok(method)
                })
                .times(1)
                .in_sequence(sequence);
        } else {
            parse_ctx
                .db
                .report_builder
                .expect_insert_method_data()
                .times(0);
        }

        if let Some(Some(partials)) = &line_session.partials {
            for Partial {
                start_col,
                end_col,
                coverage,
            } in partials
            {
                let hits = match coverage {
                    PyreportCoverage::HitCount(hits) => *hits as i64,
                    _ => 0,
                };
                parse_ctx
                    .db
                    .report_builder
                    .expect_insert_span_data()
                    .with(eq(models::SpanData {
                        source_file_id,
                        sample_id: Some(inserted_coverage_sample.id),
                        hits,
                        start_line: Some(inserted_coverage_sample.line_no),
                        start_col: start_col.map(|x| x as i64),
                        end_line: Some(inserted_coverage_sample.line_no),
                        end_col: end_col.map(|x| x as i64),
                        ..Default::default()
                    }))
                    .return_once(move |mut span| {
                        span.id = uuid::Uuid::new_v4();
                        Ok(span)
                    })
                    .times(1)
                    .in_sequence(sequence);
            }
        } else {
            parse_ctx
                .db
                .report_builder
                .expect_insert_span_data()
                .times(0);
        }

        inserted_coverage_sample
    }

    #[test]
    fn test_save_line_session_simple_line() {
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
        let input_type = models::CoverageType::Line;

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_line_with_partials() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_simple_method() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_method_with_total_complexity() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_method_with_split_complexity() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_simple_branch() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_branch_with_missing_branches_block_and_branch() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_branch_with_missing_branches_condition() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_line_session_branch_with_missing_branches_line() {
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

        set_up_line_session_expectations(
            &input_session,
            input_type,
            parse_ctx,
            &mut test_ctx.sequence,
        );
        assert!(save_line_session(&input_session, &input_type, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_report_line_line_no_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        let coverage_type = models::CoverageType::Line;
        let coverage = PyreportCoverage::HitCount(10);

        let mut sessions = Vec::new();
        for i in 0..3 {
            sessions.push(LineSession {
                session_id: i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: None,
            });
            set_up_line_session_expectations(
                &sessions[i],
                coverage_type,
                parse_ctx,
                &mut test_ctx.sequence,
            );
        }

        let report_line = ReportLine {
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: None,
            datapoints: None,
        };
        assert!(save_report_line(&report_line, parse_ctx).is_ok());
    }

    fn set_up_datapoints_expectations(
        inserted_sample: models::CoverageSample,
        parse_ctx: &mut ParseCtx<MockReport, MockReportBuilder<MockReport>>,
    ) {
        parse_ctx
            .db
            .report_builder
            .expect_associate_context()
            .with(eq(models::ContextAssoc {
                context_id: 50,
                sample_id: Some(inserted_sample.id),
                ..Default::default()
            }))
            .returning(|assoc| Ok(assoc))
            .times(1);
        parse_ctx
            .db
            .report_builder
            .expect_associate_context()
            .with(eq(models::ContextAssoc {
                context_id: 51,
                sample_id: Some(inserted_sample.id),
                ..Default::default()
            }))
            .returning(|assoc| Ok(assoc))
            .times(1);
    }

    #[test]
    fn test_save_report_line_line_with_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        let coverage_type = models::CoverageType::Line;
        let coverage = PyreportCoverage::HitCount(10);

        parse_ctx.labels_index = HashMap::from([
            ("test_label".to_string(), 50),
            ("test_label_2".to_string(), 51),
        ]);

        let mut sessions = Vec::new();
        let mut datapoints = HashMap::new();
        for i in 0..3 {
            sessions.push(LineSession {
                session_id: i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: None,
            });
            let sample = set_up_line_session_expectations(
                &sessions[i],
                coverage_type,
                parse_ctx,
                &mut test_ctx.sequence,
            );

            datapoints.insert(
                i as u32,
                CoverageDatapoint {
                    session_id: i as u32,
                    _coverage: coverage.clone(),
                    _coverage_type: Some(coverage_type),
                    labels: vec!["test_label".to_string(), "test_label_2".to_string()],
                },
            );
            set_up_datapoints_expectations(sample.clone(), parse_ctx);
        }

        let report_line = ReportLine {
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: None,
            datapoints: Some(Some(datapoints)),
        };
        assert!(save_report_line(&report_line, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_report_line_branch_no_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        let coverage_type = models::CoverageType::Branch;
        let coverage = PyreportCoverage::BranchesTaken {
            covered: 2,
            total: 4,
        };

        let mut sessions = Vec::new();
        for i in 0..3 {
            sessions.push(LineSession {
                session_id: i,
                coverage: coverage.clone(),
                branches: Some(Some(vec![MissingBranch::Line(26), MissingBranch::Line(27)])),
                partials: None,
                complexity: None,
            });
            set_up_line_session_expectations(
                &sessions[i],
                coverage_type,
                parse_ctx,
                &mut test_ctx.sequence,
            );
        }

        let report_line = ReportLine {
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: None,
            datapoints: None,
        };
        assert!(save_report_line(&report_line, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_report_line_branch_with_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        let coverage_type = models::CoverageType::Branch;
        let coverage = PyreportCoverage::BranchesTaken {
            covered: 3,
            total: 4,
        };

        parse_ctx.labels_index = HashMap::from([
            ("test_label".to_string(), 50),
            ("test_label_2".to_string(), 51),
        ]);

        let mut sessions = Vec::new();
        let mut datapoints = HashMap::new();
        for i in 0..3 {
            sessions.push(LineSession {
                session_id: i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: None,
            });
            let sample = set_up_line_session_expectations(
                &sessions[i],
                coverage_type,
                parse_ctx,
                &mut test_ctx.sequence,
            );

            datapoints.insert(
                i as u32,
                CoverageDatapoint {
                    session_id: i as u32,
                    _coverage: coverage.clone(),
                    _coverage_type: Some(coverage_type),
                    labels: vec!["test_label".to_string(), "test_label_2".to_string()],
                },
            );
            set_up_datapoints_expectations(sample.clone(), parse_ctx);
        }

        let report_line = ReportLine {
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: None,
            datapoints: Some(Some(datapoints)),
        };
        assert!(save_report_line(&report_line, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_report_line_method_no_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        let coverage_type = models::CoverageType::Method;
        let coverage = PyreportCoverage::HitCount(2);

        let mut sessions = Vec::new();
        for i in 0..3 {
            sessions.push(LineSession {
                session_id: i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: Some(Some(Complexity::PathsTaken {
                    covered: 1,
                    total: 3,
                })),
            });
            set_up_line_session_expectations(
                &sessions[i],
                coverage_type,
                parse_ctx,
                &mut test_ctx.sequence,
            );
        }

        let report_line = ReportLine {
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: Some(Some(Complexity::PathsTaken {
                covered: 1,
                total: 3,
            })),
            datapoints: None,
        };
        assert!(save_report_line(&report_line, parse_ctx).is_ok());
    }

    #[test]
    fn test_save_report_line_method_with_datapoints() {
        let mut test_ctx = setup();
        let parse_ctx = &mut test_ctx.parse_ctx;
        parse_ctx.chunk.current_line = 1;
        let coverage_type = models::CoverageType::Method;
        let coverage = PyreportCoverage::HitCount(2);

        parse_ctx.labels_index = HashMap::from([
            ("test_label".to_string(), 50),
            ("test_label_2".to_string(), 51),
        ]);

        let mut sessions = Vec::new();
        let mut datapoints = HashMap::new();
        for i in 0..3 {
            sessions.push(LineSession {
                session_id: i,
                coverage: coverage.clone(),
                branches: None,
                partials: None,
                complexity: Some(Some(Complexity::PathsTaken {
                    covered: 1,
                    total: 3,
                })),
            });
            let sample = set_up_line_session_expectations(
                &sessions[i],
                coverage_type,
                parse_ctx,
                &mut test_ctx.sequence,
            );

            datapoints.insert(
                i as u32,
                CoverageDatapoint {
                    session_id: i as u32,
                    _coverage: coverage.clone(),
                    _coverage_type: Some(coverage_type),
                    labels: vec!["test_label".to_string(), "test_label_2".to_string()],
                },
            );
            set_up_datapoints_expectations(sample.clone(), parse_ctx);
        }

        let report_line = ReportLine {
            coverage,
            sessions,
            coverage_type,
            _messages: None,
            _complexity: Some(Some(Complexity::PathsTaken {
                covered: 1,
                total: 3,
            })),
            datapoints: Some(Some(datapoints)),
        };
        assert!(save_report_line(&report_line, parse_ctx).is_ok());
    }
}
