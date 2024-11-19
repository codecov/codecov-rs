/*!
 * Types and functions to interoperate with our Python report format.
 *
 * Reports in our Python codebase are serialized in two parts:
 * - Report JSON, which describes the files and sessions in the report
 * - Chunks file, which describes line-by-line coverage data for each file
 *
 * The format is messy and can only be fully understood by reading the
 * Python source in our `shared` repository's
 * [`shared/reports/resources.py`](https://github.com/codecov/shared/tree/main/shared/reports/resources.py),
 * [`shared/reports/types.py`](https://github.com/codecov/shared/blob/main/shared/reports/types.py),
 * and [`shared/utils/sessions.py`](https://github.com/codecov/shared/blob/main/shared/utils/sessions.py).
 *
 * Parsers that will build a [`SqliteReport`] from these parts live in
 * [`crate::parsers::pyreport`] but code that will convert a
 * [`SqliteReport`] back into a Pyreport lives here.
 *
 * # Report JSON
 *
 * The report JSON describes the source files covered by the report and the
 * "sessions", or uploads, that were sent to Codecov for this commit. These
 * are kept in the `"files"` and `"sessions"` keys respectively.
 *
 * In the `"files"` object, each key is the filepath of a source file
 * relative to the project's root, and the value includes some aggregate
 * metrics as well as the index of this file in the chunks file.
 *
 * In the `"sessions"` object, each key is a numeric ID and the value is a
 * grab-bag of metadata about the upload and the same sort of aggregated
 * totals that are in the `"files"` section.
 *
 * Some particular Python types to look at to understand the report JSON:
 * - [`ReportTotals`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L30-L45).
 * - [`ReportFileSummary`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L361-L367)
 * - [`Session`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/utils/sessions.py#L111-L128O)
 * - [`SessionTotalsArray`](https://github.com/codecov/shared/blob/e97a9f422a6e224b315d6dc3821f9f5ebe9b2ddd/shared/reports/types.py#L263-L272)
 *
 * A minimal example report JSON looks something like this:
 * ```notrust
 * {
 *   "files": {
 *     # Each file's key is its path, relative to the project's root.
 *     "app.py": [
 *       # This file's index in the chunks file.
 *       0,
 *
 *       # File totals, or Aggregated coverage totals for the file.
 *       [
 *         0,           # File count
 *         19,          # Lines tracked
 *         17,          # Lines hit
 *         2,           # Lines missed
 *         0,           # Lines "partially covered"
 *         "89.47368",  # Coverage percentage
 *         0,           # Number of missed branches
 *         0,           # Number of methods
 *         0,           # Number of "messages" (unused)
 *         0,           # Number of sessions
 *         0,           # Complexity paths hit
 *         0,           # Total complexity
 *         0            # "Diff" - TODO
 *       ],
 *
 *       # Session totals. The key corresponds to one of the sessions
 *       # in the "sessions" section and the values are aggregated coverage
 *       # totals for this file in that session.
 *       {"0": [
 *         0,           # File count
 *         19,          # Lines tracked
 *         17,          # Lines hit
 *         2,           # Lines missed
 *         0,           # Lines "partially covered"
 *         "89.47368"   # Coverage percentage
 *       ]},
 *
 *       # Diff totals - TODO
 *       [0, 0, 0, 0, 0, null, 0, 0, 0, 0, null, null, 0]
 *     ]
 *   },
 *   "sessions: {
 *     "0": {
 *       # "t" for "totals"
 *       # These fields are the same as they are in the "files" section
 *       "t": [285, 19795, 1812, 17983, 0, "9.15383", 0, 0, 0, 0, 0, 0, 0],
 *
 *       # Unix timestamp in seconds that the upload was received.
 *       "d": 1690230016,
 *
 *       # Storage location of the raw upload payload
 *       "a": "v4/raw/2023-07-24/{cut}/{cut}/{cut}/{cut}.txt",
 *
 *       # Flags, which can be used to distinguish between or filter uploads by,
 *       # for instance, platform.
 *       "f": ["onlysomelabels"],
 *
 *       # "Provider" - TODO
 *       "c": null,
 *
 *       # "Build" - TODO
 *       "n": null,
 *
 *       # Name of the upload, as it will be displayed in the Codecov UI
 *       "N": "CF[348] - Carriedforward",
 *
 *       # Job name, or the name of the CI workflow that submitted the upload.
 *       "j": "worker CI",
 *
 *       # CI run URL
 *       "u": "https:#circleci.com/gh/codecov/worker/764",
 *
 *       # "State" - TODO
 *       "p": null,
 *
 *       # "Env" - TODO
 *       "e": null,
 *
 *       # Session type. Values include "uploaded" if this is a new upload or
 *       # "carriedforward" if it was inherited from some other commit.
 *       "st": "carriedforward",
 *
 *       # Extra data associated with the upload. For instance, if it was carried
 *       # forward, this will contain the commit it was inherited from.
 *       "se": { "carriedforward_from": "bcec3478e2a27bb7950f40388cf191834fb2d5a3" }
 *     }
 *   }
 * }
 * ```
 *
 * # Chunks file
 *
 * The chunks file is much less human-readable. It starts with a header
 * which is a JSON object which may contain a "labels index". The labels
 * index maps a numeric ID to a label which can be a long string like the
 * name of a test case. If that label is in the index, it will be referred
 * to with its short ID in the rest of the chunks file. Otherwise, it's
 * referred to by name.
 *
 * After the header, we have the chunks. Each chunk corresponds to a source
 * file. To figure out _which_ source file, you need to cross-reference the
 * report JSON. If this is the Nth chunk, it corresponds to the file with N
 * in its "chunk index" slot in the report JSON.
 *
 * Each chunk also has a header, which is another JSON object. This can
 * contain a list of the sessions which have data for this file.
 *
 * Following each chunk's header is line-by-line data. The first line after
 * the header corresponds to line 1 in the source file, and so on. Empty
 * lines in the chunks file correspond to "ignored" lines in the source file
 * (comments, whitespace, the like). Non-empty lines contain a jumble of
 * nested lists with all the coverage data for that line.
 *
 * Before getting into the layout of one of these `ReportLine`s, take a look
 * at a minimal example chunks file:
 * ```notrust
 * {"labels_index":{}}
 * <<<<< end_of_header >>>>>
 * {"present_sessions": [0]}
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 *
 *
 *
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 *
 *
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [1, null, [[0, 1]], null, null, [[0, 1, null, ["Th2dMtk4M_codecov"]]]]
 * [0, null, [[0, 0]], null, null, [[0, 0, null, ["Th2dMtk4M_codecov"]]]]
 * [0, null, [[0, 0]], null, null, [[0, 0, null, ["Th2dMtk4M_codecov"]]]]
 * <<<<< end_of_chunk >>>>>
 * ```
 * Note: the last chunk in a chunks file is not followed by an `<<<<<
 * end_of_chunk >>>>>` header, it's just included here for illustration.
 *
 * This chunks file has one chunk in it. That chunk's index is 0, meaning it
 * corresponds to the `app.py` file from our example report JSON. If you'll
 * recall, the report JSON indicated that `app.py` had 19 tracked lines, 17
 * of which were hit and 2 were missed. There are 19 non-empty lines in this
 * chunk, 17 of which having a hit count (the first field) of 1 and 2 having
 * a hit count of 0. Everything adds up! In addition to corroborating the
 * totals in the report JSON, the chunks file also can tell us _which_ lines
 * were missed.
 *
 * Each populated line in the chunk is structured like so:
 * ```notrust
 * [                            // Start of report line
 *   "1/2",                     // Coverage
 *   "b",                       // Coverage type
 *   [                          // List of line sessions
 *     [
 *       0,                     // Session ID
 *       "1/2",                 // Coverage
 *       ["0:jump", "1"],       // List of missed branches
 *       [0, 3, 1],             // List of "partials"
 *       [0, 1],                // Cyclomatic complexity
 *     ]
 *   ],
 *   null,                      // Messages (unused)
 *   [0, 1],                    // Complexity
 *   [                          // List of datapoints
 *     [
 *       0,                     // Session ID (redundant)
 *       "1/2",                 // Coverage
 *       null,                  // Optional coverage type
 *       ["Th2dMtk4M_codecov"], // List of labels
 *     ]
 *   ]
 * ]
 * ```
 *
 * Some callouts:
 * - `coverage_type` is usually `null` for lines, `"b"` for branches, `"m"`
 *   for methods.
 * - `coverage` is usually an integer for lines and methods and a fraction
 *   (e.g. `"1/2"`) for branches. The fraction represents the number of
 *   covered paths over the number of possible paths.
 * - There are myriad ways that missing branches are represented
 * - Partials represent a subspan of this line and its coverage status. `[0,
 *   3, 1]` means that, from character 0 to character 3, this line was hit 1
 *   time.
 * - Cyclomatic complexity is sometimes just an integer and sometimes a
 *   2-item list. When it's a list, the first item is the number of
 *   cyclomatic complexity paths taken and the second is the total
 *   cyclomatic complexity.
 * - Don't be surprised if you find a chunks file that doesn't follow the
 *   rules.
 *
 * When writing chunks files out to disk, we'll strip trailing nulls from
 * lists to make things smaller. This can dramatically change what the
 * chunks file looks like, but when fields are present, they will always be
 * in this order. If `datapoints` is present, `messages` and `complexity`
 * must also be present, even if their values are just `null`.
 *
 * Some particular Python types to look at to understand the chunks file:
 * - [`ReportLine`](https://github.com/codecov/shared/blob/f6c2c3852530192ab0c6b9fd0c0a800c2cbdb16f/shared/reports/types.py#L130)
 * - [`LineSession`](https://github.com/codecov/shared/blob/f6c2c3852530192ab0c6b9fd0c0a800c2cbdb16f/shared/reports/types.py#L76)
 * - [`CoverageDatapoint`](https://github.com/codecov/shared/blob/f6c2c3852530192ab0c6b9fd0c0a800c2cbdb16f/shared/reports/types.py#L98)
 */

use std::{
    fs::File,
    io::{BufWriter, Write},
};

use super::SqliteReport;
use crate::error::Result;

mod chunks;
mod report_json;
pub mod types;

pub(crate) const CHUNKS_FILE_HEADER_TERMINATOR: &str = "\n<<<<< end_of_header >>>>>\n";
pub(crate) const CHUNKS_FILE_END_OF_CHUNK: &str = "\n<<<<< end_of_chunk >>>>>\n";

pub trait ToPyreport {
    /// Format and write the contents of a [`SqliteReport`] to
    /// `report_json_file` and `chunks_file`.
    fn to_pyreport(&self, report_json_file: &mut File, chunks_file: &mut File) -> Result<()>;
}

impl ToPyreport for SqliteReport {
    fn to_pyreport(&self, report_json_file: &mut File, chunks_file: &mut File) -> Result<()> {
        let mut writer = BufWriter::new(report_json_file);
        report_json::sql_to_report_json(self, &mut writer)?;
        writer.flush()?;

        let mut writer = BufWriter::new(chunks_file);
        chunks::sql_to_chunks(self, &mut writer)?;
        writer.flush()?;

        Ok(())
    }
}
