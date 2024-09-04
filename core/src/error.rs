use thiserror::Error;

use crate::parsers::pyreport::chunks_serde::ChunksFileParseError;

pub type Result<T, E = CodecovError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum CodecovError {
    #[error("sqlite failure: '{0}'")]
    SqliteError(#[from] rusqlite::Error),

    #[error("sqlite migration failure: '{0}'")]
    SqliteMigrationError(#[from] rusqlite_migration::Error),

    #[error("report builder error: '{0}'")]
    ReportBuilderError(String),

    // Can't use #[from]
    #[error("parser error: '{0}'")]
    ParserError(winnow::error::ContextError),

    #[error("parser error: '{0}'")]
    Json(#[from] serde_json::Error),

    #[error("io error: '{0}'")]
    IOError(#[from] std::io::Error),

    #[cfg(feature = "pyreport")]
    #[error("failed to convert sqlite to pyreport: '{0}'")]
    PyreportConversionError(String),

    #[error(transparent)]
    ChunksFileParseError(#[from] ChunksFileParseError),
}
