use thiserror::Error;

pub type Result<O> = std::result::Result<O, CodecovError>;

#[derive(Error, Debug)]
pub enum CodecovError {
    #[error("sqlite failure: '{0}'")]
    SqliteError(#[from] rusqlite::Error),

    #[error("sqlite migration failure: '{0}'")]
    SqliteMigrationError(#[from] rusqlite_migration::Error),

    // Can't use #[from]
    #[error("parser error: '{0}'")]
    ParserError(winnow::error::ContextError),

    #[error("io error: '{0}'")]
    IOError(#[from] std::io::Error),
}
