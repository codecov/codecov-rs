pub use codecov_rs::error::CodecovError as RsCodecovError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

pub struct PyCodecovError(RsCodecovError);

impl From<PyCodecovError> for PyErr {
    fn from(error: PyCodecovError) -> Self {
        PyRuntimeError::new_err(error.0.to_string())
    }
}

impl From<RsCodecovError> for PyCodecovError {
    fn from(other: RsCodecovError) -> Self {
        Self(other)
    }
}
