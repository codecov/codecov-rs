use pyo3::prelude::*;

#[pymodule]
fn _bindings(_py: Python, _m: Bound<PyModule>) -> PyResult<()> {
    Ok(())
}
