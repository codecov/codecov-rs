use pyo3::prelude::*;

// See if non-pyo3-annotated Rust lines are still instrumented
fn raw_rust_add(a: usize, b: usize) -> usize {
    println!("hello");
    a + b
}

#[pyfunction]
fn dummy_add(a: usize, b: usize) -> PyResult<usize> {
    Ok(raw_rust_add(a, b))
}

#[pymodule]
fn _bindings(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(dummy_add, m)?)?;
    Ok(())
}
