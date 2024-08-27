use std::{
    fs::read_to_string,
    path::{Path, PathBuf},
};

pub fn sample_data_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/common/sample_data")
}

pub fn read_sample_file(path: &Path) -> String {
    read_to_string(sample_data_path().join(path)).ok().unwrap()
}
