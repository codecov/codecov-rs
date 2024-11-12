use std::{
    fmt,
    fs::File,
    io::{Read, Seek},
    path::PathBuf,
};

#[derive(Copy, Clone)]
pub enum FixtureFormat {
    Pyreport,
}

impl fmt::Display for FixtureFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FixtureFormat::Pyreport => write!(f, "pyreport"),
        }
    }
}

#[derive(Copy, Clone)]
pub enum FixtureSize {
    Large,
    Small,
}

impl fmt::Display for FixtureSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FixtureSize::Large => write!(f, "large"),
            FixtureSize::Small => write!(f, ""),
        }
    }
}

fn fixture_dir(format: FixtureFormat, size: FixtureSize) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(format.to_string())
        .join(size.to_string())
}

#[track_caller]
pub fn open_fixture(
    format: FixtureFormat,
    size: FixtureSize,
    name: &str,
) -> Result<File, &'static str> {
    let path = fixture_dir(format, size).join(name);
    let mut file = File::open(path).map_err(|_| "failed to open file")?;

    let mut buf = [0; 50];
    file.read(&mut buf)
        .map_err(|_| "failed to read beginning of file")?;
    if buf.starts_with(b"version https://git-lfs.github.com/spec/v1") {
        Err("fixture has not been pulled from Git LFS")
    } else {
        file.rewind().unwrap();
        Ok(file)
    }
}

pub fn read_fixture(
    format: FixtureFormat,
    size: FixtureSize,
    name: &str,
) -> Result<Vec<u8>, &'static str> {
    // Just make sure the file exists and that it has been pulled from Git LFS
    let mut file = open_fixture(format, size, name)?;

    // Actually read and return the contents
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|_| "failed to read file")?;
    Ok(buf)
}
