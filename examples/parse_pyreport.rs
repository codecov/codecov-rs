use std::{env, fs::File, path::PathBuf};

use codecov_rs::{error::Result, parsers::pyreport::parse_pyreport};

fn usage_error() -> ! {
    println!("Usage:");
    println!("  cargo run --example parse_pyreport -- [REPORT_JSON_PATH] [CHUNKS_PATH] [OUT_PATH]");
    println!("");
    println!("Example:");
    println!("  cargo run --example parse_pyreport -- tests/common/sample_data/codecov-rs-reports-json-d2a9ba1.txt tests/common/sample_data/codecov-rs-chunks-d2a9ba1.txt d2a9ba1.sqlite");

    std::process::exit(1);
}

pub fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        usage_error();
    }

    let report_json_file = File::open(&args[1])?;
    let chunks_file = File::open(&args[2])?;
    let out_path = PathBuf::from(&args[3]);

    parse_pyreport(&report_json_file, &chunks_file, out_path)?;

    Ok(())
}
