use std::{env, fs::File, path::PathBuf};

use codecov_rs::parsers::pyreport_shim::parse_pyreport;

fn usage_error() -> ! {
    println!("Usage:");
    println!("  cargo run --example parse_pyreport -- [REPORT_JSON_PATH] [CHUNKS_PATH] [OUT_PATH]");

    std::process::exit(1);
}

pub fn main() -> Result<(), std::io::Error> {
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
