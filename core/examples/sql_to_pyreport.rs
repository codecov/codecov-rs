use std::{env, fs::File};

use codecov_rs::{
    error::Result,
    report::{pyreport::ToPyreport, SqliteReport},
};

fn usage_error() -> ! {
    println!("Usage:");
    println!(
        "  cargo run --example parse_pyreport -- [SQLITE_PATH] [REPORT_JSON_PATH] [CHUNKS_PATH]"
    );
    println!();
    println!("Example:");
    println!(
        "  cargo run --example parse_pyreport -- d2a9ba1.sqlite ~/report_json.json ~/chunks.txt"
    );

    std::process::exit(1);
}

pub fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        usage_error();
    }

    let sqlite_path = &args[1];
    let report = SqliteReport::new(sqlite_path.into())?;

    let mut report_json_file = File::create(&args[2])?;
    let mut chunks_file = File::create(&args[3])?;

    report.to_pyreport(&mut report_json_file, &mut chunks_file)?;

    Ok(())
}
