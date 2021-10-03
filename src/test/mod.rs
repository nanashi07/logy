use std::io::Result;

use log::info;

use super::tracer;

#[test]
fn test_trace_log() -> Result<()> {
    let files = vec!["/Users/nanashi07/Desktop/2021/09/big/real/test//output.20210927-00.log.gz"];
    tracer::trace_log(
        &files,
        4000,
        "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}",
        "%Y-%m-%d %H:%M:%S%.3f",
        "real-sports-game-.+,(\\w+,\\w+)",
        "/Users/nanashi07/Desktop/2021/09/big/real/trace.output.log",
    )?;
    info!("task done");
    Ok(())
}
