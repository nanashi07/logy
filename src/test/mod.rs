use std::io::Result;

use log::info;

use super::reducer;
use super::tracer;

#[test]
fn test_reduce_log() -> Result<()> {
    let files = vec!["/Users/nanashi07/Desktop/2021/09/big/real/source/app.2021-09-26.20.real-sports-game-7b88668458-vrlrw.log"];

    reducer::reduce_logs(
        &files,
        r#"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3})"#,
        "%Y-%m-%d %H:%M:%S%.3f",
        "/Users/nanashi07/Desktop/2021/09/big/real/tt/trace.output.log",
        9,
    )?;
    Ok(())
}

#[test]
fn test_trace_log() -> Result<()> {
    let files = vec![
        "/Users/nanashi07/Desktop/2021/09/big/real/target/real-sports-game.20210927-01.log.gz",
    ];
    tracer::trace_log(
        &files,
        4000,
        r#"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3})"#,
        "%Y-%m-%d %H:%M:%S%.3f",
        "real-sports-game-.+,(\\w+,\\w+)",
        "/Users/nanashi07/Desktop/2021/09/big/real/trace.output.log",
    )?;
    info!("task done");
    Ok(())
}
