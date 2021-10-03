use clap::{App, Arg, ArgMatches, SubCommand};
use log::{error, info};
use std::io::Result;

mod models;
mod reducer;
#[cfg(test)]
mod test;
mod tracer;

fn main() -> Result<()> {
    let app = command_args();

    env_logger::init();

    if let Some(args) = app.subcommand_matches("reduce") {
        if let Some(files) = args.values_of("files") {
            reducer::reduce_logs(
                &files.collect(),
                args.value_of("prefix").unwrap(),
                args.value_of("log-time-format").unwrap(),
                args.value_of("out-file-pattern").unwrap(),
                args.value_of("compress-level")
                    .unwrap()
                    .parse::<u32>()
                    .unwrap(),
            )?;
            info!("task done");
        } else {
            error!("No source file provided");
            return Ok(());
        }
    } else if let Some(args) = app.subcommand_matches("trace") {
        if let Some(files) = args.values_of("files") {
            tracer::trace_log(
                &files.collect(),
                args.value_of("minimal-cost-time")
                    .unwrap()
                    .parse::<i64>()
                    .unwrap(),
                args.value_of("prefix").unwrap(),
                args.value_of("log-time-format").unwrap(),
                args.value_of("trace-pattern").unwrap(),
                args.value_of("out-file-pattern").unwrap(),
            )?;
            info!("task done");
        } else {
            error!("No source file provided");
            return Ok(());
        }
    }

    Ok(())
}

fn command_args<'a>() -> ArgMatches<'a> {
    App::new("logy")
        .version("0.0.1")
        .author("Bruce Tsai")
        .subcommand(
            SubCommand::with_name("reduce")
                .about("Reduce multiple log files into single one")
                .args(&[
                    Arg::with_name("prefix")
                        .short("p")
                        .long("prefix")
                        .takes_value(true)
                        .help("Prefix pattern to determin start of log line")
                        .default_value("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}"),
                    Arg::with_name("log-time-format")
                        .short("t")
                        .long("log-time")
                        .takes_value(true)
                        .help("Log time format to parse")
                        .default_value("%Y-%m-%d %H:%M:%S%.3f"),
                    Arg::with_name("out-file-pattern")
                        .short("o")
                        .long("out-files")
                        .takes_value(true)
                        .help("Output file pattern")
                        .default_value("output.%Y%m%d-%H.log"),
                    Arg::with_name("compress-level")
                        .short("c")
                        .long("compress")
                        .takes_value(true)
                        .help("Compress level for output files")
                        .default_value("9"),
                    Arg::with_name("files")
                        .required(true)
                        .multiple(true)
                        .help("Target files for reduce"),
                ]),
        )
        .subcommand(
            SubCommand::with_name("trace")
                .about("Trace log and find out long executed")
                .args(&[
                    Arg::with_name("prefix")
                        .short("p")
                        .long("prefix")
                        .takes_value(true)
                        .help("Prefix pattern to determin start of log line")
                        .default_value("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}"),
                    Arg::with_name("log-time-format")
                        .short("t")
                        .long("log-time")
                        .takes_value(true)
                        .help("Log time format to parse")
                        .default_value("%Y-%m-%d %H:%M:%S%.3f"),
                    Arg::with_name("out-file-pattern")
                        .short("o")
                        .long("out-files")
                        .takes_value(true)
                        .help("Output file pattern")
                        .default_value("traced.output.log"),
                    Arg::with_name("trace-pattern")
                        .short("g")
                        .long("trace-pattern")
                        .takes_value(true)
                        .required(true)
                        .help("Trace ID pattern in logs to group same process")
                        .default_value("output.log"),
                    Arg::with_name("minimal-cost-time")
                        .short("d")
                        .long("duration")
                        .takes_value(true)
                        .help("Minimal duration of traced process")
                        .default_value("8000"),
                    Arg::with_name("files")
                        .required(true)
                        .multiple(true)
                        .help("Target files for trace"),
                ]),
        )
        .get_matches()
}
