use chrono::{Duration, NaiveDateTime};
use log::info;
use regex::Regex;
use std::{
    cmp::{self},
    collections::HashMap,
    io::Result,
    vec,
};

use super::models::Log;
use super::models::LogDuration;
use super::models::NextLogLineFinder;
use super::models::WrappedFileReader;
use super::models::WrappedFileWriter;

fn trace_log(
    files: &Vec<&str>,
    min_cost_time: i64,
    pattern: &str,
    log_time_format: &str,
    trace_pattern: &str,
    output_file_pattern: &str,
) -> Result<()> {
    let mut map: HashMap<String, LogDuration> = HashMap::new();
    let re = Regex::new(trace_pattern).unwrap();

    for &file in files {
        info!("Load file {} to collect cost time", file);
        let mut reader = WrappedFileReader::new(file, pattern, true);
        while let Log::Line(line) = reader.next_log() {
            match re.captures(line.as_str()) {
                Some(captures) => {
                    let trace_id = captures.get(1).unwrap().as_str().to_string();
                    // TODO: slice time issue, need by variable
                    let log_time =
                        NaiveDateTime::parse_from_str(&line.clone()[0..23], log_time_format)
                            .unwrap();
                    let log_time_milli = log_time.timestamp_millis();

                    match map.get(&trace_id) {
                        Some(item) => {
                            let newone = LogDuration {
                                trace_id: item.trace_id.to_string(),
                                start_time: cmp::min(item.start_time, log_time_milli),
                                end_time: cmp::max(item.end_time, log_time_milli),
                            };
                            &map.insert(trace_id, newone);
                        }
                        None => {
                            &map.insert(
                                trace_id.clone(),
                                LogDuration {
                                    trace_id: trace_id.clone(),
                                    start_time: log_time_milli,
                                    end_time: log_time_milli,
                                },
                            );
                        }
                    }
                }
                None => {}
            }
        }

        info!("{} entries collected", map.len());
        let filtered = map
            .values()
            .filter(|&d| d.end_time - d.start_time > min_cost_time)
            .map(|d| {
                (
                    d.trace_id.to_string(),
                    LogDuration {
                        trace_id: d.trace_id.to_string(),
                        start_time: d.start_time,
                        end_time: d.end_time,
                    },
                )
            })
            .collect::<HashMap<String, LogDuration>>();
        info!(
            "{} entries cost time over than {}",
            filtered.len(),
            min_cost_time
        );

        let mut grouped_logs: HashMap<String, Vec<String>> = HashMap::new();
        reader = WrappedFileReader::new(file, pattern, true);
        let mut writer = WrappedFileWriter::new(output_file_pattern, 0);

        info!("start to output time cost logs from {}", file);
        while let Log::Line(line) = reader.next_log() {
            match re.captures(line.as_str()) {
                Some(captures) => {
                    let trace_id = captures.get(1).unwrap().as_str().to_string();
                    // TODO: slice time issue, need by variable
                    let log_time =
                        NaiveDateTime::parse_from_str(&line.clone()[0..23], log_time_format)
                            .unwrap();
                    let log_time_milli = log_time.timestamp_millis();
                    let log_hour = log_time_milli / Duration::hours(1).num_milliseconds();

                    match filtered.get(&trace_id) {
                        Some(duration) => {
                            match grouped_logs.get_mut(&trace_id) {
                                Some(value) => {
                                    value.push(line);
                                }
                                None => {
                                    grouped_logs.insert(trace_id.clone(), vec![line]);
                                }
                            }

                            if log_time_milli >= duration.end_time {
                                let v = grouped_logs.get_mut(&trace_id).unwrap();
                                v.insert(
                                    0,
                                    format!(
                                        "========================= {} =========================",
                                        Duration::milliseconds(
                                            duration.end_time - duration.start_time
                                        )
                                    ),
                                );
                                v.push("\n".repeat(3));
                                // TODO: write log
                                writer.write(log_hour, &v.join("\n"));
                                grouped_logs.remove(&trace_id);
                            }
                        }
                        None => {}
                    }
                }
                None => {}
            }
        }
        info!("finish output time cost logs from {}", file);
    }

    Ok(())
}
