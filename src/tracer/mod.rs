use chrono::{Duration, NaiveDateTime};
use log::info;
use regex::Regex;
use std::{cmp, collections::HashMap, io::Result, vec};

use super::models::{Log, LogDuration, NextLogLineFinder, WrappedFileReader, WrappedFileWriter};

pub fn trace_log(
    files: &Vec<&str>,
    min_cost_time: i64,
    pattern: &str,
    log_time_format: &str,
    trace_pattern: &str,
    output_file_pattern: &str,
) -> Result<()> {
    let re = Regex::new(trace_pattern).unwrap();
    let parse_log_time_pattern = Regex::new(&pattern.to_string()).unwrap();

    for &file in files {
        info!("load file {} to collect cost time", file);
        let mut log_groups: HashMap<String, LogDuration> = HashMap::new();
        let mut reader = WrappedFileReader::new(file, pattern, true);
        while let Log::Line(line) = reader.next_log() {
            if let Some(captures) = re.captures(line.as_str()) {
                let trace_id = captures.get(1).unwrap().as_str().to_string();
                let log_time_string = parse_log_time_pattern
                    .captures(&line)
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .as_str()
                    .to_string();
                let log_time =
                    NaiveDateTime::parse_from_str(&log_time_string, log_time_format).unwrap();
                let log_time_millis = log_time.timestamp_millis();

                if let Some(item) = log_groups.get(&trace_id) {
                    let newone = LogDuration {
                        trace_id: item.trace_id.to_string(),
                        start_time: cmp::min(item.start_time, log_time_millis),
                        end_time: cmp::max(item.end_time, log_time_millis),
                    };
                    &log_groups.insert(trace_id, newone);
                } else {
                    &log_groups.insert(
                        trace_id.clone(),
                        LogDuration {
                            trace_id: trace_id.clone(),
                            start_time: log_time_millis,
                            end_time: log_time_millis,
                        },
                    );
                }
            }
        }

        info!("{} entries collected", log_groups.len());
        let mut long_duration_logs = log_groups
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

        log_groups.clear();

        info!(
            "{} entries cost time over than {} ms",
            long_duration_logs.len(),
            min_cost_time
        );

        let mut grouped_logs: HashMap<String, Vec<String>> = HashMap::new();
        reader = WrappedFileReader::new(file, pattern, true);
        let mut writer = WrappedFileWriter::new(output_file_pattern, 0);

        info!("start to output long process logs from {}", file);
        while let Log::Line(line) = reader.next_log() {
            if let Some(captures) = re.captures(line.as_str()) {
                let trace_id = captures.get(1).unwrap().as_str().to_string();
                let log_time_string = parse_log_time_pattern
                    .captures(&line)
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .as_str()
                    .to_string();
                let log_time =
                    NaiveDateTime::parse_from_str(&log_time_string, log_time_format).unwrap();
                let log_time_millis = log_time.timestamp_millis();

                if let Some(_) = long_duration_logs.get(&trace_id) {
                    if let Some(value) = grouped_logs.get_mut(&trace_id) {
                        value.push(line);
                    } else {
                        grouped_logs.insert(trace_id.clone(), vec![line]);
                    }
                }

                // find ended log and output
                let reached_ended_logs = long_duration_logs
                    .values()
                    .filter(|&duration| duration.end_time < log_time_millis)
                    .map(|duration| duration.trace_id.clone())
                    .collect::<Vec<String>>();

                write_long_logs(
                    &mut writer,
                    &mut long_duration_logs,
                    &mut grouped_logs,
                    &reached_ended_logs,
                );
            }
        }

        // output remained
        let trace_ids = long_duration_logs
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        write_long_logs(
            &mut writer,
            &mut long_duration_logs,
            &mut grouped_logs,
            &trace_ids,
        );

        info!("finish output long process logs from {}", file);
    }

    Ok(())
}

fn write_long_logs(
    writer: &mut WrappedFileWriter,
    long_duration_logs: &mut HashMap<String, LogDuration>,
    grouped_logs: &mut HashMap<String, Vec<String>>,
    trace_ids: &Vec<String>,
) {
    for trace_id in trace_ids {
        let lines = grouped_logs.get_mut(trace_id).unwrap();
        let duration = long_duration_logs.get(trace_id).unwrap();
        lines.insert(
            0,
            format!(
                "========================= {} =========================",
                Duration::milliseconds(duration.end_time - duration.start_time)
            ),
        );
        lines.push("\n".repeat(3));

        let log_hour = duration.end_time / Duration::hours(1).num_milliseconds();
        // write log
        writer.write(log_hour, &lines.join("\n"));
        grouped_logs.remove(trace_id);
        long_duration_logs.remove(trace_id);
    }
}
