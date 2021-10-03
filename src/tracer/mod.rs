use chrono::{Duration, NaiveDateTime};
use log::info;
use regex::Regex;
use std::{
    cmp::{self},
    collections::HashMap,
    io::Result,
    vec,
};

use super::models::{Log, LogDuration, NextLogLineFinder, WrappedFileReader, WrappedFileWriter};

pub fn trace_log(
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
            if let Some(captures) = re.captures(line.as_str()) {
                let trace_id = captures.get(1).unwrap().as_str().to_string();
                // TODO: slice time issue, need by variable
                let log_time =
                    NaiveDateTime::parse_from_str(&line.clone()[0..23], log_time_format).unwrap();
                let log_time_milli = log_time.timestamp_millis();

                if let Some(item) = map.get(&trace_id) {
                    let newone = LogDuration {
                        trace_id: item.trace_id.to_string(),
                        start_time: cmp::min(item.start_time, log_time_milli),
                        end_time: cmp::max(item.end_time, log_time_milli),
                    };
                    &map.insert(trace_id, newone);
                } else {
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

        info!("{} entries collected", map.len());
        let mut filtered = map
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
            "{} entries cost time over than {} ms",
            filtered.len(),
            min_cost_time
        );

        let mut grouped_logs: HashMap<String, Vec<String>> = HashMap::new();
        reader = WrappedFileReader::new(file, pattern, true);
        let mut writer = WrappedFileWriter::new(output_file_pattern, 0);

        info!("start to output time cost logs from {}", file);
        while let Log::Line(line) = reader.next_log() {
            if let Some(captures) = re.captures(line.as_str()) {
                let trace_id = captures.get(1).unwrap().as_str().to_string();
                // TODO: slice time issue, need by variable
                let log_time =
                    NaiveDateTime::parse_from_str(&line.clone()[0..23], log_time_format).unwrap();
                let log_time_milli = log_time.timestamp_millis();

                if let Some(_) = filtered.get(&trace_id) {
                    if let Some(value) = grouped_logs.get_mut(&trace_id) {
                        value.push(line);
                    } else {
                        grouped_logs.insert(trace_id.clone(), vec![line]);
                    }
                }

                // find ended log and output
                let reached_ended_logs = filtered
                    .values()
                    .filter(|&duration| duration.end_time < log_time_milli)
                    .map(|duration| duration.trace_id.clone())
                    .collect::<Vec<String>>();

                write_long_logs(
                    &mut writer,
                    &mut filtered,
                    &mut grouped_logs,
                    &reached_ended_logs,
                );
            }
        }

        // output remained
        let trace_ids = filtered
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        write_long_logs(&mut writer, &mut filtered, &mut grouped_logs, &trace_ids);

        info!("finish output time cost logs from {}", file);
    }

    Ok(())
}

fn write_long_logs(
    writer: &mut WrappedFileWriter,
    filtered: &mut HashMap<String, LogDuration>,
    grouped_logs: &mut HashMap<String, Vec<String>>,
    trace_ids: &Vec<String>,
) {
    for trace_id in trace_ids {
        let lines = grouped_logs.get_mut(trace_id).unwrap();
        let duration = filtered.get(trace_id).unwrap();
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
        filtered.remove(trace_id);
    }
}
