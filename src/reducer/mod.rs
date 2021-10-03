use super::models::FileNameGetter;
use super::models::Log;
use super::models::LogLine;
use super::models::NextLogLineFinder;
use super::models::WrappedFileReader;
use super::models::WrappedFileWriter;

use chrono::{Duration, NaiveDateTime};
use log::debug;
use std::{
    collections::{BTreeSet, HashMap},
    io::Result,
    sync::mpsc::{self},
    thread,
};

/// read multiple files and compress output
pub fn reduce_logs(
    files: &Vec<&str>,
    pattern: &str,
    log_time_format: &str,
    output_file_pattern: &str,
    compress_level: u32,
) -> Result<()> {
    let mut writer = WrappedFileWriter::new(output_file_pattern, compress_level);

    let (tx, rx) = mpsc::sync_channel::<String>(100);
    let files = files
        .iter()
        .map(|&s| s.to_string())
        .collect::<Vec<String>>();
    let pattern = pattern.to_string();

    thread::spawn(move || {
        let mut sorted_set: BTreeSet<LogLine> = BTreeSet::new();
        let mut readers = files
            .iter()
            .map(|path| {
                (
                    path.to_string(),
                    WrappedFileReader::new(path.as_str(), pattern.as_str(), false),
                )
            })
            .collect::<HashMap<String, WrappedFileReader>>();

        let file_count = files.len();
        let mut file_done_count = 0;

        // read head line from files
        let empty_files = readers
            .values_mut()
            .filter_map(|reader| {
                let filename = reader.filename();
                if let Log::Line(line) = reader.next_log() {
                    sorted_set.insert(LogLine::new(&filename, &line));
                    None
                } else {
                    // read to end of file
                    // remove reader from list
                    Some(filename)
                }
            })
            .collect::<Vec<String>>();

        for empty_file in empty_files {
            file_done_count = file_done_count + 1;
            debug!(
                "finish reader {}/{} {}",
                file_done_count, file_count, empty_file
            );
            readers.remove(&empty_file);
        }

        while !readers.is_empty() || !sorted_set.is_empty() {
            if let Some((filename, line)) =
                sorted_set.iter().next().map(|v| (v.filename(), v.value()))
            {
                let v = LogLine::new(&filename, &line);
                sorted_set.remove(&v);
                tx.send(line).unwrap();

                if let Some(reader) = readers.get_mut(&filename) {
                    if let Log::Line(line) = reader.next_log() {
                        sorted_set.insert(LogLine::new(&filename, &line));
                    } else {
                        // read to end of file
                        // remove reader from list
                        file_done_count = file_done_count + 1;
                        debug!(
                            "finish reader {}/{} {}",
                            file_done_count, file_count, filename
                        );
                        readers.remove(&filename);
                    }
                } else if let Some(reader) = readers.values_mut().last() {
                    if let Log::Line(line) = reader.next_log() {
                        sorted_set.insert(LogLine::new(&filename, &line));
                    } else {
                        // read to end of file
                        // remove reader from list
                        file_done_count = file_done_count + 1;
                        debug!(
                            "finish reader {}/{} {}",
                            file_done_count, file_count, filename
                        );
                        readers.remove(&filename);
                    }
                }
            }
        }
    });

    let seconds_an_hour = Duration::hours(1).num_seconds();
    for value in rx {
        let log_time = NaiveDateTime::parse_from_str(&value[0..23], log_time_format).unwrap(); // TODO: slice time issue, need by variable
        let log_hour = log_time.timestamp() / seconds_an_hour;

        writer.write(log_hour, &value);
    }

    writer.flush();

    Ok(())
}