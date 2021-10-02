use std::{
    cmp::{self},
    collections::{HashMap, LinkedList},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Result, Write},
    path::Path,
    string, vec,
};

use chrono::{Duration, NaiveDateTime};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use log::info;
use regex::Regex;

fn find_long_log() -> Result<()> {
    let source_path = "/Users/nanashi07/Desktop/2021/09/slow-ke-facts/reduced";

    let pattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}";
    let log_time_format = "%Y-%m-%d %H:%M:%S%.3f";
    let trace_pattern = "\\[real-sports-game-internal-\\w+-\\w+,(\\w+,\\w+)\\]";
    let output_file_pattern = "app.%Y%m%d-%H.log";
    let group_file_pattern = "group.%Y%m%d-%H.log";

    let sorted_logs = fs::read_dir(source_path)?
        .into_iter()
        .map(|p| p.unwrap())
        .map(|p| p.path())
        .filter(|p| p.is_file())
        .filter(|p| p.extension().map(|s| s == "gz").unwrap_or(false))
        .map(|p| p.display().to_string())
        .collect::<Vec<String>>();
    // info!("{:?}", sorted_logs);
    let logs = sorted_logs.iter().map(|s| s.as_str()).collect();
    read_compress_and_group(
        &logs,
        1000,
        pattern,
        log_time_format,
        trace_pattern,
        group_file_pattern,
    )?;
    info!("======================================================================================================");

    Ok(())
}

/// read files by directly buffer reader
fn read_and_print(files: &Vec<&str>) -> Result<()> {
    let file = File::open(files[0])?;
    let mut buffer_reader = BufReader::new(file);
    let mut line = String::new();
    let mut count = 0;
    while buffer_reader.read_line(&mut line)? > 0 && count < 100 {
        info!("read_and_print = {}", line);
        count = count + 1;
        if count > 10 {
            break;
        }
    }
    Ok(())
}

/// read file by wrapper
fn read_and_print2(files: &Vec<&str>, pattern: &str) -> Result<()> {
    let file = files[0];
    let mut file_reader = WrappedFileReader {
        file: file.to_string(),
        pattern: Regex::new(pattern).unwrap(),
        reader: Box::new(BufReader::new(File::open(file).unwrap())),
        buffer: Box::new(Vec::new()),
    };
    let mut count = 0;
    while let Log::Line(line) = file_reader.next_log() {
        info!("read_and_print2 = {:?}", line);
        count = count + 1;
        if count > 10 {
            break;
        }
    }
    Ok(())
}

/// read file by wrapper with new()
fn read_and_print3(files: &Vec<&str>, pattern: &str) -> Result<()> {
    let file = files[0];
    let mut file_reader = WrappedFileReader::new(file, pattern, false);
    let mut count = 0;
    while let Log::Line(line) = file_reader.next_log() {
        info!("read_and_print3 = {:?}", line);
        count = count + 1;
        if count > 10 {
            break;
        }
    }
    Ok(())
}

/// read multiple files
fn read_and_print4(
    files: &Vec<&str>,
    pattern: &str,
    log_time_format: &str,
    output_file_pattern: &str,
) -> Result<()> {
    let mut readers = files
        .iter()
        .map(|&path| WrappedFileReader::new(path, pattern, false))
        .collect::<Vec<WrappedFileReader>>();

    let mut map: HashMap<String, String> = HashMap::new();
    let mut writer = WrappedFileWriter::new(output_file_pattern, false);

    loop {
        readers.iter_mut().for_each(|reader| {
            let filename = reader.filename();
            if !map.contains_key(&filename) || map[&filename].is_empty() {
                if let Log::Line(line) = reader.next_log() {
                    map.insert(filename, line);
                } else {
                    map.remove(&filename);
                }
            }
        });

        if let Option::Some((key, value)) = map
            .iter()
            .min_by(|(_, v1), (_, v2)| v1.cmp(v2))
            .map(|(k, v)| (k.to_string(), v.to_string()))
        {
            let log_time = NaiveDateTime::parse_from_str(&value[0..23], log_time_format).unwrap(); // TODO: slice time issue, need by variable
            let log_hour = log_time.timestamp() / Duration::hours(1).num_seconds();

            writer.write(log_hour, &value);

            map.remove(&key);
        } else {
            break;
        }
    }

    writer.flush();

    Ok(())
}
