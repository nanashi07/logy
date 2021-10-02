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

fn main() -> Result<()> {
    env_logger::init();
    do_reduce_source_log()?;

    info!("task done");
    Ok(())
}

fn do_reduce_source_log() -> Result<()> {
    let source_path = "/Users/nanashi07/Desktop/2021/09/big/real/source";
    let files = if Path::new(source_path).is_dir() {
        fs::read_dir(source_path)?
            .into_iter()
            .map(|p| p.unwrap())
            .map(|p| p.path())
            .filter(|p| p.is_file())
            .filter(|p| p.extension().map(|s| s == "log").unwrap_or(false))
            .map(|p| p.display().to_string())
            .collect::<Vec<String>>()
    } else {
        vec![source_path.to_string()]
    };

    let pattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}";
    let log_time_format = "%Y-%m-%d %H:%M:%S%.3f";
    let output_file_pattern =
        "/Users/nanashi07/Desktop/2021/09/big/real/target/app.realsports.%Y%m%d-%H.log";

    read_and_print5(
        &files.iter().map(|s| s.as_str()).collect(),
        pattern,
        log_time_format,
        output_file_pattern,
    )?;

    Ok(())
}

/// read multiple files and compress output
fn read_and_print5(
    files: &Vec<&str>,
    pattern: &str,
    log_time_format: &str,
    output_file_pattern: &str,
) -> Result<()> {
    let mut readers = files
        .iter()
        .map(|&path| {
            (
                path.to_string(),
                WrappedFileReader::new(path, pattern, false),
            )
        })
        .collect::<HashMap<String, WrappedFileReader>>();

    let mut last_reader: String = String::new();
    let mut map: HashMap<String, String> = HashMap::new();
    let mut writer = WrappedFileWriter::new(output_file_pattern, true);

    // read all head line
    readers.values_mut().for_each(|reader| {
        let filename = reader.filename();
        if !map.contains_key(&filename) || map[&filename].is_empty() {
            if let Log::Line(line) = reader.next_log() {
                map.insert(filename, line);
            } else {
                map.remove(&filename);
            }
        }
    });

    loop {
        if let Option::Some((key, value)) = map
            .iter()
            .min_by(|&(_, v1), &(_, v2)| v1.cmp(v2))
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            let log_time = NaiveDateTime::parse_from_str(&value[0..23], log_time_format).unwrap(); // TODO: slice time issue, need by variable
            let log_hour = log_time.timestamp() / Duration::hours(1).num_seconds();

            writer.write(log_hour, &value);

            // map.remove(&key);
            last_reader = key.to_string();
        } else {
            break;
        }

        // read next
        let reader = readers.get_mut(&last_reader).unwrap();
        let filename = reader.filename();
        if let Log::Line(line) = reader.next_log() {
            map.insert(filename, line);
        } else {
            // read to end of file
            map.remove(&filename);
            // remove reader from list
            readers.remove(&last_reader);
        }
    }

    writer.flush();

    Ok(())
}

fn read_compress_and_group(
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
        let mut writer = WrappedFileWriter::new(output_file_pattern, false);

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

#[derive(Debug)]
struct LogDuration {
    trace_id: String,
    start_time: i64,
    end_time: i64,
}

struct WrappedFileWriter {
    // controls output file is compressed if true
    compressed: bool,
    // last output file name
    filename: String,
    // outout file name pattern
    pattern: String,
    // flag for written content, true when not write operation occurs
    empty_content: bool,
    // output stream
    writer: Box<dyn Write>,
}

impl WrappedFileWriter {
    pub fn new(filename_pattern: &str, compressed: bool) -> WrappedFileWriter {
        let file = WrappedFileWriter::as_filename(filename_pattern, 0, compressed);
        let filename = file.as_str();

        if Path::new(filename).exists() {
            info!("remove file {}", filename);
            fs::remove_file(filename).unwrap();
        }
        info!("create file {}", filename);

        WrappedFileWriter {
            compressed,
            filename: filename.to_string(),
            pattern: filename_pattern.to_string(),
            empty_content: true,
            writer: WrappedFileWriter::create_writer(filename, compressed),
        }
    }

    pub fn write(&mut self, log_hour: i64, line: &str) {
        let filename =
            WrappedFileWriter::as_filename(self.pattern.as_str(), log_hour, self.compressed);
        if self.filename != filename {
            self.writer.flush().unwrap();

            if Path::new(&filename).exists() {
                info!("remove file {}", filename);
                fs::remove_file(&filename).unwrap();
            }

            // check file size and remove zero size file
            let previous_file = self.filename.as_str();
            let previous_path = Path::new(previous_file);
            if previous_path.exists()
                && (previous_path.metadata().unwrap().len() == 0
                    || (self.compressed && self.empty_content))
            {
                info!("remove zero size file: {}", previous_file);
                fs::remove_file(previous_file).unwrap();
            }

            info!("create file {}", filename);
            self.filename = filename;
            self.writer = WrappedFileWriter::create_writer(self.filename.as_str(), self.compressed)
        }
        // self.writer.write_all(line.as_bytes()).unwrap();
        writeln!(self.writer, "{}", line).unwrap();
        self.empty_content = false
    }

    fn as_filename(log_file_pattern: &str, log_hour: i64, compressed: bool) -> String {
        let pattern = log_file_pattern.to_owned() + if compressed { ".gz" } else { "" };
        let file_time =
            NaiveDateTime::from_timestamp(log_hour * Duration::hours(1).num_seconds(), 0);
        format!("{}", file_time.format(pattern.as_str()))
    }

    fn create_writer(filename: &str, compressed: bool) -> Box<dyn Write> {
        if !Path::new(filename).parent().unwrap().exists() {
            fs::create_dir_all(Path::new(filename).parent().unwrap()).unwrap();
        }

        if compressed {
            Box::new(GzEncoder::new(
                BufWriter::new(File::create(filename).unwrap()),
                Compression::best(),
            ))
        } else {
            Box::new(BufWriter::new(File::create(filename).unwrap()))
        }
    }

    fn flush(&mut self) {
        self.writer.flush().unwrap();
    }
}

struct WrappedFileReader {
    file: String,
    pattern: Regex,
    reader: Box<dyn BufRead>,
    buffer: Box<Vec<String>>,
}

impl WrappedFileReader {
    pub fn new(file: &str, pattern: &str, compressed: bool) -> WrappedFileReader {
        WrappedFileReader {
            file: file.to_string(),
            pattern: Regex::new(pattern).unwrap(),
            reader: if compressed {
                Box::new(BufReader::new(GzDecoder::new(BufReader::new(
                    File::open(file).unwrap(),
                ))))
            } else {
                Box::new(BufReader::new(File::open(file).unwrap()))
            },
            buffer: Box::new(Vec::new()),
        }
    }
}

trait FileNameGetter {
    fn filename(&self) -> String;
}

enum Log {
    EOF,
    Line(String),
}
trait NextLogLineFinder {
    fn next_log(&mut self) -> Log;
}

impl FileNameGetter for WrappedFileReader {
    fn filename(&self) -> String {
        self.file.clone()
    }
}

impl NextLogLineFinder for WrappedFileReader {
    fn next_log(&mut self) -> Log {
        let mut line = String::new();
        if self.reader.read_line(&mut line).unwrap() == 0 {
            Log::EOF
        } else {
            // remove line break at the end
            line = line.trim_end().to_string();
            if self.pattern.is_match(&line) {
                if self.buffer.is_empty() {
                    self.buffer.push(line);
                    self.next_log()
                } else {
                    // next log, return all of previous lines
                    let full_log = self.buffer.join("\n");
                    self.buffer.clear();
                    self.buffer.push(line);
                    Log::Line(full_log)
                }
            } else {
                // same log, add to temp and read next line
                self.buffer.push(line);
                self.next_log()
            }
        }
    }
}
