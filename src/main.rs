use std::{
    cmp::{self},
    collections::HashMap,
    env,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Result, Write},
    path::Path,
};

use chrono::{Duration, NaiveDateTime};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use regex::Regex;

fn main() -> Result<()> {
    let files = vec![
        "/Users/nanashi07/Desktop/2021/09/mq-slow/source/app.real-sports-game-internal-7bc8549c5-cg8jj.log",
        "/Users/nanashi07/Desktop/2021/09/mq-slow/source/app.real-sports-game-internal-7bc8549c5-cw58m.log"
    ];
    let pattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}";
    let log_time_format = "%Y-%m-%d %H:%M:%S%.3f";
    let trace_pattern = "\\[real-sports-game-internal-\\w+-\\w+,(\\w+,\\w+)\\]";
    let output_file_pattern = "app.%Y%m%d-%H.log";
    let group_file_pattern = "group.%Y%m%d-%H.log";

    read_and_print(&files)?;
    println!("======================================================================================================");
    read_and_print2(&files, pattern)?;
    println!("======================================================================================================");
    read_and_print3(&files, pattern)?;
    println!("======================================================================================================");
    read_and_print4(&files, pattern, log_time_format, output_file_pattern)?;
    println!("======================================================================================================");
    read_and_print5(&files, pattern, log_time_format, output_file_pattern)?;
    println!("======================================================================================================");

    let path = Path::new("/etc/resolv.conf");

    assert!(path.ends_with("resolv.conf"));
    assert!(path.ends_with("etc/resolv.conf"));
    assert!(path.ends_with("/etc/resolv.conf"));

    assert!(!path.ends_with("/resolv.conf"));

    let running_path = env::current_dir()?;
    let sorted_logs = fs::read_dir(running_path)?
        .into_iter()
        .map(|p| p.unwrap())
        .map(|p| p.path())
        .filter(|p| p.is_file())
        .filter(|p| p.extension().map(|s| s == "gz").unwrap_or(false))
        .map(|p| p.display().to_string())
        .collect::<Vec<String>>();
    // println!("{:?}", sorted_logs);
    let logs = sorted_logs.iter().map(|s| s.as_str()).collect();
    read_compress_and_group(
        &logs,
        pattern,
        log_time_format,
        trace_pattern,
        group_file_pattern,
    )?;
    println!("======================================================================================================");

    Ok(())
}

/// read files by directly buffer reader
fn read_and_print(files: &Vec<&str>) -> Result<()> {
    let file = File::open(files[0])?;
    let mut buffer_reader = BufReader::new(file);
    let mut line = String::new();
    let mut count = 0;
    while buffer_reader.read_line(&mut line)? > 0 && count < 100 {
        println!("read_and_print = {}", line);
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
        println!("read_and_print2 = {:?}", line);
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
        println!("read_and_print3 = {:?}", line);
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

        if map.is_empty() {
            break;
        } else {
            let pair = map
                .iter()
                .map(|e| Pair::new(e.0, e.1)) // create new ref object to avoid borrow from
                .min_by(|a, b| a.value.cmp(&b.value))
                .unwrap();

            let log_time =
                NaiveDateTime::parse_from_str(&pair.value[0..23], log_time_format).unwrap(); // TODO: slice time issue, need by variable
            let log_hour = log_time.timestamp() / Duration::hours(1).num_seconds();

            writer.write(log_hour, &pair.value);

            map.remove(&pair.key);
        }
    }

    writer.flush();

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
        .map(|&path| WrappedFileReader::new(path, pattern, false))
        .collect::<Vec<WrappedFileReader>>();

    let mut map: HashMap<String, String> = HashMap::new();
    let mut writer = WrappedFileWriter::new(output_file_pattern, true);

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

        if map.is_empty() {
            break;
        } else {
            let pair = map
                .iter()
                .map(|e| Pair::new(e.0, e.1)) // create new ref object to avoid borrow from
                .min_by(|a, b| a.value.cmp(&b.value))
                .unwrap();

            let log_time =
                NaiveDateTime::parse_from_str(&pair.value[0..23], log_time_format).unwrap(); // TODO: slice time issue, need by variable
            let log_hour = log_time.timestamp() / Duration::hours(1).num_seconds();

            writer.write(log_hour, &pair.value);

            map.remove(&pair.key);
        }
    }

    writer.flush();

    Ok(())
}

fn read_compress_and_group(
    files: &Vec<&str>,
    pattern: &str,
    log_time_format: &str,
    trace_pattern: &str,
    output_file_pattern: &str,
) -> Result<()> {
    let mut map: HashMap<String, LogDuration> = HashMap::new();
    let re = Regex::new(trace_pattern).unwrap();

    for &file in files {
        println!("Load file {}", file);
        let mut reader = WrappedFileReader::new(file, pattern, true);
        while let Log::Line(line) = reader.next_log() {
            match re.captures(line.as_str()) {
                Some(captures) => {
                    let trace_id = captures.get(1).map_or("", |m| m.as_str()).to_string();
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

        let filtered = map
            .values()
            .filter(|&d| d.end_time - d.start_time > 1000)
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

        for duration in filtered.values() {
            println!(
                "{:?}, {} ~ {}",
                duration.trace_id,
                NaiveDateTime::from_timestamp(
                    duration.start_time / 1000,
                    (duration.start_time % 1000 * 1_000_000) as u32
                ),
                NaiveDateTime::from_timestamp(
                    duration.end_time / 1000,
                    (duration.end_time % 1000 * 1_000_000) as u32
                )
            );
        }

        let mut grouped_logs: HashMap<String, Vec<String>> = HashMap::new();
        reader = WrappedFileReader::new(file, pattern, true);
        let mut writer = WrappedFileWriter::new(output_file_pattern, false);

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
    }

    Ok(())
}

#[derive(Debug)]
struct LogDuration {
    trace_id: String,
    start_time: i64,
    end_time: i64,
}

struct Pair {
    key: String,
    value: String,
}

impl Pair {
    pub fn new(key: &String, value: &String) -> Pair {
        Pair {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
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
            println!("remove file {}", filename);
            fs::remove_file(filename).unwrap();
        }
        println!("create file {}", filename);

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
                println!("remove file {}", filename);
                fs::remove_file(&filename).unwrap();
            }

            // check file size and remove zero size file
            let previous_file = self.filename.as_str();
            let previous_path = Path::new(previous_file);
            if previous_path.exists()
                && (previous_path.metadata().unwrap().len() == 0
                    || (self.compressed && self.empty_content))
            {
                println!("remove zero size file: {}", previous_file);
                fs::remove_file(previous_file).unwrap();
            }

            println!("create file {}", filename);
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
