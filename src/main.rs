use chrono::{Duration, NaiveDateTime};
use clap::{App, Arg, ArgMatches, SubCommand};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use log::{debug, error, info};
use regex::Regex;
use std::{
    cmp::{self, min},
    collections::{BTreeSet, HashMap},
    fmt::Display,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Result, Write},
    path::Path,
    sync::mpsc::{self},
    thread, vec,
};

fn main() -> Result<()> {
    let app = command_args();

    env_logger::init();

    if let Some(args) = app.subcommand_matches("reduce") {
        if let Some(files) = args.values_of("files") {
            reduce_logs(
                &files.collect(),
                args.value_of("prefix").unwrap(),
                args.value_of("log-time-format").unwrap(),
                args.value_of("out-file-pattern").unwrap(),
                args.value_of("compress-level")
                    .unwrap()
                    .parse::<u32>()
                    .unwrap(),
            )?;
        } else {
            error!("No source file provided");
            return Ok(());
        }
        info!("task done");
    } else if let Some(args) = app.subcommand_matches("long") {
        // TODO
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
        .get_matches()
}

/// read multiple files and compress output
fn reduce_logs(
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
            if let Some((filename, line)) = sorted_set
                .iter()
                .next()
                .map(|v| (v.filename().to_string(), v.value().to_string()))
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

#[derive(Debug)]
struct LogDuration {
    trace_id: String,
    start_time: i64,
    end_time: i64,
}

struct WrappedFileWriter {
    // controls output file is compressed, value is from 0 to 9
    compress_level: u32,
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
    pub fn new(filename_pattern: &str, compress_level: u32) -> WrappedFileWriter {
        let file = WrappedFileWriter::as_filename(filename_pattern, 0, compress_level);
        let filename = file.as_str();

        if Path::new(filename).exists() {
            info!("remove file {}", filename);
            fs::remove_file(filename).unwrap();
        }
        info!("create file {}", filename);

        WrappedFileWriter {
            compress_level,
            filename: filename.to_string(),
            pattern: filename_pattern.to_string(),
            empty_content: true,
            writer: WrappedFileWriter::create_writer(filename, compress_level),
        }
    }

    pub fn write(&mut self, log_hour: i64, line: &str) {
        let filename =
            WrappedFileWriter::as_filename(self.pattern.as_str(), log_hour, self.compress_level);
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
                    || (self.compress_level > 0 && self.empty_content))
            {
                info!("remove zero size file: {}", previous_file);
                fs::remove_file(previous_file).unwrap();
            }

            info!("create file {}", filename);
            self.filename = filename;
            self.writer =
                WrappedFileWriter::create_writer(self.filename.as_str(), self.compress_level)
        }
        // self.writer.write_all(line.as_bytes()).unwrap();
        writeln!(self.writer, "{}", line).unwrap();
        self.empty_content = false
    }

    fn as_filename(log_file_pattern: &str, log_hour: i64, compress_level: u32) -> String {
        let pattern = log_file_pattern.to_owned() + if compress_level > 0 { ".gz" } else { "" };
        let file_time =
            NaiveDateTime::from_timestamp(log_hour * Duration::hours(1).num_seconds(), 0);
        format!("{}", file_time.format(pattern.as_str()))
    }

    fn create_writer(filename: &str, compress_level: u32) -> Box<dyn Write> {
        if !Path::new(filename).parent().unwrap().exists() {
            fs::create_dir_all(Path::new(filename).parent().unwrap()).unwrap();
        }

        if compress_level > 0 {
            Box::new(GzEncoder::new(
                BufWriter::new(File::create(filename).unwrap()),
                Compression::new(min(9, compress_level)),
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

#[derive(Eq)]
struct LogLine {
    file: String,
    line: String,
}

impl LogLine {
    fn new(file: &str, line: &str) -> LogLine {
        LogLine {
            file: file.to_string(),
            line: line.to_string(),
        }
    }
    fn filename(&self) -> String {
        self.file.to_string()
    }
    fn value(&self) -> String {
        self.line.to_string()
    }
}

impl Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.line)
    }
}

impl Ord for LogLine {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.line.cmp(&other.line)
    }
}

impl PartialOrd for LogLine {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for LogLine {
    fn eq(&self, other: &Self) -> bool {
        self.line == other.line
    }
}
