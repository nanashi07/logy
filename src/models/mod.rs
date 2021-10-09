use chrono::{Duration, NaiveDateTime};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use log::info;
use regex::Regex;
use std::{
    cmp::{self, min},
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

#[derive(Debug)]
pub(crate) struct LogDuration {
    pub trace_id: String,
    pub start_time: i64,
    pub end_time: i64,
}

pub(crate) struct WrappedFileWriter {
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
        let (file, appendable) =
            WrappedFileWriter::as_filename(filename_pattern, 0, compress_level);
        let filename = file.as_str();

        info!("create file {}", filename);

        WrappedFileWriter {
            compress_level,
            filename: filename.to_string(),
            pattern: filename_pattern.to_string(),
            empty_content: true,
            writer: WrappedFileWriter::create_writer(filename, appendable, compress_level),
        }
    }

    pub fn write(&mut self, log_hour: i64, line: &str) {
        let (filename, appendable) =
            WrappedFileWriter::as_filename(self.pattern.as_str(), log_hour, self.compress_level);
        if self.filename != filename {
            self.writer.flush().unwrap();

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
            self.writer = WrappedFileWriter::create_writer(
                self.filename.as_str(),
                appendable,
                self.compress_level,
            )
        }
        // self.writer.write_all(line.as_bytes()).unwrap();
        writeln!(self.writer, "{}", line).unwrap();
        self.empty_content = false
    }

    fn as_filename(log_file_pattern: &str, log_hour: i64, compress_level: u32) -> (String, bool) {
        let pattern = log_file_pattern.to_owned() + if compress_level > 0 { ".gz" } else { "" };
        let file_time =
            NaiveDateTime::from_timestamp(log_hour * Duration::hours(1).num_seconds(), 0);
        let new_file = format!("{}", file_time.format(pattern.as_str()));
        (new_file.clone(), new_file == pattern)
    }

    fn create_writer(filename: &str, appendable: bool, compress_level: u32) -> Box<dyn Write> {
        if !Path::new(filename).parent().unwrap().exists() {
            fs::create_dir_all(Path::new(filename).parent().unwrap()).unwrap();
        }

        let file = OpenOptions::new()
            .write(true)
            .append(appendable)
            .truncate(!appendable)
            .create(true)
            .open(filename)
            .unwrap();

        if compress_level > 0 {
            Box::new(GzEncoder::new(
                BufWriter::new(file),
                Compression::new(min(9, compress_level)),
            ))
        } else {
            Box::new(BufWriter::new(file))
        }
    }

    pub fn flush(&mut self) {
        self.writer.flush().unwrap();
    }
}

pub(crate) struct WrappedFileReader {
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

pub(crate) trait FileNameGetter {
    fn filename(&self) -> String;
}

pub(crate) enum Log {
    EOF,
    Line(String),
}
pub(crate) trait NextLogLineFinder {
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
pub(crate) struct LogLine {
    file: String,
    line: String,
}

impl LogLine {
    pub fn new(file: &str, line: &str) -> LogLine {
        LogLine {
            file: file.to_string(),
            line: line.to_string(),
        }
    }
    pub fn filename(&self) -> String {
        self.file.to_string()
    }
    pub fn value(&self) -> String {
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
