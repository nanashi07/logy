use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use regex::Regex;

fn main() -> std::io::Result<()> {
    // read_and_print();
    read_and_print2()
}

fn read_and_print() -> std::io::Result<()> {
    let file_path = "/Users/nanashi07/Desktop/2021/09/order/source/app.order-67f56b5f67-gn9m5.log";
    let file = File::open(file_path)?;
    let mut buffer_reader = BufReader::new(file);
    let mut line = String::new();
    let mut count = 0;
    while buffer_reader.read_line(&mut line)? > 0 && count < 100 {
        println!("line = {}", line);
        count = count + 1;
    }
    Ok(())
}

fn read_and_print2() -> std::io::Result<()> {
    let file_path = "/Users/nanashi07/Desktop/2021/09/order/source/app.order-67f56b5f67-gn9m5.log";
    let mut file_reader = WrappedFileReader {
        file: file_path.to_string(),
        pattern: Regex::new("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}").unwrap(),
        reader: &mut BufReader::new(File::open(file_path)?),
        buffer: &mut Vec::new(),
    };
    while let Log::Line(line) = file_reader.next_log() {
        println!("line = {:?}", line);
    }
    Ok(())
}

struct WrappedFileReader<'a> {
    file: String,
    pattern: Regex,
    reader: &'a mut BufReader<File>,
    buffer: &'a mut Vec<String>,
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

impl FileNameGetter for WrappedFileReader<'_> {
    fn filename(&self) -> String {
        self.file.clone()
    }
}

impl NextLogLineFinder for WrappedFileReader<'_> {
    fn next_log(&mut self) -> Log {
        let mut line = String::new();
        if self.reader.read_line(&mut line).unwrap() == 0 {
            Log::EOF
        } else {
            if self.pattern.is_match(&line) {
                if self.buffer.is_empty() {
                    self.buffer.push(line);
                    self.next_log()
                } else {
                    // next log, return all of previous lines
                    let cc = self.buffer.join("\n");
                    self.buffer.clear();
                    self.buffer.push(line);
                    Log::Line(cc)
                }
            } else {
                // same log, add to temp and read next line
                self.buffer.push(line);
                self.next_log()
            }
        }
    }
}
