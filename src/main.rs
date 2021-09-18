use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

use regex::Regex;

fn main() -> std::io::Result<()> {
    let files = vec![
        "/Users/nanashi07/Desktop/2021/09/mq-slow/source/app.real-sports-game-internal-7bc8549c5-cg8jj.log",
        "/Users/nanashi07/Desktop/2021/09/mq-slow/source/app.real-sports-game-internal-7bc8549c5-cw58m.log"
    ];
    let pattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}";
    read_and_print(&files)?;
    println!("======================================================================================================");
    read_and_print2(&files, pattern)?;
    println!("======================================================================================================");
    read_and_print3(&files, pattern)?;
    println!("======================================================================================================");
    read_and_print4(&files, pattern)?;
    println!("======================================================================================================");
    Ok(())
}

/// read files by directly buffer reader
fn read_and_print(files: &Vec<&str>) -> std::io::Result<()> {
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
fn read_and_print2(files: &Vec<&str>, pattern: &str) -> std::io::Result<()> {
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
fn read_and_print3(files: &Vec<&str>, pattern: &str) -> std::io::Result<()> {
    let file = files[0];
    let mut file_reader = WrappedFileReader::new(String::from(file), pattern.to_string());
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
fn read_and_print4(files: &Vec<&str>, pattern: &str) -> std::io::Result<()> {
    let mut readers = files
        .iter()
        .map(|&path| WrappedFileReader::new(path.to_string(), pattern.to_string()))
        .collect::<Vec<WrappedFileReader>>();

    let mut map: HashMap<String, String> = HashMap::new();
    // let regex = Regex::new(pattern);

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
            println!("{}", pair.value);
            map.remove(&pair.key);
        }
    }

    Ok(())
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

struct WrappedFileReader {
    file: String,
    pattern: Regex,
    reader: Box<BufReader<File>>,
    buffer: Box<Vec<String>>,
}

impl WrappedFileReader {
    pub fn new(file: String, pattern: String) -> WrappedFileReader {
        WrappedFileReader {
            file: file.clone(),
            pattern: Regex::new(pattern.as_str()).unwrap(),
            reader: Box::new(BufReader::new(File::open(file.as_str()).unwrap())),
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
