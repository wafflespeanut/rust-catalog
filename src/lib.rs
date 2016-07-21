use std::cmp::Ordering::*;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher, SipHasher};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::iter;
use std::str::FromStr;

const SEP: char = '\0';

fn hash<T: Hash>(obj: &T) -> u64 {
    let mut hasher = SipHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug)]
pub struct HashFile {
    name: String,
    file: File,
    hashed: BTreeMap<u64, String>,
    capacity: usize,
    max_row_length: usize,
}

impl HashFile {
    pub fn new(name: &str, capacity: usize, length: Option<usize>) -> Result<HashFile, String> {
        Ok(HashFile {
            name: name.to_owned(),
            file: try!(match length.is_some() {
                true => OpenOptions::new().read(true)
                                          .open(name)
                                          .map_err(|e| format!("Cannot open file! ({})",
                                                               e.description())),
                false => OpenOptions::new().read(true)
                                           .write(true)
                                           .create(true)
                                           .open(name)
                                           .map_err(|e| format!("Cannot create file! ({})",
                                                                e.description())),
            }),
            hashed: BTreeMap::new(),
            capacity: capacity,
            max_row_length: length.unwrap_or(0),
        })
    }

    // NOTE: Run this finally to flush the values (if any) from the struct to the file
    pub fn finish(&mut self) -> Result<usize, String> {
        // DRY'ing...
        fn write_buffer(buf_writer: &mut BufWriter<&mut File>, line: &str, length: usize) -> Result<(), String> {
            let spaces = match line.len() < length {
                true => iter::repeat(SEP).take(length - line.len()).collect::<String>(),
                false => String::new(),
            };

            let line = format!("{}{}\n", line, spaces);
            try!(buf_writer.write(line.as_bytes())
                           .map_err(|e| format!("Cannot write line to buffer! ({})", e.description())));
            try!(buf_writer.flush()
                           .map_err(|e| format!("Cannot flush the buffer to file!({})", e.description())));
            Ok(())
        }

        {
            let buf_reader = BufReader::new(&mut self.file);
            let mut out_file = try!(OpenOptions::new().read(true)
                                                      .write(true)
                                                      .create(true)
                                                      .open(".hash_file")
                                                      .map_err(|e| format!("Cannot create temp file! ({})",
                                                                           e.description())));
            let mut buf_writer = BufWriter::new(&mut out_file);
            let mut file_iter = buf_reader.lines().filter_map(|l| l.ok()).peekable();
            let mut sort_iter = self.hashed.iter().peekable();
            let mut val_1 = file_iter.next();
            let mut val_2 = sort_iter.next();

            while val_1.is_some() && val_2.is_some() {
                let cur_val_1 = val_1.clone().unwrap();
                let k_1 = {
                    let mut split = cur_val_1.split(SEP);
                    split.next().unwrap()
                };

                let hash_1 = hash(&k_1);
                let (hash_2, cur_val_2) = val_2.clone().unwrap();

                match hash_1.cmp(&hash_2) {
                    Equal => {
                        try!(write_buffer(&mut buf_writer, &cur_val_2, self.max_row_length));
                        val_1 = file_iter.next();
                        val_2 = sort_iter.next();
                    },
                    Greater => {
                        try!(write_buffer(&mut buf_writer, &cur_val_2, self.max_row_length));
                        val_2 = sort_iter.next();
                    },
                    Less => {
                        try!(write_buffer(&mut buf_writer, &cur_val_1, self.max_row_length));
                        val_1 = file_iter.next();
                    },
                }
            }

            if let Some(line) = val_1 {
                try!(write_buffer(&mut buf_writer, &line, self.max_row_length));
            }

            for line in file_iter {
                try!(write_buffer(&mut buf_writer, &line, self.max_row_length));
            }

            if let Some((_, line)) = val_2 {
                try!(write_buffer(&mut buf_writer, &line, self.max_row_length));
            }

            for (_, line) in sort_iter {
                try!(write_buffer(&mut buf_writer, &line, self.max_row_length));
            }

        }

        self.hashed.clear();
        try!(fs::rename(".hash_file", &self.name)
                .map_err(|e| format!("Cannot rename the temp file! ({})", e.description())));
        self.file = try!(OpenOptions::new().read(true)
                                           .write(true)
                                           .create(true)
                                           .open(&self.name)
                                           .map_err(|e| format!("Cannot create new file! ({})",
                                                                e.description())));
        Ok(self.max_row_length)
    }

    // NOTE: Type parameters should be explicit so that we don't hash incorrectly
    pub fn insert<K: Display, V: Display + FromStr>(&mut self, key: K, value: V) -> Result<(), String> {
        let hashed = hash(&key.to_string());
        let string = format!("{}{}{}", key, SEP, value);
        if string.len() > self.max_row_length {
            self.max_row_length = string.len();
        }

        self.hashed.insert(hashed, string);

        if self.hashed.len() > self.capacity {  // flush to file once the capacity is full
            try!(self.finish());
        }

        Ok(())
    }

    pub fn get<K: Display, V: FromStr>(&mut self, key: K) -> Result<Option<V>, String> {
        let hashed_key = hash(&key.to_string());
        let size = try!(self.file.metadata()
                                 .map_err(|e| format!("Cannot obtain file metadata ({})", e.description()))
                                 .map(|m| m.len()));
        if size == 0 {
            return Ok(None)
        }

        let row_length = (self.max_row_length + 1) as u64;
        let mut low = 0;
        let mut high = size;

        while low <= high {
            let mid = (low + high) / 2;
            // so that we place the cursor at the start of a line
            let new_line_pos = mid - (mid + row_length) % row_length;
            try!(self.file.seek(SeekFrom::Start(new_line_pos))
                          .map_err(|e| format!("Cannot seek though file! ({})", e.description())));

            let mut reader = BufReader::new(&mut self.file);
            let mut line = String::new();
            try!(reader.read_line(&mut line)
                       .map_err(|e| format!("Cannot read line from file! ({})", e.description())));

            let mut split = line.split(SEP);
            let key = split.next().unwrap();
            let hashed = hash(&key);

            match hashed.cmp(&hashed_key) {
                Equal => {
                    let stripped = split.next().unwrap_or("").trim_right_matches(SEP);
                    let val = try!(stripped.parse::<V>()
                                           .map_err(|_| format!("Cannot parse the value from file!")));
                    return Ok(Some(val))
                },
                Less => {
                    low = mid + 1;
                },
                Greater => {
                    high = mid - 1;
                },
            }
        }

        Ok(None)
    }
}
