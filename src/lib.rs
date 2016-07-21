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
            let padding = match line.len() < length {
                true => iter::repeat(SEP).take(length - line.len()).collect::<String>(),
                false => String::new(),
            };

            let line = format!("{}{}\n", line, padding);
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
            let mut file_iter = buf_reader.lines().filter_map(|l| l.ok());
            let mut sort_iter = self.hashed.iter();
            let mut file_val = file_iter.next();
            let mut btree_val = sort_iter.next();

            while let Some((file_line,
                            btree_line_hash,
                            btree_line)) = file_val.clone()
                                                   .and_then(|v| btree_val.clone()
                                                                          .map(|(h, val)| (v, h, val))) {
                let file_line_hash = {
                    let mut split = file_line.split(SEP);
                    hash(&split.next().unwrap())
                };

                if file_line_hash == *btree_line_hash {
                    try!(write_buffer(&mut buf_writer, &btree_line, self.max_row_length));
                    file_val = file_iter.next();
                    btree_val = sort_iter.next();
                } else if file_line_hash < *btree_line_hash {
                    try!(write_buffer(&mut buf_writer, &file_line, self.max_row_length));
                    file_val = file_iter.next();
                } else {
                    try!(write_buffer(&mut buf_writer, &btree_line, self.max_row_length));
                    btree_val = sort_iter.next();
                }
            }

            if let Some(line) = file_val {
                try!(write_buffer(&mut buf_writer, &line, self.max_row_length));
            }

            for line in file_iter {
                try!(write_buffer(&mut buf_writer, &line, self.max_row_length));
            }

            if let Some((_, line)) = btree_val {
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

        // Binary search and file seeking to find the value(s)

        while low <= high {
            let mid = (low + high) / 2;
            // place the cursor at the start of a line
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

            if hashed == hashed_key {
                let stripped = split.next().unwrap_or("").trim_right_matches(SEP);
                let val = try!(stripped.parse::<V>()
                                       .map_err(|_| format!("Cannot parse the value from file!")));
                return Ok(Some(val))
            } else if hashed < hashed_key {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        Ok(None)
    }
}
