use std::cmp::Ordering::*;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher, SipHasher};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::iter;
use std::str::FromStr;

const SEP: char = '\t';
const PAD_CHAR: char = ' ';

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
    pub fn new(name: &str, capacity: usize, length: Option<usize>) -> HashFile {
        HashFile {
            name: name.to_owned(),
            file: match length.is_some() {
                true => OpenOptions::new().read(true).open(name).unwrap(),
                false => OpenOptions::new().read(true).write(true).create(true).open(name).unwrap()
            },
            hashed: BTreeMap::new(),
            capacity: capacity,
            max_row_length: match length {
                Some(l) => l,
                None => 0,
            },
        }
    }

    // NOTE: Run this finally to flush the values (if any) from the struct to the file
    pub fn finish(&mut self) {
        println!("Flushing... (row length: {})", self.max_row_length);

        // DRY'ing...
        fn write_buffer(buf_writer: &mut BufWriter<&mut File>, line: &str, length: usize) {
            let spaces = match line.len() < length {
                true => iter::repeat(PAD_CHAR).take(length - line.len()).collect::<String>(),
                false => String::new(),
            };

            let line = format!("{}{}\n", line, spaces);
            let _ = buf_writer.write(line.as_bytes());
            let _ = buf_writer.flush();
        }

        {
            let buf_reader = BufReader::new(&mut self.file);
            let mut out_file = OpenOptions::new().read(true)
                                                 .write(true)
                                                 .create(true)
                                                 .open(".hash_file")
                                                 .unwrap();
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
                        write_buffer(&mut buf_writer, &cur_val_2, self.max_row_length);
                        val_1 = file_iter.next();
                        val_2 = sort_iter.next();
                    },
                    Greater => {
                        write_buffer(&mut buf_writer, &cur_val_2, self.max_row_length);
                        val_2 = sort_iter.next();
                    },
                    Less => {
                        write_buffer(&mut buf_writer, &cur_val_1, self.max_row_length);
                        val_1 = file_iter.next();
                    },
                }
            }

            if let Some(line) = val_1 {
                write_buffer(&mut buf_writer, &line, self.max_row_length);
            }

            for line in file_iter {
                write_buffer(&mut buf_writer, &line, self.max_row_length);
            }

            if let Some((_, line)) = val_2 {
                write_buffer(&mut buf_writer, &line, self.max_row_length);
            }

            for (_, line) in sort_iter {
                write_buffer(&mut buf_writer, &line, self.max_row_length);
            }

        }

        self.hashed.clear();
        let _ = fs::rename(".hash_file", &self.name);
        self.file = OpenOptions::new().read(true)
                                .write(true)
                                .create(true)
                                .open(&self.name)
                                .unwrap();
    }

    // FIXME: Too many unwraps and somewhat inefficient!
    // NOTE: Type parameters should be explicit so that we don't hash incorrectly
    pub fn insert<K: Display, V: Display + FromStr>(&mut self, key: K, value: V) {
        let hashed = hash(&key.to_string());
        let string = format!("{}{}{}", key, SEP, value);
        if string.len() > self.max_row_length {
            self.max_row_length = string.len();
        }

        self.hashed.insert(hashed, string);

        if self.hashed.len() > self.capacity {  // flush to file once the capacity is full
            self.finish();
        }
    }

    pub fn get<K: Display, V: FromStr>(&mut self, key: K) -> Option<V> {
        let mut seeks = 0;
        let hashed_key = hash(&key.to_string());
        let timer = ::std::time::Instant::now();
        let size = self.file.metadata().unwrap().len();
        println!("file size: {}", size);
        if size == 0 {
            return None
        }

        let row_length = (self.max_row_length + 1) as u64;
        println!("row length: {:?}", row_length);
        let mut low = 0;
        let mut high = size;

        while low <= high {
            let mid = (low + high) / 2;
            println!("\n{:?}", (low, high, mid));
            // so that we place the cursor at the start of a line
            let new_line_pos = mid - (mid + row_length) % row_length;

            let mut line = String::new();
            let _ = self.file.seek(SeekFrom::Start(new_line_pos));
            println!("Seeking to {:?}", new_line_pos);
            seeks += 1;

            let mut reader = BufReader::new(&mut self.file);
            let _ = reader.read_line(&mut line);
            println!("Got {:?}", line);

            let mut split = line.split(SEP);
            let key = split.next().unwrap();
            let hashed = hash(&key);

            println!("Comparing {} vs {} in range({}, {}, {})", hashed, hashed_key, low, mid, high);

            match hashed.cmp(&hashed_key) {
                Equal => {
                    let val = split.next().unwrap().parse::<V>().ok().unwrap();
                    println!("Time taken: {:?}", timer.elapsed());
                    println!("Seeks: {:?}", seeks);
                    return Some(val)
                },
                Less => {
                    low = mid + 1;
                },
                Greater => {
                    high = mid - 1;
                },
            }
        }

        println!("Time taken: {:?}", timer.elapsed());
        println!("Seeks: {:?}", seeks);
        None
    }
}
