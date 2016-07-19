use std::cmp::Ordering::*;
use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher, SipHasher};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::iter;
use std::str::FromStr;
use std::u64;

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
    min_value: u64,
    file: File,
    hashed: Vec<u64>,
    key_vals: Vec<String>,
    capacity: usize,
    max_row_length: usize,
}

impl HashFile {
    pub fn new(name: &str, capacity: usize, length: Option<usize>) -> HashFile {
        HashFile {
            name: name.to_owned(),
            min_value: u64::MAX,
            file: match length.is_some() {
                true => OpenOptions::new().read(true).open(name).unwrap(),
                false => OpenOptions::new().read(true).write(true).create(true).open(name).unwrap()
            },
            hashed: Vec::with_capacity(capacity),
            key_vals: Vec::with_capacity(capacity),
            capacity: capacity,
            max_row_length: match length {
                Some(l) => l,
                None => 0,
            },
        }
    }

    // NOTE: Run this finally to flush the values (if any) from the struct to the file
    pub fn finish<K: Hash + FromStr>(&mut self) {
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
            let mut sort_iter = self.hashed.drain(..).zip(self.key_vals.drain(..)).peekable();
            let mut val_1 = file_iter.next();
            let mut val_2 = sort_iter.next();

            while val_1.is_some() && val_2.is_some() {
                let cur_val_1 = val_1.clone().unwrap();
                let k_1 = {
                    let mut split = cur_val_1.split(SEP);
                    let key = split.next().unwrap();
                    key.parse::<K>().ok().unwrap()
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

        let _ = fs::rename(".hash_file", &self.name);
        self.file = OpenOptions::new().read(true)
                                .write(true)
                                .create(true)
                                .open(&self.name)
                                .unwrap();
    }

    // FIXME: Too many unwraps and somewhat inefficient!
    // NOTE: Type parameters should be explicit so that we don't hash incorrectly
    pub fn insert<K: Display + Hash + FromStr, V: Display + FromStr>(&mut self, key: K, value: V) {
        let hashed = hash(&key);
        if hashed < self.min_value {
            self.min_value = hashed;
        }

        let string = format!("{}{}{}", key, SEP, value);
        if string.len() > self.max_row_length {
            self.max_row_length = string.len();
        }

        if self.hashed.is_empty() {
            self.hashed.push(hashed);
            self.key_vals.push(string);
            return
        }

        // Binary search

        let mut low = 0;
        let mut high = self.hashed.len() - 1;

        while low <= high {
            let mid = (high + low) / 2;
            match self.hashed[mid].cmp(&hashed) {
                Equal => {
                    let mut old_val = self.key_vals.get_mut(mid).unwrap();
                    *old_val = string;
                    return
                },
                Greater => {
                    if mid == 0 {
                        low = 0;
                        break
                    } else {
                        high = mid - 1;
                    }
                },
                Less => {
                    low = mid + 1;
                },
            }
        }

        self.hashed.insert(low, hashed);
        self.key_vals.insert(low, string);

        // Drain, merge the sorted collections, and write to file once the capacity is full

        if self.hashed.len() > self.capacity {
            self.finish::<K>();
        }
    }

    pub fn get<K: Display + Hash + FromStr, V: Display + FromStr>(&mut self, key: K) -> Option<V> {
        let hashed_key = hash(&key);
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
            let mut reader = BufReader::new(&mut self.file);
            let _ = reader.read_line(&mut line);
            println!("Got {:?}", line);

            let mut split = line.split(SEP);
            let key = split.next().unwrap().parse::<K>().ok().unwrap();
            let hashed = hash(&key);

            println!("Comparing {} vs {} in range({}, {}, {})", hashed, hashed_key, low, mid, high);

            match hashed.cmp(&hashed_key) {
                Equal => {
                    let val = split.next().unwrap().parse::<V>().ok().unwrap();
                    println!("Time taken: {:?}", timer.elapsed());
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
        None
    }
}
