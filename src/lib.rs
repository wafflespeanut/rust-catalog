use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher, SipHasher};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::iter;
use std::mem;
use std::ops::AddAssign;
use std::str::FromStr;

const SEP: char = '\0';

fn hash<T: Hash>(obj: &T) -> u64 {
    let mut hasher = SipHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

fn write_buffer(buf_writer: &mut BufWriter<&mut File>,
                line: &str, pad_length: &mut usize) -> Result<(), String> {
    let padding = if line.len() < *pad_length {
        iter::repeat(SEP).take(*pad_length - line.len()).collect::<String>()
    } else {
        if line.len() > *pad_length {
            *pad_length = line.len();   // change pad length if we get a giant string
        }

        String::new()
    };

    let line = format!("{}{}\n", line, padding);
    try!(buf_writer.write(line.as_bytes())
                   .map_err(|e| format!("Cannot write line to buffer! ({})", e.description())));
    try!(buf_writer.flush()
                   .map_err(|e| format!("Cannot flush the buffer to file!({})", e.description())));
    Ok(())
}

struct KeyValue<K: Display + FromStr + Hash, V: Display + FromStr> {
    key: K,
    value: V,
    count: usize,
}

impl<K: Display + FromStr + Hash, V: Display + FromStr> KeyValue<K, V> {
    pub fn new(key: K, val: V) -> KeyValue<K, V> {
        KeyValue {
            key: key,
            value: val,
            count: 0,
        }
    }
}

impl<K: Display + FromStr + Hash, V: Display + FromStr> Display for KeyValue<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}{}{}{}", self.key, SEP, self.value, SEP, self.count)
    }
}

impl<K: Display + FromStr + Hash, V: Display + FromStr> FromStr for KeyValue<K, V> {
    type Err = String;

    fn from_str(s: &str) -> Result<KeyValue<K, V>, String> {
        let mut split = s.split(SEP);
        Ok(KeyValue {
            key: try!(split.next().unwrap_or("")
                                  .parse::<K>()
                                  .map_err(|_| format!("Cannot parse the key!"))),
            value: try!(split.next().unwrap_or("")
                                    .parse::<V>()
                                    .map_err(|_| format!("Cannot parse the value!"))),
            count: try!(split.next().unwrap_or("")
                                    .parse::<usize>()
                                    .map_err(|_| format!("Cannot parse 'overwritten' count"))),
        })
    }
}

impl<K: Display + FromStr + Hash, V: Display + FromStr> AddAssign for KeyValue<K, V> {
    fn add_assign(&mut self, other: KeyValue<K, V>) {
        self.value = other.value;
        self.count += 1;
    }
}

impl<K: Display + FromStr + Hash, V: Display + FromStr> Hash for KeyValue<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

pub struct HashFile<K: Display + FromStr + Hash, V: Display + FromStr> {
    name: String,
    file: File,
    hashed: BTreeMap<u64, KeyValue<K, V>>,
    capacity: usize,
    max_row_length: usize,
}

impl<K: Display + FromStr + Hash, V: Display + FromStr> HashFile<K, V> {
    pub fn new(name: &str, capacity: usize, length: Option<usize>) -> Result<HashFile<K, V>, String> {
        if length.is_none() {
            let _ = fs::remove_file(name);
        }

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

    /// Run this finally to flush the values (if any) from the struct to the file
    pub fn finish(&mut self) -> Result<usize, String> {
        if self.hashed.len() > 0 {
            try!(self.flush_map());
        }

        // FIXME: any way around this?
        // We need to traverse one last time to confirm that each row has the same length

        {
            let buf_reader = BufReader::new(&mut self.file);
            let mut out_file = try!(OpenOptions::new().read(true)
                                                      .write(true)
                                                      .create(true)
                                                      .open(".hash_file")
                                                      .map_err(|e| format!("Cannot create temp file! ({})",
                                                                           e.description())));
            let mut buf_writer = BufWriter::new(&mut out_file);

            for line in buf_reader.lines().filter_map(|l| l.ok()) {
                // Even though this takes a mutable reference, we can be certain that
                // we've found the maximum row length for this session
                try!(write_buffer(&mut buf_writer, &line, &mut self.max_row_length));
            }
        }

        try!(fs::rename(".hash_file", &self.name)
                .map_err(|e| format!("Cannot rename the temp file! ({})", e.description())));
        self.file = try!(OpenOptions::new().read(true)
                                           .write(true)
                                           .open(&self.name)
                                           .map_err(|e| format!("Cannot open the working file! ({})",
                                                                e.description())));
        Ok(self.max_row_length)
    }

    fn flush_map(&mut self) -> Result<(), String> {
        let map = mem::replace(&mut self.hashed, BTreeMap::new());

        {
            let buf_reader = BufReader::new(&mut self.file);
            let mut out_file = try!(OpenOptions::new().read(true)
                                                      .write(true)
                                                      .create(true)
                                                      .open(".hash_file")
                                                      .map_err(|e| format!("Cannot create temp file! ({})",
                                                                           e.description())));
            let mut buf_writer = BufWriter::new(&mut out_file);

            // both the iterators throw the values in ascending order
            let mut file_iter = buf_reader.lines().filter_map(|l| l.ok()).peekable();
            let mut map_iter = map.into_iter().peekable();

            loop {
                let compare_result = match (file_iter.peek(), map_iter.peek()) {
                    (Some(file_line), Some(&(ref btree_key_hash, _))) => {
                        let key = file_line.split(SEP).next().unwrap();
                        let file_key_hash = match key.parse::<K>() {
                            Ok(k_v) => hash(&k_v),
                            Err(_) => {
                                // skip the line if we find any errors
                                // (we don't wanna stop the giant build because of this error)
                                continue
                            },
                        };

                        file_key_hash.cmp(btree_key_hash)
                    },
                    (Some(_), None) => Ordering::Less,
                    (None, Some(&(_, _))) => Ordering::Greater,
                    (None, None) => break,
                };

                match compare_result {
                    Ordering::Equal => {
                        let file_line = file_iter.next().unwrap();
                        let (_, btree_key_val) = map_iter.next().unwrap();
                        let mut file_key_val = match file_line.parse::<KeyValue<K, V>>() {
                            Ok(k_v) => k_v,
                            Err(_) => continue,     // skip on error
                        };

                        file_key_val += btree_key_val;
                        try!(write_buffer(&mut buf_writer, &file_key_val.to_string(),
                                          &mut self.max_row_length));
                    },
                    Ordering::Less => {
                        try!(write_buffer(&mut buf_writer, &file_iter.next().unwrap(),
                                          &mut self.max_row_length));
                    },
                    Ordering::Greater => {
                        let (_, btree_key_val) = map_iter.next().unwrap();
                        try!(write_buffer(&mut buf_writer, &(btree_key_val.to_string()),
                                          &mut self.max_row_length));
                    },
                }
            }
        }

        try!(fs::rename(".hash_file", &self.name)
                .map_err(|e| format!("Cannot rename the temp file! ({})", e.description())));
        self.file = try!(OpenOptions::new().read(true)
                                           .write(true)
                                           .open(&self.name)
                                           .map_err(|e| format!("Cannot open the working file! ({})",
                                                                e.description())));
        Ok(())
    }

    // NOTE: Type parameters should be explicit so that we don't hash incorrectly
    pub fn insert(&mut self, key: K, value: V) -> Result<(), String> {
        let key_val = KeyValue::new(key, value);
        let hashed = hash(&key_val);
        if let Some(k_v) = self.hashed.get_mut(&hashed) {
            *k_v += key_val;
            return Ok(())
        }

        self.hashed.insert(hashed, key_val);
        if self.hashed.len() > self.capacity {  // flush to file once the capacity is full
            try!(self.flush_map());
        }

        Ok(())
    }

    pub fn get(&mut self, key: K) -> Result<Option<(V, usize)>, String> {
        let hashed_key = hash(&key);
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

            // we'll only need the hash of the key
            let mut split = line.split(SEP);
            let key_str = split.next().unwrap();
            let key = try!(key_str.parse::<K>()
                                  .map_err(|_| format!("Cannot parse the key from file!")));
            let hashed = hash(&key);

            if hashed == hashed_key {
                let key_val = try!(line.trim_right().parse::<KeyValue<K, V>>());
                return Ok(Some((key_val.value, key_val.count)))
            } else if hashed < hashed_key {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        Ok(None)
    }
}
