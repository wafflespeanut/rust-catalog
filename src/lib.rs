//! The [`HashMap`][hash-map] and [`BTreeMap`][btree-map] in the standard library
//! offer very good performance when it comes to inserting and getting stuff,
//! but they're memory killers. If the "stuff" gets large - say, a trillion
//! (10<sup>12</sup>) of them, then we're gonna be in trouble, as we'll then
//! be needing gigs of RAM to hold the data.
//!
//! Moreover, once the program quits, all the *hard-earned* stuff gets deallocated,
//! and we'd have to re-insert them allover again. `HashFile` deals with this specific
//! problem. It makes use of a `BTreeMap` for storing the keys and values. So, until
//! it reaches the defined capacity, it offers the same performance as that of the
//! btree-map. However, once (and whenever) it reaches the capacity, it *flushes*
//! the stuff to a file (both the parameters can be defined in its methods).
//!
//! Hence, at any given moment, the upper limit for the memory eaten by this thing
//! is set by its [capacity][capacity]. This gives us good control over the space-time
//! trade-off. But, the flushing will take O(2<sup>n</sup>) time, depending on the
//! processor and I/O speed, as it does things on the fly with the help of iterators.
//!
//! After the [final manual flush][finish], the file can be stored, moved around, and
//! since it makes use of binary search, values can be obtained in O(log-n) time
//! whenever required (depending on the seeking speed of the drive). For example,
//! a seek takes around 0.03 ms, and a file containing a trillion values demands
//! about 40 seeks (in the worse case), which translates to 1.2 ms.
//!
//! [*See the `HashFile` type for more info.*][hash-file]
//!
//! [hash-map]: https://doc.rust-lang.org/std/collections/struct.HashMap.html
//! [btree-map]: https://doc.rust-lang.org/std/collections/struct.BTreeMap.html
//! [finish]: struct.HashFile.html#method.finish
//! [capacity]: struct.HashFile.html#method.set_capacity
//! [hash-file]: struct.HashFile.html
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
use std::path::Path;
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

// FIXME: This should be changed to serialization
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

/// An implementation of a "file-based" map which stores key-value pairs in
/// sorted fashion in a file, and gets them using binary search and file seeking in
/// O(log-n) time.
///
/// During insertion, the hash of the supplied key is obtained (using the built-in
/// [`SipHasher`][hasher]), which acts as the key for sorting. While flushing, two
/// iterators (one for the maintained map, and the other for the file) throw the
/// key-value pairs in ascending order. The hashes of the pairs are compared and
/// written to a temporary file, and finally the file is renamed to the original file.
///
/// Basically, the file is a CSV format with keys and values separated by a null byte.
/// Each line in the file is ensured to have the same length, by properly padding it
/// with the null byte (which is done by calling the [`finish`][finish] method). This
/// is very necessary for finding the key-value pairs. While getting, the hash for
/// the given key is computed, and a [binary search][search] is made by seeking through
/// the file.
///
/// [finish]: #method.finish
/// [hasher]: https://doc.rust-lang.org/std/hash/struct.SipHasher.html
/// [search]: https://en.wikipedia.org/wiki/Binary_search_algorithm
pub struct HashFile<K: Display + FromStr + Hash, V: Display + FromStr, P: AsRef<Path>> {
    file: File,
    file_path: P,
    hashed: BTreeMap<u64, KeyValue<K, V>>,
    capacity: usize,
    line_length: usize,
}

impl<K: Display + FromStr + Hash, V: Display + FromStr, P: AsRef<Path>> HashFile<K, V, P> {
    pub fn new(path: P) -> Result<HashFile<K, V, P>, String> {
        Ok(HashFile {
            file: try!(OpenOptions::new()
                                   .read(true)
                                   .write(true)
                                   .create(true)
                                   .open(&path)
                                   .map_err(|e| format!("Cannot create/open file! ({})",
                                                        e.description()))),
            file_path: path,
            hashed: BTreeMap::new(),
            capacity: 0,
            line_length: 0,
        })
    }

    pub fn set_capacity(mut self, capacity: usize) -> HashFile<K, V, P> {
        self.capacity = capacity;
        self
    }

    pub fn set_length(mut self, length: usize) -> HashFile<K, V, P> {
        self.line_length = length;
        self
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
            let mut out_file = try!(OpenOptions::new()
                                                .read(true)
                                                .write(true)
                                                .create(true)
                                                .open(".hash_file")
                                                .map_err(|e| format!("Cannot create temp file! ({})",
                                                                     e.description())));
            let mut buf_writer = BufWriter::new(&mut out_file);

            for line in buf_reader.lines().filter_map(|l| l.ok()) {
                // Even though this takes a mutable reference, we can be certain that
                // we've found the maximum row length for this session
                try!(write_buffer(&mut buf_writer, &line, &mut self.line_length));
            }
        }

        try!(fs::rename(".hash_file", &self.file_path)
                .map_err(|e| format!("Cannot rename the temp file! ({})", e.description())));
        self.file = try!(OpenOptions::new()
                                     .read(true)
                                     .write(true)
                                     .open(&self.file_path)
                                     .map_err(|e| format!("Cannot open the working file! ({})",
                                                          e.description())));
        Ok(self.line_length)
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
                                          &mut self.line_length));
                    },
                    Ordering::Less => {
                        try!(write_buffer(&mut buf_writer, &file_iter.next().unwrap(),
                                          &mut self.line_length));
                    },
                    Ordering::Greater => {
                        let (_, btree_key_val) = map_iter.next().unwrap();
                        try!(write_buffer(&mut buf_writer, &(btree_key_val.to_string()),
                                          &mut self.line_length));
                    },
                }
            }
        }

        try!(fs::rename(".hash_file", &self.file_path)
                .map_err(|e| format!("Cannot rename the temp file! ({})", e.description())));
        self.file = try!(OpenOptions::new().read(true)
                                           .write(true)
                                           .open(&self.file_path)
                                           .map_err(|e| format!("Cannot open the working file! ({})",
                                                                e.description())));
        Ok(())
    }

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

    pub fn get(&mut self, key: &K) -> Result<Option<(V, usize)>, String> {
        let hashed_key = hash(key);
        let size = try!(self.file.metadata()
                                 .map_err(|e| format!("Cannot obtain file metadata ({})", e.description()))
                                 .map(|m| m.len()));
        if size == 0 {
            return Ok(None)
        }

        let row_length = (self.line_length + 1) as u64;
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
