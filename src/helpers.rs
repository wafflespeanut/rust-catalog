use std::error::Error;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher, SipHasher};
use std::io::{BufWriter, Write};
use std::iter;

pub const SEP: char = '\0';
pub const TEMP_FILE: &'static str = ".hash_file";

/// Computes the hash for the given object using the built-in
/// [`SipHasher`](https://doc.rust-lang.org/std/hash/struct.SipHasher.html)
pub fn hash<T: Hash>(obj: &T) -> u64 {
    let mut hasher = SipHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

/// Writes a line to the given buffer
/// (pads the line with null bytes to fit to the given length)
pub fn write_buffer(buf_writer: &mut BufWriter<&mut File>,
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

/// Creates a temp file and opens in read/write mode
pub fn create_temp_file() -> Result<File, String> {
    OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(TEMP_FILE)
                .map_err(|e| format!("Cannot create temp file! ({})",
                                     e.description()))
}
