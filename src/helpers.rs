use std::error::Error;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher, SipHasher};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::iter;

pub const SEP: char = '\0';

/// Computes the hash for the given object using the built-in `SipHasher`
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

/// Opens a file in read/write mode (or creates if it doesn't exist)
pub fn create_or_open_file(path: &str) -> Result<File, String> {
    OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .map_err(|e| format!("Cannot create/open file at {}! ({})",
                                     path, e.description()))
}

pub fn seek_from_start(file: &mut File, pos: u64) -> Result<(), String> {
    file.seek(SeekFrom::Start(pos))
        .map(|_| ())
        .map_err(|e| format!("Cannot seek through file! ({})", e.description()))
}

pub fn get_size(file: &File) -> Result<u64, String> {
    file.metadata()
        .map(|m| m.len())
        .map_err(|e| format!("Cannot obtain file metadata ({})", e.description()))
}

pub fn read_one_line(file: &mut File) -> Result<String, String> {
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line)
          .map(|_| line)
          .map_err(|e| format!("Cannot read line from file! ({})", e.description()))
}
