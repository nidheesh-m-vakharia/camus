//! On-disk log entries.
//!
//! A record starts with `CM01`, a count, one `u32` length per batch, the
//! postcard of the entry with those batches emptied, then each batch's bytes
//! once. A reader that knows a batch's offset preads that range and leaves
//! the rest of the entry on disk. An older record, with no magic, is plain
//! postcard.

use std::io::{self, Write};

use serde::de::DeserializeOwned;
use serde::Serialize;

const MAGIC: &[u8; 4] = b"CM01";

/// An entry that stores some byte strings beside its postcard, so a reader
/// can take one of them without decoding the entry.
pub trait SplitBatches: Serialize + DeserializeOwned + Clone {
    /// Each separable byte string, in order. Empty when the entry has none.
    fn batches(&self) -> Vec<&[u8]> {
        Vec::new()
    }

    /// Drop those byte strings. The postcard of what remains is the header.
    fn clear_batches(&mut self) {}

    /// Put `batches` back, in the same order [`SplitBatches::batches`] yielded.
    fn fill_batches(&mut self, batches: Vec<Vec<u8>>) {
        let _ = batches;
    }
}

/// Write a postcard `header` and then each batch's bytes, with a length table
/// in front so a reader can pread one batch.
pub fn write_parts(header: &[u8], batches: &[&[u8]], mut out: impl Write) -> io::Result<()> {
    if u32::try_from(batches.len()).is_err() || u32::try_from(header.len()).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an entry header does not fit the length table",
        ));
    }
    out.write_all(MAGIC)?;
    out.write_all(&(batches.len() as u32).to_le_bytes())?;
    for batch in batches {
        let len = u32::try_from(batch.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "a batch is longer than 4 GiB")
        })?;
        out.write_all(&len.to_le_bytes())?;
    }
    out.write_all(&(header.len() as u32).to_le_bytes())?;
    out.write_all(header)?;
    for batch in batches {
        out.write_all(batch)?;
    }
    Ok(())
}

/// Header bytes and each batch, when `bytes` is a [`write_parts`] record.
/// `None` when it is an older postcard record.
pub fn read_parts(bytes: &[u8]) -> io::Result<Option<(&[u8], Vec<Vec<u8>>)>> {
    if bytes.len() < 4 || &bytes[..4] != MAGIC {
        return Ok(None);
    }
    let (header, batches) = split(bytes)?;
    Ok(Some((header, batches)))
}

/// Offset of batch `pos` within a [`write_parts`] record.
pub fn batch_offset(header_len: usize, batch_lens: &[usize], pos: usize) -> Option<u64> {
    if pos >= batch_lens.len() {
        return None;
    }
    let mut at = 4 + 4 + batch_lens.len() * 4 + 4 + header_len;
    for len in batch_lens.iter().take(pos) {
        at += *len;
    }
    Some(at as u64)
}

/// How many bytes [`write_entry`] emits.
pub fn encoded_len<T: SplitBatches>(entry: &T) -> io::Result<usize> {
    let (header, batches) = header_and_batches(entry)?;
    Ok(framed_len(header.len(), &batches))
}

/// Write one entry. The batch bytes are copied from `entry` into `out`.
pub fn write_entry<T: SplitBatches>(entry: &T, mut out: impl Write) -> io::Result<()> {
    let (header, batches) = header_and_batches(entry)?;
    out.write_all(MAGIC)?;
    out.write_all(&(batches.len() as u32).to_le_bytes())?;
    for batch in &batches {
        out.write_all(&(*batch as u32).to_le_bytes())?;
    }
    out.write_all(&(header.len() as u32).to_le_bytes())?;
    out.write_all(&header)?;
    // Lengths were checked in `header_and_batches`. The bytes themselves are
    // still on `entry`.
    for batch in entry.batches() {
        out.write_all(batch)?;
    }
    Ok(())
}

/// Byte offset of batch `pos` within an encoded entry, when it has one.
pub fn batch_at<T: SplitBatches>(entry: &T, pos: usize) -> io::Result<Option<u64>> {
    let (header, batches) = header_and_batches(entry)?;
    if pos >= batches.len() {
        return Ok(None);
    }
    let mut at = 4 + 4 + batches.len() * 4 + 4 + header.len();
    for len in batches.iter().take(pos) {
        at += *len;
    }
    Ok(Some(at as u64))
}

/// Decode a record written by [`write_entry`], or a plain postcard record.
pub fn decode_entry<T: SplitBatches>(bytes: &[u8]) -> io::Result<T> {
    if bytes.len() < 4 || &bytes[..4] != MAGIC {
        return postcard::from_bytes(bytes).map_err(invalid);
    }
    let (header, batches) = split(bytes)?;
    let mut entry: T = postcard::from_bytes(header).map_err(invalid)?;
    entry.fill_batches(batches);
    Ok(entry)
}

fn header_and_batches<T: SplitBatches>(entry: &T) -> io::Result<(Vec<u8>, Vec<usize>)> {
    let mut stripped = entry.clone();
    stripped.clear_batches();
    let header = postcard::to_stdvec(&stripped).map_err(invalid)?;
    let mut batches = Vec::new();
    for batch in entry.batches() {
        let len = u32::try_from(batch.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "a batch is longer than 4 GiB")
        })?;
        batches.push(len as usize);
    }
    if u32::try_from(batches.len()).is_err() || u32::try_from(header.len()).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "an entry header does not fit the length table",
        ));
    }
    Ok((header, batches))
}

fn framed_len(header: usize, batches: &[usize]) -> usize {
    4 + 4 + batches.len() * 4 + 4 + header + batches.iter().sum::<usize>()
}

fn split(bytes: &[u8]) -> Result<(&[u8], Vec<Vec<u8>>), io::Error> {
    if bytes.len() < 8 {
        return Err(invalid("short entry"));
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let table = 8 + count * 4;
    if bytes.len() < table + 4 {
        return Err(invalid("short length table"));
    }
    let mut lens = Vec::with_capacity(count);
    for index in 0..count {
        let at = 8 + index * 4;
        lens.push(u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize);
    }
    let header_len = u32::from_le_bytes(bytes[table..table + 4].try_into().unwrap()) as usize;
    let header_at = table + 4;
    let header_end = header_at
        .checked_add(header_len)
        .ok_or_else(|| invalid("header length overflow"))?;
    if bytes.len() < header_end {
        return Err(invalid("short header"));
    }
    let mut cursor = header_end;
    let mut batches = Vec::with_capacity(count);
    for len in lens {
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| invalid("batch length overflow"))?;
        if bytes.len() < end {
            return Err(invalid("short batch"));
        }
        batches.push(bytes[cursor..end].to_vec());
        cursor = end;
    }
    Ok((&bytes[header_at..header_end], batches))
}

fn invalid(err: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug)]
    struct Sample {
        tag: u8,
        raws: Vec<Vec<u8>>,
    }

    impl SplitBatches for Sample {
        fn batches(&self) -> Vec<&[u8]> {
            self.raws.iter().map(|raw| raw.as_slice()).collect()
        }

        fn clear_batches(&mut self) {
            for raw in &mut self.raws {
                raw.clear();
            }
        }

        fn fill_batches(&mut self, batches: Vec<Vec<u8>>) {
            for (slot, batch) in self.raws.iter_mut().zip(batches) {
                *slot = batch;
            }
        }
    }

    #[test]
    fn one_batch_sits_at_a_stable_offset() -> io::Result<()> {
        let entry = Sample {
            tag: 7,
            raws: vec![vec![1u8; 20], vec![2u8; 50]],
        };
        let mut bytes = Vec::new();
        write_entry(&entry, &mut bytes)?;
        assert_eq!(bytes.len(), encoded_len(&entry)?);
        let decoded: Sample = decode_entry(&bytes)?;
        assert_eq!(decoded, entry);
        let at = batch_at(&entry, 1)?.ok_or_else(|| invalid("second batch"))?;
        let start = at as usize;
        assert_eq!(&bytes[start..start + 50], &[2u8; 50]);
        assert!(start + 50 == bytes.len());
        Ok(())
    }

    #[test]
    fn a_plain_postcard_record_still_decodes() -> io::Result<()> {
        let entry = Sample {
            tag: 1,
            raws: vec![vec![9]],
        };
        let bytes = postcard::to_stdvec(&entry).map_err(invalid)?;
        let decoded: Sample = decode_entry(&bytes)?;
        assert_eq!(decoded, entry);
        Ok(())
    }
}
