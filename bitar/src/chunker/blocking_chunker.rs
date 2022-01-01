use std::io::{self, Read};

use bytes::BytesMut;

use crate::{chunker::Chunker, Chunk};

const REFILL_SIZE: usize = 1024 * 1024;

/// A streaming chunker to use with any source which implements tokio AsyncRead.
pub struct BlockingChunker<C, R> {
    chunk_start: u64,
    buf: BytesMut,
    chunker: C,
    reader: R,
}

impl<C, R> BlockingChunker<C, R> {
    pub fn new(chunker: C, reader: R) -> Self {
        Self {
            chunk_start: 0,
            buf: BytesMut::with_capacity(REFILL_SIZE),
            chunker,
            reader,
        }
    }

    /// Get next chunk.
    ///
    /// None is returned when there are no more chunks to be read.
    pub fn next_chunk(&mut self) -> Option<io::Result<(u64, Chunk)>>
    where
        C: Chunker + Send + Sized,
        R: Read + Send + Sized,
    {
        loop {
            if !self.buf.is_empty() {
                if let Some(chunk) = self.chunker.next(&mut self.buf) {
                    let offset = self.chunk_start;
                    self.chunk_start += chunk.len() as u64;
                    return Some(Ok((offset, chunk)));
                }
            }
            // No chunk found in the buffer. Read data and append to buffer.
            match refill_buf(&mut self.buf, &mut self.reader) {
                Ok(0) if self.buf.is_empty() => {
                    // EOF and empty buffer.
                    return None;
                }
                Ok(0) => {
                    // EOF and some data left in buffer.
                    let chunk = Chunk(self.buf.split().freeze());
                    return Some(Ok((self.chunk_start, chunk)));
                }
                Ok(_) => {
                    // Buffer refilled.
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

impl<C, R> Iterator for BlockingChunker<C, R>
where
    C: Chunker + Send + Sized,
    R: Read + Send + Sized,
{
    type Item = io::Result<(u64, Chunk)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk()
    }
}

fn refill_buf<R>(buf: &mut BytesMut, mut reader: R) -> io::Result<usize>
where
    R: Read,
{
    let mut read_count = 0;
    let before_size = buf.len();
    {
        let new_size = before_size + REFILL_SIZE;
        if buf.capacity() < new_size {
            buf.reserve(REFILL_SIZE);
        }
        unsafe {
            // Use unsafe set_len() here instead of resize as we don't care for
            // zeroing the content of buf.
            buf.set_len(new_size);
        }
    }
    while read_count < REFILL_SIZE {
        let offset = before_size + read_count;
        let rc = match reader.read(&mut buf[offset..]) {
            Ok(0) => break, // EOF
            Ok(rc) => rc,
            Err(err) => {
                buf.resize(before_size + read_count, 0);
                return Err(err);
            }
        };
        read_count += rc;
    }
    buf.resize(before_size + read_count, 0);
    Ok(read_count)
}
