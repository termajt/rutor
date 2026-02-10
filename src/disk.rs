use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{bitfield::Bitfield, torrent::TorrentFile};

const WRITE_BUFFER_BYTES: usize = 256 * 1024;
const FLUSH_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug)]
struct PieceWriteBuffer {
    size: usize,
    buffer: BTreeMap<usize, Vec<u8>>,
    last_flush: Instant,
}

impl PieceWriteBuffer {
    fn new() -> Self {
        Self {
            size: 0,
            buffer: BTreeMap::new(),
            last_flush: Instant::now(),
        }
    }

    fn push(&mut self, offset: usize, data: Vec<u8>) {
        self.size += data.len();
        self.buffer.insert(offset, data);
    }

    fn should_flush(&self) -> bool {
        self.size >= WRITE_BUFFER_BYTES
            || (!self.buffer.is_empty() && self.last_flush.elapsed() >= FLUSH_TIMEOUT)
    }

    fn flush<F>(&mut self, mut write_fn: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(usize, &[u8]) -> Result<(), Box<dyn std::error::Error>>,
    {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let mut merged_blocks = Vec::new();
        let mut current_offset = 0;
        let mut current_data = Vec::new();

        for (&offset, data) in &self.buffer {
            if current_data.is_empty() {
                current_offset = offset;
                current_data.extend_from_slice(data);
            } else if current_offset + current_data.len() == offset {
                current_data.extend_from_slice(data);
            } else {
                merged_blocks.push((current_offset, std::mem::take(&mut current_data)));
                current_offset = offset;
                current_data.extend_from_slice(data);
            }
        }

        if !current_data.is_empty() {
            merged_blocks.push((current_offset, current_data));
        }

        for (offset, data) in merged_blocks {
            write_fn(offset, &data)?;
        }

        self.buffer.clear();
        self.size = 0;
        self.last_flush = Instant::now();

        Ok(())
    }
}

#[derive(Debug)]
struct FileHandleCache {
    handles: HashMap<PathBuf, File>,
}

impl FileHandleCache {
    fn new() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    pub fn with_file<F, T>(&mut self, path: &PathBuf, f: F) -> Result<T, Box<dyn std::error::Error>>
    where
        F: FnOnce(&mut File) -> Result<T, Box<dyn std::error::Error>>,
    {
        if !self.handles.contains_key(path) {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;
            self.handles.insert(path.to_path_buf(), file);
        }

        let file = self.handles.get_mut(path).unwrap();
        let result = f(file)?;
        file.flush()?;

        Ok(result)
    }

    fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for handle in self.handles.values_mut() {
            handle.flush()?;
        }
        self.handles.clear();

        Ok(())
    }
}

#[derive(Debug)]
pub struct DiskManager {
    file_handle_cache: FileHandleCache,
    total_pieces: usize,
    total_size: u64,
    piece_length: usize,
    files: Vec<TorrentFile>,
    write_buffers: HashMap<usize, PieceWriteBuffer>,
}

impl DiskManager {
    pub fn new(
        total_pieces: usize,
        total_size: u64,
        piece_length: usize,
        files: Vec<TorrentFile>,
    ) -> Self {
        Self {
            file_handle_cache: FileHandleCache::new(),
            total_pieces,
            total_size,
            piece_length,
            files,
            write_buffers: HashMap::new(),
        }
    }

    pub fn write_to_disk(
        &mut self,
        piece_index: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let entry = self
            .write_buffers
            .entry(piece_index)
            .or_insert_with(PieceWriteBuffer::new);

        entry.push(offset, data.to_vec());

        if entry.should_flush() {
            self.flush_piece(piece_index)?;
        }

        Ok(())
    }

    pub fn flush_piece(&mut self, piece_index: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut entry) = self.write_buffers.remove(&piece_index) {
            entry.flush(|offset, data| {
                self.write_block_direct(piece_index, offset, data)?;
                Ok(())
            })?;
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let pieces: Vec<usize> = self.write_buffers.keys().cloned().collect();
        for p in pieces {
            self.flush_piece(p)?;
        }
        Ok(())
    }

    fn write_block_direct(
        &mut self,
        piece_index: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let piece_size = if piece_index as usize == self.total_pieces - 1 {
            let remainder = self.total_size % self.piece_length as u64;
            if remainder == 0 {
                self.piece_length as u64
            } else {
                remainder
            }
        } else {
            self.piece_length as u64
        };
        if (offset as u64 + data.len() as u64) > piece_size {
            return Err("piece overflow".into());
        }
        let mut offset_in_torrent = piece_index as u64 * self.piece_length as u64 + offset as u64;
        let mut remaining = data;
        for file in self.files.iter_mut() {
            if offset_in_torrent >= file.length {
                offset_in_torrent -= file.length;
                continue;
            }

            let write_start = offset_in_torrent as usize;
            let write_len = std::cmp::min(file.length as usize - write_start, remaining.len());
            self.file_handle_cache.with_file(&file.path, |f| {
                f.seek(SeekFrom::Start(write_start as u64))?;
                f.write_all(&remaining[..write_len])?;
                Ok(())
            })?;
            remaining = &remaining[write_len..];
            offset_in_torrent = 0;
            if remaining.is_empty() {
                break;
            }
        }

        if !remaining.is_empty() {
            return Err("block exceeds torrent total size".into());
        }

        Ok(())
    }

    pub fn files(&self) -> &[TorrentFile] {
        &self.files
    }

    pub fn verified_bytes_for_file(&self, file_index: usize, bitfield: &Bitfield) -> u64 {
        let (file_start, file_end) = self.file_range(file_index);
        let mut verified = 0;
        for piece_index in 0..self.total_pieces {
            if !bitfield.get(&piece_index) {
                continue;
            }

            let (piece_start, piece_end) = self.piece_range(piece_index);

            let overlap_start = file_start.max(piece_start);
            let overlap_end = file_end.min(piece_end);

            if overlap_start < overlap_end {
                verified += overlap_end - overlap_start;
            }
        }

        verified
    }

    pub fn read_piece(
        &mut self,
        piece_index: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut buf = Vec::new();

        let result = self.read_piece_chunks(piece_index, 8192, |data, _| {
            buf.extend_from_slice(&data);
            Ok(())
        });

        if let Err(e) = result {
            return Err(e);
        }

        Ok(buf)
    }

    fn file_range(&self, file_index: usize) -> (u64, u64) {
        let start = self.files[..file_index]
            .iter()
            .map(|f| f.length)
            .sum::<u64>();

        let end = start + self.files[file_index].length;
        (start, end)
    }

    fn piece_range(&self, piece_index: usize) -> (u64, u64) {
        let start = piece_index as u64 * self.piece_length as u64;
        let mut len = self.piece_length as u64;

        if piece_index == self.total_pieces - 1 {
            let remainder = self.total_size % self.piece_length as u64;
            if remainder != 0 {
                len = remainder;
            }
        }

        (start, start + len)
    }

    pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.file_handle_cache.close()?;

        Ok(())
    }

    pub fn flush_expired_buffers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let expired: Vec<usize> = self
            .write_buffers
            .iter()
            .filter(|(_, buf)| buf.last_flush.elapsed() >= FLUSH_TIMEOUT)
            .map(|(piece, _)| *piece)
            .collect();

        for piece in expired {
            self.flush_piece(piece)?;
        }

        Ok(())
    }

    pub fn read_piece_chunks<F>(
        &mut self,
        piece_index: usize,
        chunk_size: usize,
        mut on_chunk: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(Vec<u8>, bool) -> Result<(), Box<dyn std::error::Error>>,
    {
        let (piece_start, piece_end) = self.piece_range(piece_index);
        let piece_len = (piece_end - piece_start) as usize;

        let mut offset_in_piece = 0usize;
        let mut offset_in_torrent = piece_start;

        let mut buf = vec![0u8; chunk_size.min(piece_len)];

        for file in &self.files {
            if offset_in_torrent >= file.length {
                offset_in_torrent -= file.length;
                continue;
            }

            let mut file_offset = offset_in_torrent as usize;
            let mut remaining_in_piece = piece_len - offset_in_piece;

            while remaining_in_piece > 0 && file_offset < file.length as usize {
                let read_len = std::cmp::min(
                    chunk_size,
                    std::cmp::min(remaining_in_piece, file.length as usize - file_offset),
                );

                self.file_handle_cache.with_file(&file.path, |f| {
                    f.seek(SeekFrom::Start(file_offset as u64))?;
                    f.read_exact(&mut buf[..read_len])?;
                    Ok(())
                })?;

                let is_last = offset_in_piece + read_len == piece_len;

                on_chunk(buf[..read_len].to_vec(), is_last)?;

                offset_in_piece += read_len;
                remaining_in_piece -= read_len;
                file_offset += read_len;
            }

            offset_in_torrent = 0;

            if offset_in_piece == piece_len {
                break;
            }
        }

        Ok(())
    }
}
