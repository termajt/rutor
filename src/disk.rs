use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Mutex,
};

use crate::{bitfield::Bitfield, torrent::TorrentFile};

#[derive(Debug)]
struct FileHandleCache {
    handles: Mutex<HashMap<PathBuf, File>>,
}

impl FileHandleCache {
    fn new() -> Self {
        Self {
            handles: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_file<F, T>(&self, path: &PathBuf, f: F) -> Result<T, Box<dyn std::error::Error>>
    where
        F: FnOnce(&mut File) -> Result<T, Box<dyn std::error::Error>>,
    {
        let mut handles = self.handles.lock().unwrap();
        if !handles.contains_key(path) {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;
            handles.insert(path.to_path_buf(), file);
        }

        let file = handles.get_mut(path).unwrap();
        let result = f(file)?;
        file.flush()?;

        Ok(result)
    }
}

#[derive(Debug)]
pub struct DiskManager {
    file_handle_cache: FileHandleCache,
    total_pieces: usize,
    total_size: u64,
    piece_length: usize,
    files: Vec<TorrentFile>,
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
        }
    }

    pub fn write_to_disk(
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

    pub fn read_piece(&self, piece_index: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let (piece_start, piece_end) = self.piece_range(piece_index);
        let piece_len = (piece_end - piece_start) as usize;
        let mut buf = vec![0u8; piece_len];

        let mut offset_in_piece = 0;
        let mut offset_in_torrent = piece_start;

        for file in &self.files {
            if offset_in_torrent >= file.length {
                offset_in_torrent -= file.length;
                continue;
            }

            let read_start = offset_in_torrent as usize;
            let read_len = std::cmp::min(
                file.length as usize - read_start,
                piece_len - offset_in_piece,
            );

            self.file_handle_cache.with_file(&file.path, |f| {
                f.seek(SeekFrom::Start(read_start as u64))?;
                f.read_exact(&mut buf[offset_in_piece..offset_in_piece + read_len])?;
                Ok(())
            })?;

            offset_in_piece += read_len;
            offset_in_torrent = 0;
            if offset_in_piece == piece_len {
                break;
            }
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
}
