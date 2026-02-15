use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    thread::JoinHandle,
};

use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender};
use sha1::{Digest, Sha1};

use crate::{engine::EngineEvent, torrent::Torrent};

#[derive(Debug)]
pub enum DiskError {
    WriteFailure(io::Error),
    ReadFailure(io::Error),
}

#[derive(Debug)]
pub enum DiskJob {
    WriteBlock {
        piece: u32,
        offset: u32,
        data: Bytes,
        respond_to: Sender<EngineEvent>,
    },
    ReadBlock {
        piece: u32,
        offset: u32,
        length: u32,
        respond_to: Sender<EngineEvent>,
    },
    VerifyPiece {
        piece: u32,
        expected_hash: [u8; 20],
        respond_to: Sender<EngineEvent>,
    },
    Flush,
    FlushPiece {
        piece: u32,
    },
    Stop,
}

#[derive(Debug)]
struct FileEntry {
    name: String,
    file: Mutex<File>,
    length: u64,
    offset: u64,
    downloaded: AtomicU64,
    verified: AtomicU64,
}

#[derive(Debug)]
struct TorrentLayout {
    piece_length: u32,
    total_length: u64,
    files: Vec<FileEntry>,
}

impl TorrentLayout {
    pub fn locate(&self, piece: u32, piece_offset: u32, length: u32) -> Vec<(usize, u64, u64)> {
        let global_offset = piece as u64 * self.piece_length as u64 + piece_offset as u64;

        let mut remaining = length as u64;
        let mut result = Vec::new();

        for (i, file) in self.files.iter().enumerate() {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            if global_offset >= file_end {
                continue;
            }

            if global_offset < file_end {
                let file_offset = global_offset.max(file_start) - file_start;
                let writable = (file_end - (file_start + file_offset)).min(remaining);

                result.push((i, file_offset, writable));

                remaining -= writable;

                if remaining == 0 {
                    break;
                }
            }
        }

        result
    }
}

#[derive(Debug)]
pub struct DiskInner {
    pending_writes: Mutex<HashMap<u32, Arc<AtomicU32>>>,
    queued_hashes: Mutex<HashMap<u32, [u8; 20]>>,
}

impl DiskInner {
    fn new() -> Self {
        Self {
            pending_writes: Mutex::new(HashMap::new()),
            queued_hashes: Mutex::new(HashMap::new()),
        }
    }

    pub fn start_write(&self, piece: u32) {
        let mut map = self.pending_writes.lock().unwrap();
        let counter = map
            .entry(piece)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)));
        counter.fetch_add(1, Ordering::SeqCst);
    }

    pub fn queue_verify(&self, piece: u32, expected_hash: [u8; 20]) -> bool {
        let mut writes = self.pending_writes.lock().unwrap();
        let counter = writes
            .entry(piece)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)));

        if counter.load(Ordering::SeqCst) == 0 {
            return true;
        }

        let mut queued = self.queued_hashes.lock().unwrap();
        queued.insert(piece, expected_hash);

        false
    }

    fn block_done(&self, piece: u32) -> Option<[u8; 20]> {
        let counter_opt = {
            let map = self.pending_writes.lock().unwrap();
            map.get(&piece).cloned()
        };

        if let Some(counter) = counter_opt {
            let prev = counter.fetch_sub(1, Ordering::SeqCst);
            let remaining = prev.saturating_sub(1);
            if remaining == 0 {
                let mut map = self.pending_writes.lock().unwrap();
                map.remove(&piece);

                let mut queued = self.queued_hashes.lock().unwrap();
                return queued.remove(&piece);
            }
        }

        None
    }
}

#[derive(Debug)]
pub struct DiskManager {
    layout: Arc<TorrentLayout>,
    joinables: Vec<JoinHandle<()>>,
    pub inner: Arc<DiskInner>,
}

impl DiskManager {
    pub fn new(torrent: &Torrent) -> io::Result<Self> {
        let mut files = Vec::new();
        let mut current_offset = 0u64;
        for f in &torrent.info.files {
            if let Some(parent) = f.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&f.path)?;

            let name = f
                .path
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or("unknown".to_string());

            files.push(FileEntry {
                name,
                file: Mutex::new(file),
                length: f.length,
                offset: current_offset,
                downloaded: AtomicU64::new(0),
                verified: AtomicU64::new(0),
            });
            current_offset += f.length;
        }
        let layout = TorrentLayout {
            piece_length: torrent.info.piece_length,
            total_length: torrent.info.total_size,
            files,
        };
        Ok(Self {
            layout: Arc::new(layout),
            joinables: Vec::new(),
            inner: Arc::new(DiskInner::new()),
        })
    }

    pub fn file_progress(&self) -> Vec<(String, u64, u64, u64)> {
        self.layout
            .files
            .iter()
            .map(|f| {
                (
                    f.name.clone(),
                    f.downloaded.load(Ordering::Relaxed),
                    f.verified.load(Ordering::Relaxed),
                    f.length,
                )
            })
            .collect()
    }

    pub fn join(&mut self) {
        while !self.joinables.is_empty() {
            let j = self.joinables.pop().unwrap();
            if let Err(e) = j.join() {
                eprintln!("failed to join worker: {:?}", e);
            }
        }
    }

    pub fn start(&mut self, workers: usize, rx: &Receiver<DiskJob>) {
        for _ in 0..workers {
            let layout = self.layout.clone();
            let rx = rx.clone();
            let inner = self.inner.clone();
            self.joinables.push(std::thread::spawn(move || {
                eprintln!(
                    "disk io worker {:?} starting...",
                    std::thread::current().id()
                );
                loop {
                    match rx.recv() {
                        Ok(ev) => match ev {
                            DiskJob::WriteBlock {
                                piece,
                                offset,
                                data,
                                respond_to,
                            } => {
                                let result = handle_write(piece, offset, &data, &layout);
                                let _ = respond_to.send(EngineEvent::BlockWritten { result });

                                if let Some(hash) = inner.block_done(piece) {
                                    let _ =
                                        respond_to.send(EngineEvent::VerifyPiece { piece, hash });
                                }
                            }
                            DiskJob::ReadBlock {
                                piece,
                                offset,
                                length,
                                respond_to,
                            } => {
                                let result = handle_read(piece, offset, length, &layout);
                                let _ = respond_to.send(EngineEvent::ReadBlock { result: result });
                            }
                            DiskJob::VerifyPiece {
                                piece,
                                expected_hash,
                                respond_to,
                            } => {
                                let result = verify_piece(piece, &layout, &expected_hash);
                                let _ = respond_to.send(EngineEvent::PieceVerification { result });
                            }
                            DiskJob::Stop => break,
                            DiskJob::Flush => {
                                for entry in &layout.files {
                                    if let Ok(f) = entry.file.lock() {
                                        let _ = f.sync_data();
                                    }
                                }
                            }
                            DiskJob::FlushPiece { piece } => {
                                if let Err(e) = flush_piece(piece, &layout) {
                                    eprintln!("failed to flush piece {}: {:?}", piece, e);
                                }
                            }
                        },
                        Err(_) => break,
                    }
                }
                eprintln!(
                    "disk io worker {:?} exiting...",
                    std::thread::current().id()
                );
            }));
        }
    }
}

fn handle_write(
    piece: u32,
    offset: u32,
    data: &[u8],
    layout: &TorrentLayout,
) -> Result<(), DiskError> {
    let locations = layout.locate(piece, offset, data.len() as u32);

    let mut written = 0usize;

    for (file_idx, file_offset, len) in locations {
        let entry = &layout.files[file_idx];

        let mut file = entry.file.lock().map_err(|_| {
            DiskError::WriteFailure(io::Error::new(io::ErrorKind::Other, "file mutex poisoned"))
        })?;

        file.seek(io::SeekFrom::Start(file_offset))
            .map_err(DiskError::WriteFailure)?;

        let end = written + len as usize;
        file.write_all(&data[written..end])
            .map_err(DiskError::WriteFailure)?;

        entry.downloaded.fetch_add(len, Ordering::Relaxed);

        written = end;
    }

    debug_assert!(written == data.len());

    Ok(())
}

fn flush_piece(piece: u32, layout: &TorrentLayout) -> Result<(), DiskError> {
    let piece_start = piece as u64 * layout.piece_length as u64;

    let piece_len = if piece_start + layout.piece_length as u64 > layout.total_length {
        layout.total_length - piece_start
    } else {
        layout.piece_length as u64
    };

    let piece_end = piece_start + piece_len;

    for entry in &layout.files {
        let file_start = entry.offset;
        let file_end = entry.offset + entry.length;

        if piece_start < file_end && piece_end > file_start {
            if let Ok(f) = entry.file.lock() {
                f.sync_data().map_err(DiskError::WriteFailure)?;
            }
        }
    }

    Ok(())
}

fn handle_read(
    _piece: u32,
    _offset: u32,
    _length: u32,
    _layout: &TorrentLayout,
) -> Result<(u32, u32, Bytes), DiskError> {
    todo!()
}

fn verify_piece(
    piece: u32,
    layout: &TorrentLayout,
    expected_hash: &[u8; 20],
) -> Result<(u32, bool), DiskError> {
    flush_piece(piece, layout)?;
    let piece_start = piece as u64 * layout.piece_length as u64;

    let piece_len = if piece_start + layout.piece_length as u64 > layout.total_length {
        layout.total_length - piece_start
    } else {
        layout.piece_length as u64
    };

    let piece_end = piece_start + piece_len;

    let mut hasher = Sha1::new();
    let mut remaining = piece_len;

    let mut bytes_processed = 0;
    for entry in &layout.files {
        let file_start = entry.offset;
        let file_end = entry.offset + entry.length;

        if piece_start >= file_end || piece_end <= file_start {
            continue;
        }

        let read_start = piece_start.max(file_start);
        let read_end = piece_end.min(file_end);

        let file_offset = read_start - file_start;
        let to_read = read_end - read_start;

        let mut file = entry.file.lock().map_err(|_| {
            DiskError::ReadFailure(io::Error::new(io::ErrorKind::Other, "file mutex poisoned"))
        })?;

        file.seek(io::SeekFrom::Start(file_offset))
            .map_err(DiskError::ReadFailure)?;

        let mut buffer = [0u8; 16 * 1024];
        let mut left = to_read;
        while left > 0 {
            let read_len = buffer.len().min(left as usize);
            let n = file
                .read(&mut buffer[..read_len])
                .map_err(DiskError::ReadFailure)?;
            bytes_processed += n;

            if n == 0 {
                return Err(DiskError::ReadFailure(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF while verifying piece",
                )));
            }

            hasher.update(&buffer[..n]);
            left -= n as u64;
            remaining -= n as u64;

            if bytes_processed >= 256 * 1024 {
                std::thread::yield_now();
                bytes_processed = 0;
            }
        }
    }

    debug_assert!(remaining == 0);

    let digest = hasher.finalize();

    let ok = digest.as_slice() == expected_hash;
    if ok {
        for (file_idx, bytes) in piece_file_ranges(piece, layout) {
            layout.files[file_idx]
                .verified
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    Ok((piece, ok))
}

fn piece_file_ranges(piece: u32, layout: &TorrentLayout) -> Vec<(usize, u64)> {
    let piece_start = piece as u64 * layout.piece_length as u64;
    let piece_len = if piece_start + layout.piece_length as u64 > layout.total_length {
        layout.total_length - piece_start
    } else {
        layout.piece_length as u64
    };
    let piece_end = piece_start + piece_len;

    let mut result = Vec::new();

    for (i, entry) in layout.files.iter().enumerate() {
        let file_start = entry.offset;
        let file_end = entry.offset + entry.length;

        if piece_start >= file_end || piece_end <= file_start {
            continue;
        }

        let overlap = piece_end.min(file_end) - piece_start.max(file_start);

        result.push((i, overlap));
    }

    result
}
