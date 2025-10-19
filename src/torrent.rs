use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    fmt,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Mutex, RwLock},
};

use crate::{
    bencode::{self, Bencode},
    bitfield::Bitfield,
};

#[derive(Debug)]
pub enum Error {
    LoadError(String),
    IoError(std::io::Error),
    BencodeError(bencode::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}

impl From<bencode::Error> for Error {
    fn from(value: bencode::Error) -> Self {
        Error::BencodeError(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::LoadError(msg) => write!(f, "Load error: {}", msg),
            Error::IoError(e) => write!(f, "IO error: {}", e),
            Error::BencodeError(e) => write!(f, "Bencode error: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError(e) => Some(e),
            Error::BencodeError(e) => Some(e),
            _ => None,
        }
    }
}

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
            let file = OpenOptions::new().write(true).create(true).open(path)?;
            handles.insert(path.to_path_buf(), file);
        }

        let file = handles.get_mut(path).unwrap();
        f(file)
    }
}

#[derive(Debug)]
pub struct FileSlice {
    pub file_index: usize,
    pub offset_in_file: u64,
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct TorrentFile {
    length: u64,
    pub path: Vec<String>,
}

#[derive(Debug)]
pub struct TorrentInfo {
    pub name: String,
    pub piece_length: u32,
    pub piece_hashes: Vec<[u8; 20]>,
    pub total_size: u64,
    pub files: Vec<TorrentFile>,
    bitfield: RwLock<Bitfield>,
    destination: Option<PathBuf>,
    file_handle_cache: FileHandleCache,
}

impl TorrentInfo {
    pub fn from_bencode(bencode: &Bencode, destination: Option<PathBuf>) -> Result<Self, Error> {
        let map = bencode.dict()?;
        let name = match map.get(&b"name".to_vec()) {
            Some(bname) => String::from_utf8_lossy(bname.bytes()?).to_string(),
            None => return Err(Error::LoadError("missing 'name' field".into())),
        };
        let piece_length = match map.get(&b"piece length".to_vec()) {
            Some(bpiece_length) => bpiece_length.int()? as u32,
            None => return Err(Error::LoadError("missing 'piece length' field".into())),
        };
        let piece_hashes = match map.get(&b"pieces".to_vec()) {
            Some(bpieces) => {
                let pieces_bytes = bpieces.bytes()?;
                if pieces_bytes.len() % 20 != 0 {
                    return Err(Error::LoadError(format!(
                        "pieces field length {} is not a multiple of 20",
                        pieces_bytes.len()
                    )));
                }
                pieces_bytes
                    .chunks_exact(20)
                    .map(|chunk| {
                        let mut hash = [0u8; 20];
                        hash.copy_from_slice(chunk);
                        hash
                    })
                    .collect::<Vec<_>>()
            }
            None => return Err(Error::LoadError("missing 'pieces' field".into())),
        };
        let mut total_size: u64 = 0;
        let mut files = Vec::new();
        if let Some(bfiles) = map.get(&b"files".to_vec()) {
            let file_list = bfiles.list()?;
            for bfile in file_list {
                let file_dict = bfile.dict()?;

                let length = file_dict
                    .get(&b"length".to_vec())
                    .ok_or_else(|| Error::LoadError("file missing length".into()))?
                    .int()? as u64;

                let path_list = file_dict
                    .get(&b"path".to_vec())
                    .ok_or_else(|| Error::LoadError("file missing path".into()))?
                    .list()?;

                let path = path_list
                    .iter()
                    .map(|p| {
                        let bytes = p.bytes()?;
                        Ok(String::from_utf8_lossy(bytes).to_string())
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                total_size += length;
                files.push(TorrentFile {
                    length: length,
                    path: path,
                });
            }
        } else if let Some(blength) = map.get(&b"length".to_vec()) {
            total_size = blength.int()? as u64;
            files.push(TorrentFile {
                length: total_size,
                path: vec![name.clone()],
            });
        } else {
            return Err(Error::LoadError(
                "neither 'length' nor 'files' present in info dict".into(),
            ));
        }
        let total_pieces = piece_hashes.len();
        Ok(TorrentInfo {
            name: name,
            piece_length: piece_length,
            piece_hashes: piece_hashes,
            total_size: total_size,
            files: files,
            bitfield: RwLock::new(Bitfield::new(total_pieces)),
            destination: destination,
            file_handle_cache: FileHandleCache::new(),
        })
    }

    pub fn piece_file_slices(&self, piece_index: usize) -> Vec<FileSlice> {
        let piece_length = self.piece_length as u64;
        let piece_start = piece_index as u64 * piece_length;
        let piece_end = ((piece_index + 1) as u64 * piece_length).min(self.total_size);

        let mut slices = Vec::new();
        let mut remaining = piece_end - piece_start;
        let mut global_offset = 0;
        for (i, file) in self.files.iter().enumerate() {
            let file_end = global_offset + file.length;
            if file_end <= piece_start {
                global_offset = file_end;
                continue;
            }

            if global_offset >= piece_end {
                break;
            }

            let start_in_file = piece_start.saturating_sub(global_offset);
            let length = (file_end.min(piece_end) - (global_offset + start_in_file)).min(remaining);
            slices.push(FileSlice {
                file_index: i,
                offset_in_file: start_in_file,
                length: length,
            });
            remaining -= length;
            global_offset = file_end;
            if remaining == 0 {
                break;
            }
        }
        slices
    }

    pub fn write_data_to_disk(
        &self,
        piece_index: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut data_offset = 0;
        for slice in self.piece_file_slices(piece_index) {
            let file = &self.files[slice.file_index];
            let path_str = file.path.join(&std::path::MAIN_SEPARATOR.to_string());
            let path: PathBuf = if let Some(dest) = &self.destination {
                dest.join(path_str)
            } else {
                PathBuf::from(path_str)
            };

            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let slice_start_offset = if offset as u64 > slice.offset_in_file {
                offset as u64 - slice.offset_in_file
            } else {
                0
            };

            let write_len = std::cmp::min(
                slice.length - slice_start_offset,
                (data.len() - data_offset) as u64,
            );

            if write_len == 0 {
                continue;
            }

            self.file_handle_cache.with_file(&path, |f| {
                f.seek(SeekFrom::Start(slice.offset_in_file + slice_start_offset))?;
                f.write_all(&data[data_offset..data_offset + write_len as usize])?;
                Ok(())
            })?;
            data_offset += write_len as usize;
            if data_offset >= data.len() {
                break;
            }
        }

        Ok(())
    }

    pub fn has_any_pieces(&self) -> bool {
        let bitfield = self.bitfield.read().unwrap();
        bitfield.has_any()
    }

    pub fn set_bitfield_index(&self, index: usize) {
        let mut bitfield = self.bitfield.write().unwrap();
        bitfield.set(&index, true);
    }

    pub fn bitfield(&self) -> Bitfield {
        let bitfield = self.bitfield.read().unwrap();
        bitfield.clone()
    }

    pub fn is_complete(&self) -> bool {
        let bitfield = self.bitfield.read().unwrap();
        bitfield.count_ones() == self.piece_hashes.len()
    }

    pub fn pieces_left(&self) -> usize {
        let bitfield = self.bitfield.read().unwrap();
        bitfield.count_zeros()
    }

    pub fn pieces_verified(&self) -> usize {
        let bitfield = self.bitfield.read().unwrap();
        bitfield.count_ones()
    }

    pub fn bitfield_differs(&self, other: &Bitfield) -> bool {
        let bitfield = self.bitfield.read().unwrap();
        bitfield.differs_from(other)
    }
}

#[derive(Debug)]
pub struct Torrent {
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub info: TorrentInfo,
    pub info_hash: [u8; 20],
}

impl Torrent {
    pub fn new(bytes: Vec<u8>, destination: Option<PathBuf>) -> Result<Self, Error> {
        let bencode = bencode::decode(&bytes)?;
        Torrent::from_bencode(bencode, destination)
    }

    pub fn from_bencode(bencode: Bencode, destination: Option<PathBuf>) -> Result<Self, Error> {
        match bencode {
            Bencode::Dict(map) => {
                let mut announce: Option<String> = None;
                let mut announce_list = Vec::new();
                if let Some(bannounce) = map.get(&b"announce".to_vec()) {
                    announce = Some(String::from_utf8_lossy(bannounce.bytes()?).to_string());
                }
                if let Some(bannounce_list) = map.get(&b"announce-list".to_vec()) {
                    let tiers = bannounce_list.list()?;
                    for btier in tiers {
                        let tier_list = btier.list()?;
                        let mut tier_urls = Vec::new();

                        for burl in tier_list {
                            tier_urls.push(String::from_utf8_lossy(burl.bytes()?).to_string());
                        }

                        if !tier_urls.is_empty() {
                            announce_list.push(tier_urls);
                        }
                    }
                }
                let binfo = map
                    .get(&b"info".to_vec())
                    .ok_or_else(|| Error::LoadError("missing 'info' dict".into()))?;
                let info_hash = compute_info_hash(binfo)?;
                let info = TorrentInfo::from_bencode(binfo, destination)?;
                Ok(Torrent {
                    announce: announce,
                    announce_list: announce_list,
                    info: info,
                    info_hash: info_hash,
                })
            }
            _ => Err(Error::LoadError(String::from("bencode must be dict"))),
        }
    }

    pub fn from_file(file: &File, destination: Option<PathBuf>) -> Result<Self, Error> {
        let reader = BufReader::new(file);
        Torrent::from_reader(reader, destination)
    }

    pub fn from_reader<R: Read>(reader: R, destination: Option<PathBuf>) -> Result<Self, Error> {
        let bencode = bencode::decode_from_reader(reader)?;
        Torrent::from_bencode(bencode, destination)
    }

    pub fn write_to_disk(
        &self,
        piece_index: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.info.write_data_to_disk(piece_index, offset, data)
    }
}

fn compute_info_hash(binfo: &Bencode) -> Result<[u8; 20], Error> {
    let buffer = bencode::encode(binfo);

    let mut hasher = Sha1::new();
    hasher.update(&buffer);
    let result = hasher.finalize();

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&result[..]);
    Ok(hash)
}
