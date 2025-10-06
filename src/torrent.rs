use sha1::{Digest, Sha1};
use std::{
    fmt,
    fs::File,
    io::{BufReader, Read},
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

#[derive(Debug, Clone)]
pub struct TorrentFile {
    length: u64,
    path: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TorrentInfo {
    name: String,
    piece_length: u32,
    piece_hashes: Vec<[u8; 20]>,
    pub total_size: u64,
    files: Vec<TorrentFile>,
    bitfield: Bitfield,
}

impl TorrentInfo {
    pub fn from_bencode(bencode: &Bencode) -> Result<Self, Error> {
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
        let bitfield = Bitfield::new(piece_hashes.len());
        Ok(TorrentInfo {
            name: name,
            piece_length: piece_length,
            piece_hashes: piece_hashes,
            total_size: total_size,
            files: files,
            bitfield: bitfield,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Torrent {
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub info: TorrentInfo,
    pub info_hash: [u8; 20],
}

impl Torrent {
    pub fn new(bytes: Vec<u8>) -> Result<Self, Error> {
        let bencode = bencode::decode(&bytes)?;
        Torrent::from_bencode(bencode)
    }

    pub fn from_bencode(bencode: Bencode) -> Result<Self, Error> {
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
                let info = TorrentInfo::from_bencode(binfo)?;
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

    pub fn from_file(file: &File) -> Result<Self, Error> {
        let reader = BufReader::new(file);
        Torrent::from_reader(reader)
    }

    pub fn from_reader<R: Read>(reader: R) -> Result<Self, Error> {
        let bencode = bencode::decode_from_reader(reader)?;
        Torrent::from_bencode(bencode)
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
