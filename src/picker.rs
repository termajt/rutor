use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use sha1::{Digest, Sha1};

use crate::{bitfield::Bitfield, disk::DiskManager};

pub const BLOCK_SIZE: usize = 16 * 1024;

#[derive(Debug)]
pub struct PieceReceiveStatus {
    pub piece: usize,
    pub received: bool,
    pub complete: bool,
    pub expected_hash: [u8; 20],
}

#[derive(Debug)]
pub struct BlockRequest {
    pub piece: usize,
    pub offset: usize,
    pub length: usize,
}

#[derive(Debug)]
struct Piece {
    received: Vec<bool>,
    completed: bool,
    expected_hash: [u8; 20],
    length: usize,
}

impl Piece {
    fn new(piece_len: usize, block_size: usize, expected_hash: [u8; 20]) -> Self {
        let blocks = (piece_len + block_size - 1) / block_size;
        Self {
            received: vec![false; blocks],
            completed: false,
            expected_hash,
            length: piece_len,
        }
    }

    fn mark_block(&mut self, offset: usize) -> bool {
        let block_idx = offset / BLOCK_SIZE;
        if self.received[block_idx] {
            return false;
        }
        self.received[block_idx] = true;
        true
    }

    fn is_complete(&self) -> bool {
        self.received.iter().all(|&b| b)
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    requested: HashMap<(usize, usize), (Instant, SocketAddr)>,
    endgame: bool,
    pieces: HashMap<usize, Piece>,
}

impl PiecePicker {
    pub fn new(
        piece_length: usize,
        total_size: u64,
        piece_hashes: Vec<[u8; 20]>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut pieces = HashMap::new();

        let num_pieces = piece_hashes.len();

        for i in 0..num_pieces {
            let len = if i == num_pieces - 1 {
                total_size as usize - piece_length * (num_pieces - 1)
            } else {
                piece_length
            };
            let expected_hash: [u8; 20] = piece_hashes[i].as_slice().try_into()?;
            pieces.insert(i, Piece::new(len, BLOCK_SIZE, expected_hash));
        }
        Ok(Self {
            requested: HashMap::new(),
            endgame: false,
            pieces,
        })
    }

    pub fn pick_blocks_for_peer(
        &mut self,
        addr: &SocketAddr,
        peer_bf: &Bitfield,
        my_bf: &Bitfield,
        mut free: usize,
    ) -> Vec<BlockRequest> {
        let mut out = Vec::new();

        if free == 0 {
            return out;
        }

        let now = Instant::now();
        for piece_idx in peer_bf.get_ones() {
            if free == 0 {
                break;
            }

            if my_bf.get(&piece_idx) {
                continue;
            }

            let piece = match self.pieces.get(&piece_idx) {
                Some(p) => p,
                None => continue,
            };

            if piece.completed || piece.is_complete() {
                continue;
            }

            let blocks = blocks_per_piece(piece.length, BLOCK_SIZE);

            for block_idx in 0..blocks {
                if free == 0 {
                    break;
                }

                let offset = block_idx * BLOCK_SIZE;
                let key = (piece_idx, offset);

                if self.requested.contains_key(&key) && !self.endgame {
                    continue;
                }

                let remaining = piece.length.saturating_sub(offset);
                let length = remaining.min(BLOCK_SIZE);

                self.requested.insert(key, (now, addr.clone()));

                out.push(BlockRequest {
                    piece: piece_idx,
                    offset,
                    length,
                });

                free -= 1;
            }
        }

        out
    }

    pub fn on_piece_verified(&mut self, piece_idx: usize) {
        self.pieces.remove(&piece_idx);
    }

    pub fn on_piece_verification_failure(&mut self, piece_idx: usize) {
        let piece = match self.pieces.get_mut(&piece_idx) {
            Some(p) => p,
            None => return,
        };
        piece.completed = false;
        piece.received.fill(false);
    }

    pub fn reap_timeouts_for_peer(
        &mut self,
        addr: &SocketAddr,
        timeout: Duration,
    ) -> Vec<(usize, usize)> {
        let now = Instant::now();
        let mut reaped = Vec::new();
        self.requested.retain(|(piece, offset), &mut (t, owner)| {
            if owner != *addr {
                return true; // keep blocks requested from other peers
            }
            let keep = now.duration_since(t) < timeout;
            if !keep {
                reaped.push((*piece, *offset));
            }
            keep
        });

        reaped
    }

    pub fn on_block_received(
        &mut self,
        piece_idx: usize,
        offset: usize,
    ) -> Option<PieceReceiveStatus> {
        self.requested.remove(&(piece_idx, offset));
        if let Some(piece) = self.pieces.get_mut(&piece_idx) {
            if piece.mark_block(offset) {
                return Some(PieceReceiveStatus {
                    piece: piece_idx,
                    received: true,
                    complete: piece.is_complete(),
                    expected_hash: piece.expected_hash,
                });
            }
        }

        None
    }

    pub fn update_endgame(&mut self, remaining_blocks: usize) {
        self.endgame = remaining_blocks < 20;
    }

    pub fn enter_endgame(&mut self) {
        self.endgame = true;
    }
}

fn blocks_per_piece(piece_len: usize, block_size: usize) -> usize {
    (piece_len + block_size - 1) / block_size
}

pub fn verify_piece(piece: usize, expected_hash: [u8; 20], disk_mgr: &DiskManager) -> bool {
    match disk_mgr.read_piece(piece) {
        Ok(data) => {
            let mut hasher = Sha1::new();
            hasher.update(&data);
            let result = hasher.finalize();
            result.as_slice() == expected_hash
        }
        Err(e) => {
            eprintln!(
                "failed to read piece {} for verification: {}",
                piece,
                e.to_string()
            );
            false
        }
    }
}
