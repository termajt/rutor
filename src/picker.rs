use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use rand::{rngs::ThreadRng, seq::SliceRandom};
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
    expected_hash: [u8; 20],
    length: usize,
    completed: bool,
    availability: usize,
    received_count: usize,
}

impl Piece {
    fn new(piece_len: usize, block_size: usize, expected_hash: [u8; 20]) -> Self {
        let blocks = (piece_len + block_size - 1) / block_size;
        Self {
            received: vec![false; blocks],
            expected_hash,
            length: piece_len,
            completed: false,
            availability: 0,
            received_count: 0,
        }
    }

    fn mark_block(&mut self, offset: usize) -> bool {
        let block_idx = offset / BLOCK_SIZE;
        if self.received[block_idx] {
            return false;
        }
        self.received[block_idx] = true;
        self.received_count += 1;
        true
    }

    fn is_complete(&self) -> bool {
        if self.completed {
            return true;
        }
        self.received.iter().all(|&b| b)
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    requested: HashMap<(usize, usize), (Instant, SocketAddr)>,
    endgame: bool,
    pieces: HashMap<usize, Piece>,
    sorted_pieces: Vec<usize>,
    dirty: bool,
    rng: ThreadRng,
    total_blocks: usize,
    endgame_threshold: f64,
}

impl PiecePicker {
    pub fn new(
        piece_length: usize,
        total_size: u64,
        piece_hashes: Vec<[u8; 20]>,
        endgame_threshold: f64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut pieces = HashMap::new();
        let mut total_blocks = 0;

        let num_pieces = piece_hashes.len();

        for i in 0..num_pieces {
            let len = if i == num_pieces - 1 {
                total_size as usize - piece_length * (num_pieces - 1)
            } else {
                piece_length
            };
            let expected_hash: [u8; 20] = piece_hashes[i].as_slice().try_into()?;
            let piece = Piece::new(len, BLOCK_SIZE, expected_hash);
            total_blocks += piece.received.len();
            pieces.insert(i, piece);
        }
        Ok(Self {
            requested: HashMap::new(),
            endgame: false,
            pieces,
            sorted_pieces: Vec::new(),
            dirty: true,
            rng: rand::rng(),
            total_blocks,
            endgame_threshold,
        })
    }

    fn rebuild_if_needed(&mut self) {
        if !self.dirty {
            return;
        }

        let mut v: Vec<_> = self
            .pieces
            .iter()
            .map(|(idx, p)| (*idx, p.availability, p.received_count))
            .collect();
        v.sort_by_key(|&(_, availability, received_count)| (availability, received_count));
        let top_n = 10.min(v.len());
        if top_n > 1 {
            v[..top_n].shuffle(&mut self.rng);
        }

        self.sorted_pieces = v.into_iter().map(|(idx, _, _)| idx).collect();
        self.dirty = false;
    }

    fn pick_endgame_blocks(
        &mut self,
        addr: &SocketAddr,
        peer_bf: &Bitfield,
        now: Instant,
        mut free: usize,
    ) -> Vec<BlockRequest> {
        let mut out = Vec::new();
        let mut remaining_blocks = Vec::new();
        for (piece_idx, piece) in &self.pieces {
            if piece.completed {
                continue;
            }
            if !peer_bf.get(piece_idx) {
                continue;
            }
            let blocks = blocks_per_piece(piece.length, BLOCK_SIZE);
            for block_idx in 0..blocks {
                if !piece.received[block_idx] {
                    remaining_blocks.push((*piece_idx, block_idx));
                }
            }
        }

        remaining_blocks.shuffle(&mut self.rng);

        for (piece_idx, block_idx) in remaining_blocks.into_iter().take(free) {
            let piece = &self.pieces[&piece_idx];
            let offset = block_idx * BLOCK_SIZE;
            let length = (piece.length - offset).min(BLOCK_SIZE);

            self.requested.insert((piece_idx, offset), (now, *addr));

            out.push(BlockRequest {
                piece: piece_idx,
                offset,
                length,
            });
            free = free.saturating_sub(1);
            if free == 0 {
                break;
            }
        }

        return out;
    }

    fn pick_rarest_first_blocks(
        &mut self,
        addr: &SocketAddr,
        peer_bf: &Bitfield,
        now: Instant,
        mut free: usize,
    ) -> Vec<BlockRequest> {
        let mut out = Vec::new();
        for piece_index in &self.sorted_pieces {
            if free == 0 {
                break;
            }
            if !peer_bf.get(piece_index) {
                continue;
            }

            let piece = match self.pieces.get_mut(&piece_index) {
                Some(p) => p,
                None => continue,
            };
            if piece.completed {
                continue;
            }

            let blocks = blocks_per_piece(piece.length, BLOCK_SIZE);

            for block_idx in 0..blocks {
                if free == 0 {
                    break;
                }
                if piece.received[block_idx] {
                    continue;
                }

                let offset = block_idx * BLOCK_SIZE;
                let key = (*piece_index, offset);

                if self.requested.contains_key(&key) {
                    continue;
                }

                let remaining = piece.length.saturating_sub(offset);
                let length = remaining.min(BLOCK_SIZE);

                self.requested.insert(key, (now, *addr));

                out.push(BlockRequest {
                    piece: *piece_index,
                    offset,
                    length,
                });

                free = free.saturating_sub(1);
            }
        }

        out
    }

    pub fn pick_blocks_for_peer(
        &mut self,
        addr: &SocketAddr,
        peer_bf: &Bitfield,
        free: usize,
    ) -> Vec<BlockRequest> {
        if free == 0 {
            return vec![];
        }

        let now = Instant::now();

        self.rebuild_if_needed();

        if self.endgame {
            self.pick_endgame_blocks(addr, peer_bf, now, free)
        } else {
            self.pick_rarest_first_blocks(addr, peer_bf, now, free)
        }
    }

    pub fn on_piece_verified(&mut self, piece_idx: usize) {
        self.pieces.remove(&piece_idx);
        self.dirty = true;
    }

    pub fn on_piece_verification_failure(&mut self, piece_idx: usize) {
        self.requested.retain(|(p, _), _| *p != piece_idx);
        let piece = match self.pieces.get_mut(&piece_idx) {
            Some(p) => p,
            None => return,
        };
        piece.received.fill(false);
        piece.completed = false;
        eprintln!("piece {} failed verification", piece_idx);
    }

    pub fn on_peer_bitfield(&mut self, bf: &Bitfield) {
        for piece_index in bf.get_ones() {
            if let Some(piece) = self.pieces.get_mut(&piece_index) {
                piece.availability += 1;
                self.dirty = true;
            }
        }
    }

    pub fn on_peer_have(&mut self, piece_index: &usize) {
        if let Some(piece) = self.pieces.get_mut(piece_index) {
            piece.availability += 1;
            self.dirty = true;
        }
    }

    pub fn on_peer_disconnected(&mut self, bf: &Bitfield) {
        for piece_index in bf.get_ones() {
            if let Some(piece) = self.pieces.get_mut(&piece_index) {
                piece.availability = piece.availability.saturating_sub(1);
                self.dirty = true;
            }
        }
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
                let complete = piece.is_complete();
                piece.completed = complete;
                return Some(PieceReceiveStatus {
                    piece: piece_idx,
                    received: true,
                    complete: complete,
                    expected_hash: piece.expected_hash,
                });
            }
        }

        None
    }

    pub fn update_endgame(&mut self) {
        if self.endgame {
            return;
        }

        let remaining_ratio = self.remaining_blocks() as f64 / self.total_blocks as f64;
        if remaining_ratio < self.endgame_threshold {
            self.enter_endgame();
        }
    }

    pub fn enter_endgame(&mut self) {
        if self.endgame {
            return;
        }
        self.endgame = true;
    }

    fn remaining_blocks(&self) -> usize {
        self.pieces
            .values()
            .map(|piece| piece.received.iter().filter(|&&b| !b).count())
            .sum()
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
