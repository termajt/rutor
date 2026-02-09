use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use rand::{rngs::ThreadRng, seq::SliceRandom};
use sha1::{Digest, Sha1};

use crate::bitfield::Bitfield;

pub const BLOCK_SIZE: usize = 16 * 1024;

type BlockKey = (usize, usize);

#[derive(Debug)]
struct BlockInFlight {
    first_requested_at: Instant,
    requested_by: HashSet<SocketAddr>,
}

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
        self.received_count >= self.received.len()
    }

    fn reset(&mut self) {
        self.received.fill(false);
        self.received_count = 0;
    }

    fn downloaded_bytes(&self) -> usize {
        let bytes = self.received_count * BLOCK_SIZE;
        bytes.min(self.length)
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    requested: HashMap<BlockKey, BlockInFlight>,
    endgame: bool,
    pieces: HashMap<usize, Piece>,
    sorted_pieces: Vec<usize>,
    dirty: bool,
    rng: ThreadRng,
    inflight_per_peer: HashMap<SocketAddr, usize>,
    max_inflight_per_peer: usize,
    verified_bytes: usize,
    total_blocks: u64,
}

impl PiecePicker {
    pub fn new(
        piece_length: usize,
        total_size: u64,
        piece_hashes: Vec<[u8; 20]>,
        max_inflight_per_peer: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut pieces = HashMap::new();

        let num_pieces = piece_hashes.len();
        let mut total_blocks = 0u64;

        for i in 0..num_pieces {
            let len = if i == num_pieces - 1 {
                total_size as usize - piece_length * (num_pieces - 1)
            } else {
                piece_length
            };
            let expected_hash: [u8; 20] = piece_hashes[i].as_slice().try_into()?;
            let piece = Piece::new(len, BLOCK_SIZE, expected_hash);
            total_blocks += piece.received.len() as u64;
            pieces.insert(i, piece);
        }
        Ok(Self {
            requested: HashMap::new(),
            endgame: false,
            pieces,
            sorted_pieces: Vec::new(),
            dirty: true,
            rng: rand::rng(),
            inflight_per_peer: HashMap::new(),
            max_inflight_per_peer,
            verified_bytes: 0,
            total_blocks,
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

        let top_n = 10.min(v.len());
        if top_n > 0 {
            v.select_nth_unstable_by(top_n - 1, |a, b| (a.1, a.2).cmp(&(b.1, b.2)));
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
            if piece.is_complete() {
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

        for (piece_idx, block_idx) in remaining_blocks {
            if free == 0 {
                break;
            }
            let piece = &self.pieces[&piece_idx];
            let offset = block_idx * BLOCK_SIZE;
            let length = (piece.length - offset).min(BLOCK_SIZE);

            let key = (piece_idx, offset);
            match self.requested.get_mut(&key) {
                Some(inflight) => {
                    if !inflight.requested_by.insert(*addr) {
                        continue;
                    }
                }
                None => {
                    let mut block_in_flight = BlockInFlight {
                        first_requested_at: now,
                        requested_by: HashSet::new(),
                    };
                    block_in_flight.requested_by.insert(*addr);
                    self.requested.insert(key, block_in_flight);
                }
            }
            *self.inflight_per_peer.entry(*addr).or_default() += 1;
            out.push(BlockRequest {
                piece: piece_idx,
                offset,
                length,
            });
            free = free.saturating_sub(1);
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
        self.rebuild_if_needed();
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
            if piece.is_complete() {
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
                let mut block_in_flight = BlockInFlight {
                    first_requested_at: now,
                    requested_by: HashSet::new(),
                };
                block_in_flight.requested_by.insert(*addr);
                self.requested.insert(key, block_in_flight);
                *self.inflight_per_peer.entry(*addr).or_default() += 1;
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
    ) -> Vec<BlockRequest> {
        let inflight = self.inflight_per_peer.get(addr).unwrap_or(&0);
        if *inflight >= self.max_inflight_per_peer {
            return Vec::new();
        }

        let free = self.max_inflight_per_peer.saturating_sub(*inflight);
        if free == 0 {
            eprintln!("no free blocks to pick");
            return Vec::new();
        }

        let now = Instant::now();

        if self.endgame {
            return self.pick_endgame_blocks(addr, peer_bf, now, free);
        } else {
            self.pick_rarest_first_blocks(addr, peer_bf, now, free)
        }
    }

    pub fn on_piece_verified(&mut self, piece_idx: usize) {
        if let Some(piece) = self.pieces.remove(&piece_idx) {
            self.verified_bytes += piece.length;
        }
        self.requested.retain(|(p, _), _| *p != piece_idx);
        self.maybe_enter_endgame();
        self.dirty = true;
    }

    pub fn on_piece_verification_failure(&mut self, piece_idx: usize) {
        self.requested.retain(|(p, _), _| *p != piece_idx);
        let piece = match self.pieces.get_mut(&piece_idx) {
            Some(p) => p,
            None => return,
        };
        piece.reset();
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

    pub fn on_peer_disconnected(&mut self, addr: &SocketAddr, bf: &Bitfield) {
        self.inflight_per_peer.remove(addr);
        let mut to_remove = Vec::new();
        for (key, inflight) in self.requested.iter_mut() {
            inflight.requested_by.remove(addr);
            if inflight.requested_by.is_empty() {
                to_remove.push(*key);
            }
        }
        for key in to_remove {
            self.requested.remove(&key);
        }
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
        let mut to_remove = Vec::new();
        for (key, inflight) in self.requested.iter_mut() {
            if now.duration_since(inflight.first_requested_at) >= timeout {
                if inflight.requested_by.remove(addr) {
                    if let Some(count) = self.inflight_per_peer.get_mut(addr) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            self.inflight_per_peer.remove(addr);
                        }
                    }
                    reaped.push(*key);
                }
            }
            if inflight.requested_by.is_empty() {
                to_remove.push(*key);
            }
        }
        for key in to_remove {
            self.requested.remove(&key);
        }

        reaped
    }

    pub fn on_block_received(
        &mut self,
        addr: &SocketAddr,
        piece_idx: usize,
        offset: usize,
    ) -> Option<PieceReceiveStatus> {
        let key = (piece_idx, offset);
        let (complete, expected_hash) = {
            let piece = self.pieces.get_mut(&piece_idx)?;

            let was_new = piece.mark_block(offset);
            if !was_new {
                return None;
            }
            (piece.is_complete(), piece.expected_hash)
        };

        if let Some(inflight) = self.requested.get_mut(&key) {
            inflight.requested_by.remove(addr);
            if inflight.requested_by.is_empty() {
                self.requested.remove(&key);
            }
        }

        if let Some(count) = self.inflight_per_peer.get_mut(addr) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.inflight_per_peer.remove(addr);
            }
        }

        self.maybe_enter_endgame();

        self.dirty = true;

        Some(PieceReceiveStatus {
            piece: piece_idx,
            received: true,
            complete: complete,
            expected_hash: expected_hash,
        })
    }

    pub fn is_endgame(&self) -> bool {
        self.endgame
    }

    pub fn in_flight(&self) -> usize {
        self.inflight_per_peer.values().sum()
    }

    pub fn opportunistic_downloaded_bytes(&self) -> u64 {
        let mut bytes = self.verified_bytes as u64;
        for piece in self.pieces.values() {
            if piece.is_complete() {
                bytes += piece.length as u64;
            } else {
                bytes += piece.downloaded_bytes() as u64;
            }
        }

        bytes
    }

    fn maybe_enter_endgame(&mut self) {
        if self.endgame {
            return;
        }

        let remaining: u64 = self
            .pieces
            .values()
            .map(|p| p.received.len() as u64 - p.received_count as u64)
            .sum();

        let remaining_ratio = remaining as f64 / self.total_blocks as f64;
        if remaining <= 50 || remaining_ratio <= 0.02 {
            self.endgame = true;
            eprintln!(
                "entering endgame: {} / {} blocks remaining ({:.2}%)",
                remaining,
                self.total_blocks,
                remaining_ratio * 100.0
            );
        }
    }
}

fn blocks_per_piece(piece_len: usize, block_size: usize) -> usize {
    (piece_len + block_size - 1) / block_size
}

pub fn verify_piece(expected_hash: [u8; 20], data: &[u8]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    result.as_slice() == expected_hash
}
