use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use sha1::{Digest, Sha1};

use crate::{bitfield::Bitfield, torrent::Torrent};

pub type PeerId = SocketAddr;

#[derive(Debug)]
struct PeerStats {
    avg_speed: f64,
    last_update: Instant,
}

impl PeerStats {
    const ALPHA: f64 = 0.3;

    fn new() -> Self {
        Self {
            avg_speed: 1024.0,
            last_update: Instant::now(),
        }
    }

    fn update(&mut self, block_size: usize, elapsed: Duration) {
        if elapsed.is_zero() {
            return;
        }
        let speed = block_size as f64 / elapsed.as_secs_f64();
        self.avg_speed = Self::ALPHA * speed + (1.0 - Self::ALPHA) * self.avg_speed;
        self.last_update = Instant::now();
    }

    fn expected_timeout(&self, block_size: usize) -> Duration {
        let expected_time = block_size as f64 / self.avg_speed;
        // 3x safety margin
        Duration::from_secs_f64(expected_time * 3.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockState {
    Missing,
    Requested,
    Have,
}

#[derive(Debug)]
pub struct PickerConfig {
    pub max_requests_per_peer: usize,
    pub block_size: usize,
    pub sequential: bool,
    pub endgame_threshold: usize,
}

impl Default for PickerConfig {
    fn default() -> Self {
        Self {
            max_requests_per_peer: 16,
            block_size: 16 * 1024,
            sequential: false,
            endgame_threshold: 64,
        }
    }
}

#[derive(Debug)]
struct Piece {
    index: usize,
    length: usize,
    blocks: Vec<BlockState>,
    expected_hash: [u8; 20],
    received_bytes: usize,
    received_blocks: HashMap<usize, Vec<u8>>,
    hasher: Sha1,
    next_hash_index: usize,
    verified: bool,
    expected_hash_string: String
}

impl Piece {
    fn new(index: usize, length: usize, block_size: usize, expected_hash: [u8; 20]) -> Self {
        let num_blocks = ((length as u64 + block_size as u64 - 1) / block_size as u64) as usize;
        Self {
            index: index,
            length: length,
            blocks: vec![BlockState::Missing; num_blocks],
            expected_hash: expected_hash,
            received_bytes: 0,
            received_blocks: HashMap::new(),
            hasher: Sha1::new(),
            next_hash_index: 0,
            verified: false,
            expected_hash_string: bytes_to_hex(&expected_hash)
        }
    }

    fn is_complete(&self) -> bool {
        if self.verified {
            return true;
        }
        for b in self.blocks.iter() {
            if !matches!(b, BlockState::Have) {
                return false;
            }
        }
        true
    }

    fn remaining_blocks(&self) -> usize {
        self.blocks
            .iter()
            .filter(|b| !matches!(b, BlockState::Have))
            .count()
    }

    fn add_block(&mut self, block_index: usize, data: &[u8]) -> bool {
        if matches!(self.blocks[block_index], BlockState::Have) {
            return false;
        }
        self.blocks[block_index] = BlockState::Have;
        self.received_bytes += data.len();
        self.received_blocks.insert(block_index, data.to_vec());
        self.hash_contiguous_blocks();
        true
    }

    fn hash_contiguous_blocks(&mut self) {
        while let Some(data) = self.received_blocks.remove(&self.next_hash_index) {
            self.hasher.update(&data);
            self.next_hash_index += 1;
        }
    }

    fn verify_and_reset(&mut self) -> PieceVerification {
        self.hash_contiguous_blocks();
        if self.next_hash_index != self.blocks.len() {
            return PieceVerification::new(self.length, false, &self.expected_hash_string, "".to_string(), self.index);
        }
        let hasher = std::mem::take(&mut self.hasher);
        let result = &hasher.finalize()[..];
        let ok = result == self.expected_hash;

        self.received_blocks.clear();
        self.hasher = Sha1::new();
        self.received_bytes = 0;
        self.next_hash_index = 0;
        self.verified = ok;
        if !ok {
            for b in self.blocks.iter_mut() {
                *b = BlockState::Missing;
            }
        }
        PieceVerification::new(self.length, ok, &self.expected_hash_string, bytes_to_hex(result), self.index)
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    pieces: HashMap<usize, Piece>,
    piece_count: usize,
    config: PickerConfig,
    peer_has: HashMap<PeerId, Bitfield>,
    availability: Vec<usize>,
    outstanding_requests_per_peer: HashMap<PeerId, usize>,
    duplicates: HashMap<(usize, usize), HashSet<PeerId>>,
    rarity_order: Vec<usize>,
    rarity_dirty: bool,
    peer_stats: HashMap<PeerId, PeerStats>,
    request_timeouts: HashMap<(usize, usize), Instant>,
}

impl PiecePicker {
    pub fn new(
        total_size: u64,
        piece_length: usize,
        config: PickerConfig,
        torrent: Arc<RwLock<Torrent>>,
    ) -> Self {
        let torrent = torrent.read().unwrap();
        let piece_count = ((total_size + piece_length as u64 - 1) / piece_length as u64) as usize;
        let mut pieces = HashMap::with_capacity(piece_count);
        for i in 0..piece_count {
            let offset = (i as u64) * (piece_length as u64);
            let remaining = if offset + piece_length as u64 > total_size {
                (total_size - offset) as usize
            } else {
                piece_length
            };
            pieces.insert(
                i,
                Piece::new(
                    i,
                    remaining,
                    config.block_size,
                    torrent.info.piece_hashes[i],
                ),
            );
        }
        drop(torrent);
        Self {
            pieces: pieces,
            piece_count: piece_count,
            config: config,
            peer_has: HashMap::new(),
            availability: vec![0; piece_count],
            outstanding_requests_per_peer: HashMap::new(),
            duplicates: HashMap::new(),
            rarity_order: Vec::new(),
            rarity_dirty: true,
            peer_stats: HashMap::new(),
            request_timeouts: HashMap::new(),
        }
    }

    pub fn update_peer_bitfield(&mut self, peer: &PeerId, bitfield: &Bitfield) {
        if let Some(old) = self.peer_has.get(peer) {
            for i in old.get_ones() {
                if let Some(slot) = self.availability.get_mut(i) {
                    if *slot > 0 {
                        *slot -= 1;
                    }
                }
            }
        }
        for i in bitfield.get_ones() {
            if let Some(slot) = self.availability.get_mut(i) {
                *slot += 1;
            }
        }
        self.peer_has.insert(*peer, bitfield.clone());
        self.rarity_dirty = true;
    }

    pub fn mark_block_requested(&mut self, piece_index: usize, block_index: usize, peer: &PeerId) {
        if let Some(p) = self.pieces.get_mut(&piece_index) {
            p.blocks[block_index] = BlockState::Requested;
            self.request_timeouts
                .insert((piece_index, block_index), Instant::now());
            self.outstanding_requests_per_peer
                .entry(*peer)
                .and_modify(|v| *v += 1)
                .or_insert(1);
            self.duplicates
                .entry((piece_index, block_index))
                .or_default()
                .insert(*peer);
        }
    }

    pub fn handle_block_received(
        &mut self,
        received_from: &PeerId,
        piece_index: usize,
        offset: usize,
        data: &[u8],
    ) -> (Vec<PeerId>, bool) {
        let mut cancel_peers = Vec::new();
        let mut received = false;
        if let Some(p) = self.pieces.get_mut(&piece_index) {
            let block_index = offset / self.config.block_size;
            received = p.add_block(block_index, data);
            if received {
                if let Some(start_time) = self.request_timeouts.remove(&(piece_index, block_index))
                {
                    let elapsed = start_time.elapsed();
                    self.peer_stats
                        .entry(*received_from)
                        .or_insert_with(PeerStats::new)
                        .update(data.len(), elapsed);
                }
            }
            if let Some(peers) = self.duplicates.remove(&(piece_index, block_index)) {
                for peer in peers {
                    if &peer != received_from {
                        cancel_peers.push(peer);
                    }
                }
            }
            if let Some(cnt) = self.outstanding_requests_per_peer.get_mut(received_from) {
                *cnt = cnt.saturating_sub(1);
                if *cnt == 0 {
                    self.outstanding_requests_per_peer.remove(received_from);
                }
            }
            self.request_timeouts.remove(&(piece_index, block_index));
        }
        (cancel_peers, received)
    }

    pub fn verify_piece(&mut self, piece_index: usize) -> Result<PieceVerification, Box<dyn std::error::Error>> {
        let Some(p) = self.pieces.get_mut(&piece_index) else {
            return Err(format!("no piece {}, already verified", piece_index).into());
        };
        if !p.is_complete() {
            return Err(format!("piece {} is not complete yet", piece_index).into());
        }
        let verification = p.verify_and_reset();
        if verification.verified {
            self.pieces.remove(&piece_index);
        }
        self.rarity_dirty = true;
        Ok(verification)
    }

    pub fn total_remaining_blocks(&self) -> usize {
        self.pieces.values().map(|p| p.remaining_blocks()).sum()
    }

    fn total_outstanding_requests(&self) -> usize {
        self.outstanding_requests_per_peer.values().sum()
    }

    fn build_rarity_order(&mut self) {
        if !self.rarity_dirty {
            return;
        }
        let mut items: Vec<(usize, usize)> = Vec::with_capacity(self.piece_count);
        for (_, p) in &self.pieces {
            if p.is_complete() {
                continue;
            }
            let avail = self.availability[p.index];
            items.push((avail, p.index));
        }
        items.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        self.rarity_order = items.into_iter().map(|(_, idx)| idx).collect();
        self.rarity_dirty = false;
    }

    pub fn cleanup_timed_out_requests(&mut self) {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (&(piece_index, block_index), &start_time) in &self.request_timeouts {
            if let Some(peers) = self.duplicates.get(&(piece_index, block_index)) {
                for peer in peers {
                    let expected_timeout = self
                        .peer_stats
                        .get(peer)
                        .map(|s| s.expected_timeout(self.config.block_size))
                        .unwrap_or(Duration::from_secs(10));
                    if now.duration_since(start_time) >= expected_timeout {
                        timed_out.push((piece_index, block_index));
                        break;
                    }
                }
            }
        }

        for (piece_index, block_index) in timed_out {
            self.request_timeouts.remove(&(piece_index, block_index));
            if let Some(peers) = self.duplicates.remove(&(piece_index, block_index)) {
                for peer in peers {
                    if let Some(cnt) = self.outstanding_requests_per_peer.get_mut(&peer) {
                        *cnt = cnt.saturating_sub(1);
                        if *cnt == 0 {
                            self.outstanding_requests_per_peer.remove(&peer);
                        }
                    }
                }
            }

            if let Some(piece) = self.pieces.get_mut(&piece_index) {
                if !matches!(piece.blocks[block_index], BlockState::Have) {
                    piece.blocks[block_index] = BlockState::Missing;
                }
            }
        }
    }

    pub fn peer_disconnected(&mut self, peer: &PeerId) {
        self.outstanding_requests_per_peer.remove(peer);
        for set in self.duplicates.values_mut() {
            set.remove(peer);
        }
        self.peer_stats.remove(peer);
    }

    fn calculate_endgame_threshold(&self, connected_peers: usize) -> usize {
        let min_pieces = 4;
        let peer_factor = (connected_peers * 2).max(min_pieces);
        let dynamic = (self.piece_count as f32 * 0.05) as usize;
        peer_factor.max(dynamic).min(64)
    }

    pub fn pick_requests_for_peers(
        &mut self,
        peers: &[PeerId],
        max_outstanding_requests: usize,
    ) -> HashMap<PeerId, Vec<(u32, u32, u32)>> {
        let mut results: HashMap<SocketAddr, Vec<(u32, u32, u32)>> = HashMap::new();
        if peers.is_empty() {
            return results;
        }
        let total_outstanding = self.total_outstanding_requests();
        if total_outstanding >= max_outstanding_requests {
            return results;
        }

        let mut global_remaining = max_outstanding_requests - total_outstanding;
        if global_remaining == 0 {
            return results;
        }

        let mut sorted_peers = peers.to_vec();
        let remaining = self.total_remaining_blocks();
        let endgame_threshold = self.calculate_endgame_threshold(peers.len());
        if remaining <= endgame_threshold {
            // When very few pieces remain, switch to endgame mode,
            // prioritize peers that can deliver the fastest (lowest expected timeout),
            // to minimize the risk of waiting on slow peers for final pieces.
            sorted_peers.sort_by(|a, b| {
                // Estimate how long each peer would take to deliver one block.
                // If stats are missing, assume a conservative 10-second timeout.
                let ta = self
                    .peer_stats
                    .get(a)
                    .map(|s| s.expected_timeout(self.config.block_size))
                    .unwrap_or(Duration::from_secs(10));
                let tb = self
                    .peer_stats
                    .get(b)
                    .map(|s| s.expected_timeout(self.config.block_size))
                    .unwrap_or(Duration::from_secs(10));

                // Lower timeout = better (asc order)
                ta.cmp(&tb)
            });
        } else {
            // Normal operating mode:
            // Balance between throughput and fairness using a weighted priority.
            // Peers with higher avg_speed and fewer outstanding requests are favored,
            // but slow peers still receive some work.
            sorted_peers.sort_by(|a, b| {
                // Historical average download speeds (bytes/sec),
                // fall back to a default baseline if unknown.
                let sa = self
                    .peer_stats
                    .get(a)
                    .map(|s| s.avg_speed)
                    .unwrap_or(1024.0);
                let sb = self
                    .peer_stats
                    .get(b)
                    .map(|s| s.avg_speed)
                    .unwrap_or(1024.0);
                let ra = *self.outstanding_requests_per_peer.get(a).unwrap_or(&0);
                let rb = *self.outstanding_requests_per_peer.get(b).unwrap_or(&0);

                // Weighted priority: higher speed and fewer outstanding = better
                let pa = sa / (1.0 + ra as f64);
                let pb = sb / (1.0 + rb as f64);

                // Sort descending: higher priority peers first.
                pb.partial_cmp(&pa).unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        let max_dups = if remaining <= endgame_threshold {
            4
        } else if remaining <= endgame_threshold * 4 {
            2
        } else {
            1
        };
        self.build_rarity_order();

        for peer in sorted_peers {
            if global_remaining == 0 {
                break;
            }

            let peer_has = match self.peer_has.get(&peer) {
                Some(bf) => bf.clone(),
                None => continue,
            };

            let peer_outstanding = *self.outstanding_requests_per_peer.get(&peer).unwrap_or(&0);
            if peer_outstanding >= max_outstanding_requests {
                continue;
            }

            let peer_remaining = max_outstanding_requests - peer_outstanding;
            let can_send = std::cmp::min(peer_remaining, global_remaining);
            if can_send == 0 {
                continue;
            }

            let mut picked = Vec::new();
            let mut to_mark = Vec::new();

            'outer: for &pidx in &self.rarity_order {
                let Some(piece) = self.pieces.get_mut(&pidx) else {
                    continue;
                };
                if !peer_has.get(&pidx) || piece.is_complete() {
                    continue;
                }

                for b_idx in 0..piece.blocks.len() {
                    if picked.len() >= can_send {
                        break 'outer;
                    }

                    match piece.blocks[b_idx] {
                        BlockState::Have => continue,
                        BlockState::Requested => {
                            let key = (pidx, b_idx);
                            if let Some(peers) = self.duplicates.get(&key) {
                                if peers.contains(&peer) {
                                    continue;
                                }
                                if peers.len() >= max_dups {
                                    continue;
                                }
                            }
                        }
                        BlockState::Missing => {}
                    }

                    let block_size = self.config.block_size;
                    let begin = (b_idx as u32) * block_size as u32;
                    let remaining_in_piece = piece.length.saturating_sub(begin as usize);
                    let len = std::cmp::min(block_size, remaining_in_piece);
                    picked.push((pidx as u32, begin, len as u32));
                    to_mark.push((pidx, b_idx));
                }
            }

            for (pidx, b_idx) in to_mark {
                self.mark_block_requested(pidx, b_idx, &peer);
            }

            if !picked.is_empty() {
                global_remaining = global_remaining.saturating_sub(picked.len());
                results.insert(peer, picked);
            }
        }

        results
    }
}

#[derive(Debug)]
pub struct PieceVerification {
    pub length: usize,
    pub verified: bool,
    pub expected_hash: String,
    pub actual_hash: String,
    pub piece_index: usize
}

impl PieceVerification {
    pub fn new(length: usize, verified: bool, expected_hash: &String, actual_hash: String, piece_index: usize) -> Self {
        PieceVerification { length, verified, expected_hash: expected_hash.to_string(), actual_hash, piece_index }
    }
}

fn bytes_to_hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}
