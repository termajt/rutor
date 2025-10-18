use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
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
            avg_speed: 16_000.0,
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
}

impl Piece {
    fn new(index: usize, length: usize, block_size: usize, expected_hash: [u8; 20]) -> Self {
        let num_blocks = ((length as u64 + block_size as u64 - 1) / block_size as u64) as usize;
        Self {
            index: index,
            length: length,
            blocks: vec![BlockState::Missing; num_blocks],
            expected_hash: expected_hash,
        }
    }

    fn is_complete(&self) -> bool {
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
}

#[derive(Debug)]
pub struct PiecePicker {
    pieces: Vec<Piece>,
    piece_count: usize,
    config: PickerConfig,
    peer_has: HashMap<PeerId, Bitfield>,
    availability: Vec<usize>,
    outstanding_requests_per_peer: HashMap<PeerId, usize>,
    duplicates: HashMap<(usize, usize), HashSet<PeerId>>,
    piece_data: HashMap<usize, Vec<u8>>,
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
        torrent: Arc<Torrent>,
    ) -> Self {
        let piece_count = ((total_size + piece_length as u64 - 1) / piece_length as u64) as usize;
        let mut pieces = Vec::with_capacity(piece_count);
        for i in 0..piece_count {
            let offset = (i as u64) * (piece_length as u64);
            let remaining = if offset + piece_length as u64 > total_size {
                (total_size - offset) as usize
            } else {
                piece_length
            };
            pieces.push(Piece::new(
                i,
                remaining,
                config.block_size,
                torrent.info.piece_hashes[i],
            ));
        }
        Self {
            pieces: pieces,
            piece_count: piece_count,
            config: config,
            peer_has: HashMap::new(),
            availability: vec![0; piece_count],
            outstanding_requests_per_peer: HashMap::new(),
            duplicates: HashMap::new(),
            piece_data: HashMap::new(),
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
        if let Some(p) = self.pieces.get_mut(piece_index) {
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
        let block_index = offset / self.config.block_size;
        let p = &mut self.pieces[piece_index];
        let mut received = false;
        if !matches!(p.blocks[block_index], BlockState::Have) {
            p.blocks[block_index] = BlockState::Have;
            let buf = self
                .piece_data
                .entry(piece_index)
                .or_insert_with(|| vec![0; p.length]);
            buf[offset..offset + data.len()].copy_from_slice(data);
            if let Some(start_time) = self.request_timeouts.remove(&(piece_index, block_index)) {
                let elapsed = start_time.elapsed();
                self.peer_stats
                    .entry(*received_from)
                    .or_insert_with(PeerStats::new)
                    .update(data.len(), elapsed);
            }
            received = true;
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
        (cancel_peers, received)
    }

    pub fn verify_piece(&mut self, piece_index: usize) -> Result<bool, Box<dyn std::error::Error>> {
        let Some(p) = self.pieces.get_mut(piece_index) else {
            return Err(format!("no piece for index: {}", piece_index).into());
        };
        if !p.is_complete() {
            return Err(format!("piece {} is not complete yet", piece_index).into());
        }
        let Some(buf) = self.piece_data.remove(&piece_index) else {
            return Err(format!("no buffer for piece: {}", piece_index).into());
        };
        let mut hasher = Sha1::new();
        hasher.update(buf);
        let result = hasher.finalize();
        self.rarity_dirty = true;
        if result[..] == p.expected_hash {
            return Ok(true);
        } else {
            for i in 0..p.blocks.len() {
                p.blocks[i] = BlockState::Missing;
            }
        }
        Ok(false)
    }

    pub fn total_remaining_blocks(&self) -> usize {
        self.pieces.iter().map(|p| p.remaining_blocks()).sum()
    }

    fn total_outstanding_requests(&self) -> usize {
        self.outstanding_requests_per_peer.values().sum()
    }

    fn build_rarity_order(&mut self) {
        if !self.rarity_dirty {
            return;
        }
        let mut items: Vec<(usize, usize)> = Vec::with_capacity(self.piece_count);
        for p in &self.pieces {
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

            if let Some(piece) = self.pieces.get_mut(piece_index) {
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

    pub fn pick_requests_for_peers(
        &mut self,
        peers: &[PeerId],
        max_outstanding_requests: usize,
    ) -> HashMap<PeerId, Vec<(u32, u32, u32)>> {
        let mut results: HashMap<SocketAddr, Vec<(u32, u32, u32)>> = HashMap::new();
        let mut sorted_peers = peers.to_vec();
        sorted_peers
            .sort_by_key(|peer| *self.outstanding_requests_per_peer.get(peer).unwrap_or(&0));

        let total_outstanding = self.total_outstanding_requests();
        if total_outstanding >= max_outstanding_requests {
            return results;
        }

        let mut global_remaining = max_outstanding_requests - total_outstanding;
        if global_remaining == 0 {
            return results;
        }

        let remaining = self.total_remaining_blocks();
        let max_dups = if remaining <= 8 {
            4
        } else if remaining <= 32 {
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
                if !peer_has.get(&pidx) || self.pieces[pidx].is_complete() {
                    continue;
                }

                let piece = &mut self.pieces[pidx];
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
