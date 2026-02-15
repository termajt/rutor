use std::{
    collections::{HashMap, HashSet},
    ops::{BitOr, BitOrAssign},
};

use mio::Token;
use rand::seq::SliceRandom;

use crate::{BLOCK_SIZE, PeerMessage, bitfield::Bitfield, engine::Tick};

const MAX_INFLIGHT_BLOCKS: usize = 512;

#[derive(Debug)]
pub struct Request {
    pub piece: u32,
    pub offset: u32,
    pub length: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PieceReceiveStatus(u8);

impl PieceReceiveStatus {
    pub const NONE: Self = Self(0);
    pub const BLOCK_RECEIVED: Self = Self(1 << 0);
    pub const PIECE_COMPLETE: Self = Self(1 << 1);

    pub fn contains(&self, other: Self) -> bool {
        self.0 & other.0 != 0
    }
}

impl BitOr for PieceReceiveStatus {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for PieceReceiveStatus {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0
    }
}

#[derive(Debug, PartialEq, Eq)]
enum BlockStatus {
    NotRequested,
    Pending,
    Received,
}

#[derive(Debug)]
struct Block {
    requested_by: HashSet<Token>,
    requested_at: Tick,
    status: BlockStatus,
}

impl Block {
    fn new() -> Self {
        Self {
            requested_by: HashSet::new(),
            requested_at: Tick(0),
            status: BlockStatus::NotRequested,
        }
    }
}

#[derive(Debug)]
struct Piece {
    blocks: Vec<Block>,
    received_count: usize,
    length: u32,
    block_size: u32,
}

impl Piece {
    fn new(length: u32, block_size: u32) -> Self {
        Self {
            blocks: generate_blocks(length, block_size),
            received_count: 0,
            length,
            block_size,
        }
    }

    fn is_complete(&self) -> bool {
        self.received_count >= self.blocks.len()
    }

    fn mark_received(&mut self, offset: u32) -> bool {
        let block_idx = offset as usize / BLOCK_SIZE;
        let block = match self.blocks.get_mut(block_idx) {
            Some(b) => b,
            None => return false,
        };
        match block.status {
            BlockStatus::NotRequested | BlockStatus::Pending => {
                block.status = BlockStatus::Received;
                self.received_count += 1;
                return true;
            }
            BlockStatus::Received => {
                return false;
            }
        }
    }

    fn reset(&mut self) {
        self.received_count = 0;
        self.blocks = generate_blocks(self.length, self.block_size);
    }

    fn downloaded_bytes(&self) -> u32 {
        let bytes = self.received_count * self.block_size as usize;
        bytes.min(self.length as usize) as u32
    }
}

#[derive(Debug)]
pub struct PiecePicker {
    bitfield: Bitfield,
    pieces: HashMap<usize, Piece>,
    availability: Vec<usize>,
    total_blocks: u64,
    endgame: bool,
    verified_bytes: u64,
    inflight_blocks: usize,
    candidates: Vec<usize>,
    verified_pieces: usize,
}

impl PiecePicker {
    pub fn new(total_pieces: usize, total_length: u64, piece_length: u32) -> Self {
        let mut pieces = HashMap::new();

        let mut total_blocks = 0u64;

        for i in 0..total_pieces {
            let len = length_for_piece(i, total_pieces, total_length, piece_length);
            let piece = Piece::new(len, BLOCK_SIZE as u32);
            total_blocks += piece.blocks.len() as u64;
            pieces.insert(i, piece);
        }
        Self {
            bitfield: Bitfield::new(total_pieces),
            pieces,
            availability: vec![0; total_pieces],
            total_blocks,
            endgame: false,
            verified_bytes: 0,
            inflight_blocks: 0,
            candidates: Vec::with_capacity(total_pieces),
            verified_pieces: 0,
        }
    }

    pub fn register_peer_bitfield(&mut self, bitfield: &Bitfield) {
        for i in 0..bitfield.len() {
            if bitfield.get(&i) {
                self.availability[i] += 1;
            }
        }
    }

    pub fn remove_peer_bitfield(&mut self, bitfield: &Bitfield) {
        for i in 0..bitfield.len() {
            if bitfield.get(&i) {
                self.availability[i] = self.availability[i].saturating_sub(1);
            }
        }
    }

    pub fn register_peer_have(&mut self, piece: u32) {
        self.availability[piece as usize] += 1;
    }

    pub fn on_block_received(&mut self, piece: u32, offset: u32) -> PieceReceiveStatus {
        let idx = piece as usize;
        let mut status = PieceReceiveStatus::NONE;
        if let Some(p) = self.pieces.get_mut(&idx) {
            self.inflight_blocks = self.inflight_blocks.saturating_sub(1);
            if p.mark_received(offset) {
                status |= PieceReceiveStatus::BLOCK_RECEIVED;
                if p.is_complete() {
                    status |= PieceReceiveStatus::PIECE_COMPLETE;
                }
            }
        }

        status
    }

    pub fn on_piece_verified(&mut self, piece: u32, verified: bool) {
        let idx = piece as usize;
        if verified {
            if let Some(p) = self.pieces.remove(&idx) {
                self.bitfield.set(&idx, true);
                self.verified_bytes += p.length as u64;
                self.verified_pieces += 1;
            }
            return;
        }
        if let Some(p) = self.pieces.get_mut(&idx) {
            self.bitfield.set(&idx, false);
            p.reset();
        }
    }

    pub fn missing_blocks_for_peer(&self, bitfield: &Bitfield) -> Vec<(u32, u32)> {
        let mut blocks = Vec::new();

        for (piece_index, piece) in self.pieces.iter() {
            if !bitfield.get(piece_index) {
                continue;
            }
            for (block_idx, block) in piece.blocks.iter().enumerate() {
                if block.status != BlockStatus::Received {
                    let offset = block_idx * BLOCK_SIZE;
                    blocks.push((*piece_index as u32, offset as u32));
                }
            }
        }

        blocks
    }

    pub fn pick_blocks(
        &mut self,
        token: Token,
        peer_bitfield: &Bitfield,
        now: Tick,
        requested_count: usize,
    ) -> Vec<PeerMessage> {
        let inflight = self.inflight_blocks;
        if inflight >= MAX_INFLIGHT_BLOCKS {
            return vec![];
        }

        let max_requestable = MAX_INFLIGHT_BLOCKS
            .saturating_sub(inflight)
            .min(requested_count);
        if max_requestable == 0 {
            return vec![];
        }

        if self.endgame {
            self.pick_endgame_blocks(token, max_requestable, peer_bitfield, now)
        } else {
            self.pick_rarest_first_blocks(token, max_requestable, peer_bitfield, now)
        }
    }

    fn pick_endgame_blocks(
        &mut self,
        token: Token,
        max_requestable: usize,
        peer_bitfield: &Bitfield,
        now: Tick,
    ) -> Vec<PeerMessage> {
        let mut requests = Vec::new();
        let mut count = max_requestable;

        self.candidates.clear();
        for (&i, _) in self.pieces.iter() {
            if peer_bitfield.get(&i) {
                self.candidates.push(i);
            }
        }
        self.candidates.shuffle(&mut rand::rng());

        for &piece_idx in &self.candidates {
            if count == 0 {
                break;
            }
            let piece = match self.pieces.get_mut(&piece_idx) {
                Some(p) => p,
                None => continue,
            };

            if piece.is_complete() {
                // Waiting for verification
                continue;
            }

            for (block_idx, block) in piece.blocks.iter_mut().enumerate() {
                if count == 0 {
                    break;
                }
                match block.status {
                    BlockStatus::NotRequested => {
                        block.requested_at = now;
                        block.status = BlockStatus::Pending;
                    }
                    BlockStatus::Pending => {
                        if block.requested_by.contains(&token) || block.requested_by.len() >= 2 {
                            continue;
                        }
                    }
                    BlockStatus::Received => continue,
                }
                self.inflight_blocks += 1;
                block.requested_by.insert(token);

                let offset = block_idx * BLOCK_SIZE;
                let length = BLOCK_SIZE.min(piece.length as usize - offset);

                requests.push(PeerMessage::Request((
                    piece_idx as u32,
                    offset as u32,
                    length as u32,
                )));

                count = count.saturating_sub(1);
            }
        }

        requests
    }

    fn pick_rarest_first_blocks(
        &mut self,
        token: Token,
        max_requestable: usize,
        peer_bitfield: &Bitfield,
        now: Tick,
    ) -> Vec<PeerMessage> {
        let mut requests = Vec::new();
        let mut count = max_requestable;

        self.candidates.clear();
        for (&i, _) in self.pieces.iter() {
            if peer_bitfield.get(&i) {
                self.candidates.push(i);
            }
        }
        self.candidates.sort_by_key(|&i| self.availability[i]);

        for &piece_idx in &self.candidates {
            if count == 0 {
                break;
            }
            let piece = match self.pieces.get_mut(&piece_idx) {
                Some(p) => p,
                None => continue,
            };

            if piece.is_complete() {
                // Waiting for verification
                continue;
            }

            for (block_idx, block) in piece.blocks.iter_mut().enumerate() {
                if count == 0 {
                    break;
                }
                match block.status {
                    BlockStatus::NotRequested => {
                        block.requested_at = now;
                        block.status = BlockStatus::Pending;
                    }
                    BlockStatus::Pending => {
                        if !block.requested_by.is_empty() {
                            continue;
                        }
                    }
                    BlockStatus::Received => continue,
                }
                self.inflight_blocks += 1;
                block.requested_by.insert(token);

                let offset = block_idx * BLOCK_SIZE;
                let length = BLOCK_SIZE.min(piece.length as usize - offset);

                requests.push(PeerMessage::Request((
                    piece_idx as u32,
                    offset as u32,
                    length as u32,
                )));

                count = count.saturating_sub(1);
            }
        }

        requests
    }

    pub fn bitfield(&self) -> &Bitfield {
        &self.bitfield
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

    pub fn requeue_blocks(&mut self, blocks: Vec<(u32, u32)>) {
        for (piece_index, offset) in blocks {
            let piece = match self.pieces.get_mut(&(piece_index as usize)) {
                Some(p) => p,
                None => continue,
            };
            let block_idx = offset as usize / BLOCK_SIZE;
            let block = &mut piece.blocks[block_idx];
            if block.status == BlockStatus::Received {
                continue;
            }
            block.status = BlockStatus::NotRequested;
            block.requested_by.clear();
            self.inflight_blocks = self.inflight_blocks.saturating_sub(1);
        }
    }

    pub fn requeue_blocks_for_peer(&mut self, token: Token) -> Vec<(u32, u32)> {
        let mut requeued = Vec::new();

        for (piece_idx, piece) in self.pieces.iter_mut() {
            for (block_idx, block) in piece.blocks.iter_mut().enumerate() {
                if block.requested_by.remove(&token) {
                    if block.status == BlockStatus::Pending && block.requested_by.is_empty() {
                        block.status = BlockStatus::NotRequested;
                        self.inflight_blocks = self.inflight_blocks.saturating_sub(1);
                        requeued.push((*piece_idx as u32, (block_idx * BLOCK_SIZE) as u32));
                    }
                }
            }
        }

        requeued
    }

    pub fn requeue_timed_out_blocks(&mut self, now: Tick, timeout: Tick) -> Vec<(u32, u32)> {
        let mut requeued = Vec::new();

        for (piece_idx, piece) in self.pieces.iter_mut() {
            for (block_idx, block) in piece.blocks.iter_mut().enumerate() {
                if block.status != BlockStatus::Pending {
                    continue;
                }
                if now >= block.requested_at + timeout {
                    block.status = BlockStatus::NotRequested;
                    block.requested_by.clear();
                    self.inflight_blocks = self.inflight_blocks.saturating_sub(1);
                    requeued.push((*piece_idx as u32, (block_idx * BLOCK_SIZE) as u32));
                }
            }
        }

        requeued
    }

    pub fn inflight_blocks(&self) -> usize {
        self.inflight_blocks
    }

    fn remaining_blocks(&self) -> u64 {
        self.pieces
            .values()
            .map(|piece| {
                piece
                    .blocks
                    .iter()
                    .filter(|b| b.status != BlockStatus::Received)
                    .count() as u64
            })
            .sum()
    }

    pub fn maybe_enter_endgame(&mut self) -> bool {
        if self.endgame {
            return true;
        }

        let remaining = self.remaining_blocks();
        if remaining > 0 && remaining <= self.inflight_blocks as u64 {
            self.endgame = true;
            log::info!(
                "entering endgame: {} / {} blocks remaining, inflight: {}",
                remaining,
                self.total_blocks,
                self.inflight_blocks
            );
            return true;
        }

        false
    }

    pub fn is_complete(&self) -> bool {
        self.pieces.is_empty()
    }

    pub fn verified_pieces(&self) -> usize {
        self.verified_pieces
    }

    pub fn is_endgame(&self) -> bool {
        self.endgame
    }
}

fn generate_blocks(piece_length: u32, block_size: u32) -> Vec<Block> {
    let block_count = (piece_length + block_size - 1) / block_size;
    let mut blocks = Vec::new();

    for _ in 0..block_count {
        blocks.push(Block::new());
    }

    blocks
}

fn length_for_piece(
    piece: usize,
    total_pieces: usize,
    total_length: u64,
    piece_length: u32,
) -> u32 {
    if piece == total_pieces - 1 {
        (total_length - piece_length as u64 * (total_pieces as u64 - 1)) as u32
    } else {
        piece_length
    }
}
