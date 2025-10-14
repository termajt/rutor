use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    usize,
};

use rand::seq::IndexedRandom;
use sha1::{Digest, Sha1};

use crate::{
    bitfield::Bitfield,
    consts::{self, ClientEvent, PeerEvent, PieceEvent},
    peer::{PeerManager, PeerMessage},
    pubsub::PubSub,
    torrent::Torrent,
};

const BLOCK_SIZE: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlockState {
    Missing,
    Requested,
    Received,
}

#[derive(Debug)]
struct BlockInfo {
    index: u32,
    begin: u32,
    length: u32,
    state: BlockState,
    requested_by: HashMap<SocketAddr, Instant>,
}

impl BlockInfo {
    fn new(index: u32, begin: u32, length: u32) -> Self {
        Self {
            index: index,
            begin: begin,
            length: length,
            state: BlockState::Missing,
            requested_by: HashMap::new(),
        }
    }

    fn is_expired(&self, peer_speeds: &HashMap<SocketAddr, f64>) -> bool {
        self.requested_by.iter().any(|(addr, &req_time)| {
            let speed = peer_speeds.get(addr).copied().unwrap_or(16_384.0);
            Instant::now().duration_since(req_time)
                > dynamic_expiration(self.length as usize, speed)
        })
    }
}

fn dynamic_expiration(block_size: usize, peer_speed: f64) -> Duration {
    let seconds = (block_size as f64 / peer_speed).ceil() as u64;
    Duration::from_secs(seconds.max(10))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceState {
    Missing,
    Downloading,
    Complete,
    Verified,
}

#[derive(Debug)]
pub struct PieceData {
    pub buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct PieceManager {
    bitfield: Mutex<Bitfield>,
    pieces: Vec<PieceState>,
    torrent: Arc<Torrent>,
    piece_availability_cache: Option<Vec<usize>>,
    piece_states: Vec<Vec<BlockInfo>>,
    piece_data: Vec<Option<PieceData>>,
    peer_manager: Arc<PeerManager>,
    peer_event_tx: Arc<PubSub<PeerEvent>>,
    client_event_tx: Arc<PubSub<ClientEvent>>,
    blocks_per_piece: usize,
}

impl PieceManager {
    /// Creates a new `PieceManager` from the given torrent metadata.
    pub fn new(
        torrent: Arc<Torrent>,
        peer_manager: Arc<PeerManager>,
        peer_event_tx: Arc<PubSub<PeerEvent>>,
        client_event_tx: Arc<PubSub<ClientEvent>>,
    ) -> Self {
        let total_pieces = torrent.info.piece_hashes.len();
        let total_size = torrent.info.total_size;
        let piece_length = torrent.info.piece_length;
        let mut piece_states = Vec::new();
        //let message_rx = command_tx.subscribe(consts::TOPIC_TORRENT_COMMAND);
        for piece_index in 0..total_pieces {
            let piece_size = if piece_index == total_pieces - 1 {
                (total_size as u32) - (piece_index as u32 * piece_length)
            } else {
                piece_length
            };
            let mut blocks = Vec::new();
            let mut begin = 0u32;
            while begin < piece_size {
                let len = std::cmp::min(BLOCK_SIZE as u32, piece_size - begin);
                blocks.push(BlockInfo::new(piece_index as u32, begin, len));
                begin += len;
            }
            piece_states.push(blocks);
        }
        let blocks_per_piece = piece_length as usize / BLOCK_SIZE;
        PieceManager {
            bitfield: Mutex::new(Bitfield::new(total_pieces)),
            pieces: vec![PieceState::Missing; total_pieces],
            torrent: torrent,
            piece_availability_cache: None,
            piece_states: piece_states,
            piece_data: (0..total_pieces).map(|_| None).collect(),
            peer_manager: peer_manager,
            peer_event_tx: peer_event_tx,
            client_event_tx: client_event_tx,
            blocks_per_piece: blocks_per_piece,
        }
    }

    fn total_pieces(&self) -> usize {
        self.torrent.info.piece_hashes.len()
    }

    fn mark_block_received(&mut self, index: u32, begin: u32, data: &[u8]) {
        if let Some(blocks) = self.piece_states.get_mut(index as usize) {
            if let Some(block) = blocks.iter_mut().find(|b| b.begin == begin) {
                if block.state == BlockState::Received {
                    return;
                }
                block.state = BlockState::Received;
                self.peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::SendToAll {
                        message: PeerMessage::Cancel((index, block.begin, block.length)),
                    },
                );
                self.client_event_tx.publish(
                    consts::TOPIC_CLIENT_EVENT,
                    ClientEvent::BytesDownloaded {
                        data_size: data.len(),
                    },
                );
            }

            if let Some(ref mut pdata) = self.piece_data[index as usize] {
                let offset = begin as usize;
                pdata.buffer[offset..offset + data.len()].copy_from_slice(data);
            }

            if blocks.iter().all(|b| b.state == BlockState::Received) {
                self.pieces[index as usize] = PieceState::Complete;
                self.verify_piece(index as usize);
            }
        }
    }

    fn verify_piece(&mut self, index: usize) {
        if index >= self.total_pieces() {
            return;
        }
        let expected_hash = self.torrent.info.piece_hashes[index];
        if let Some(ref pdata) = self.piece_data[index] {
            let mut hasher = Sha1::new();
            hasher.update(&pdata.buffer);
            let result = hasher.finalize();

            if result[..] == expected_hash {
                self.pieces[index] = PieceState::Verified;
                {
                    let mut bitfield = self.bitfield.lock().unwrap();
                    bitfield.set(index, true);
                }
                if let Err(e) = self.torrent.write_to_disk(index, &pdata.buffer) {
                    eprintln!("❌ Piece {index} could not be written to disk: {e}");
                }
                self.peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::SendToAll {
                        message: PeerMessage::Have(index as u32),
                    },
                );
            } else {
                eprintln!("❌ Piece {index} failed verification");
                self.client_event_tx.publish(
                    consts::TOPIC_CLIENT_EVENT,
                    ClientEvent::PieceVerificationFailure {
                        piece_index: index,
                        data_size: pdata.buffer.len(),
                    },
                );
                self.pieces[index] = PieceState::Missing;
            }
            self.piece_data[index] = None;
        }
    }

    pub fn is_complete(&self) -> bool {
        let bitfield = self.bitfield.lock().unwrap();
        bitfield.count_ones() == self.total_pieces()
    }

    pub fn pieces_left(&self) -> usize {
        let bitfield = self.bitfield.lock().unwrap();
        bitfield.count_zeros()
    }

    pub fn pieces_verified(&self) -> usize {
        let bitfield = self.bitfield.lock().unwrap();
        bitfield.count_ones()
    }

    fn piece_availability(&mut self) -> Vec<usize> {
        if let Some(ref cached) = self.piece_availability_cache {
            return cached.clone();
        }
        let mut counts = vec![0; self.total_pieces()];
        let connected = self.peer_manager.connected.read().unwrap();
        for pc in connected.values() {
            for (i, byte) in pc.bitfield.as_bytes().iter().enumerate() {
                for bit_offset in 0..8 {
                    let index = i * 8 + bit_offset;
                    if index >= self.total_pieces() {
                        break;
                    }
                    if (byte & (1 << (7 - bit_offset))) != 0 {
                        counts[index] += 1;
                    }
                }
            }
        }
        self.piece_availability_cache = Some(counts.clone());
        counts
    }

    fn piece_length_for_index(&self, index: usize) -> usize {
        if index == (self.total_pieces() - 1) {
            let remainder = self.torrent.info.total_size % self.torrent.info.piece_length as u64;
            if remainder == 0 {
                self.torrent.info.piece_length as usize
            } else {
                remainder as usize
            }
        } else {
            self.torrent.info.piece_length as usize
        }
    }

    fn calculate_throttle(&self) -> (usize, usize) {
        let connected_peers = self.peer_manager.connected_peers();
        // let max_pieces = (connected_peers + self.blocks_per_piece - 1) / self.blocks_per_piece;
        let max_pieces = std::cmp::min(connected_peers / 2, 20);
        let max_outstanding_per_piece = std::cmp::min(self.blocks_per_piece, connected_peers / 2);
        (max_pieces.max(1), max_outstanding_per_piece.max(1))
    }

    fn select_pieces(&mut self) {
        let availability = self.piece_availability();

        let mut candidates: Vec<_> = self
            .pieces
            .iter()
            .enumerate()
            .filter(|(_, state)| matches!(state, PieceState::Missing | PieceState::Downloading))
            .map(|(i, _)| i)
            .collect();

        candidates.sort_by_key(|&i| availability[i]);

        let (max_active_pieces, max_outstanding_per_piece) = self.calculate_throttle();
        let mut active_indicies = Vec::new();
        let mut downloading_count = self
            .pieces
            .iter()
            .filter(|s| **s == PieceState::Downloading)
            .count();
        for &i in &candidates {
            if self.pieces[i] == PieceState::Downloading {
                active_indicies.push(i);
            } else if downloading_count < max_active_pieces {
                active_indicies.push(i);
                downloading_count += 1;
            } else {
                break;
            }
        }

        let peer_speeds = self.peer_manager.get_download_speeds();

        for index in active_indicies {
            let peers = self.peer_manager.peers_with_piece(index);
            if peers.is_empty() {
                continue;
            }

            let available_peers: Vec<_> = peers
                .iter()
                .filter(|addr| !self.peer_manager.is_choked(addr))
                .collect();

            if available_peers.is_empty() {
                continue;
            }

            let maybe_peer = available_peers.choose(&mut rand::rng());

            let Some(peer) = maybe_peer else {
                continue;
            };

            let piece_len = self.piece_length_for_index(index);
            let blocks = &mut self.piece_states[index];
            let mut sent_this_round = 0;
            for block in blocks.iter_mut() {
                if block.state == BlockState::Received {
                    continue;
                }
                if block.is_expired(&peer_speeds) {
                    block.state = BlockState::Missing;
                    block.requested_by.clear();
                }
                if block.state == BlockState::Requested && block.requested_by.contains_key(peer) {
                    continue;
                }
                if sent_this_round >= max_outstanding_per_piece {
                    break;
                }

                block.state = BlockState::Requested;
                block.requested_by.insert(**peer, Instant::now());

                if self.pieces[index] == PieceState::Missing {
                    self.pieces[index] = PieceState::Downloading;
                }

                if self.piece_data[index].is_none() {
                    self.piece_data[index] = Some(PieceData {
                        buffer: vec![0u8; piece_len],
                    });
                }

                self.peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::Send {
                        addr: **peer,
                        message: PeerMessage::Request((block.index, block.begin, block.length)),
                    },
                );

                sent_this_round += 1;
            }
        }
    }

    pub fn handle_event(&mut self, ev: Arc<PieceEvent>) {
        match &*ev {
            PieceEvent::BlockData {
                piece_index,
                begin,
                data,
            } => {
                self.mark_block_received(*piece_index, *begin, &data);
                self.select_pieces();
            }
            PieceEvent::PieceAvailabilityChange => {
                self.piece_availability_cache = None;
                self.select_pieces();
            }
            PieceEvent::PeerChokeChanged { addr: _, choked } => {
                if !choked {
                    self.select_pieces();
                }
            }
        }
    }
}
