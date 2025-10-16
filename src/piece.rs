use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
    usize,
};

use rand::seq::IndexedRandom;
use sha1::{Digest, Sha1};

use crate::{
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
    begin: u32,
    length: u32,
    state: BlockState,
    requested_by: HashMap<SocketAddr, Instant>,
}

impl BlockInfo {
    fn new(begin: u32, length: u32) -> Self {
        Self {
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
struct Piece {
    index: usize,
    state: PieceState,
    blocks: Vec<BlockInfo>,
    expected_hash: [u8; 20],
    hasher: Sha1,
    next_block_offset: usize,
    out_of_order_blocks: BTreeMap<u32, Vec<u8>>,
}

impl Piece {
    fn new(index: usize, piece_size: usize, expected_hash: [u8; 20]) -> Self {
        let mut blocks = Vec::new();
        let mut begin = 0;
        while begin < piece_size {
            let len = std::cmp::min(BLOCK_SIZE, piece_size - begin);
            blocks.push(BlockInfo::new(begin as u32, len as u32));
            begin += len;
        }

        Self {
            index: index,
            state: PieceState::Missing,
            blocks: blocks,
            expected_hash: expected_hash,
            hasher: Sha1::new(),
            next_block_offset: 0,
            out_of_order_blocks: BTreeMap::new(),
        }
    }

    fn mark_block_received(&mut self, begin: u32, data: &[u8]) -> bool {
        if let Some(block) = self.blocks.iter_mut().find(|b| b.begin == begin) {
            if matches!(block.state, BlockState::Received) {
                return false;
            }
            block.state = BlockState::Received;
            if begin == self.next_block_offset as u32 {
                self.hasher.update(data);
                self.next_block_offset += data.len();

                while let Some(buffered) = self
                    .out_of_order_blocks
                    .remove(&(self.next_block_offset as u32))
                {
                    self.hasher.update(&buffered);
                    self.next_block_offset += buffered.len();
                }
            } else {
                self.out_of_order_blocks.insert(begin, data.to_vec());
            }

            if self.blocks.iter().all(|b| b.state == BlockState::Received) {
                self.state = PieceState::Complete;
            }
            return true;
        }
        false
    }

    fn verify(&mut self) -> bool {
        let hash_result = self.hasher.clone().finalize();
        let valid = hash_result.as_slice() == self.expected_hash;
        if valid {
            self.state = PieceState::Verified;
        } else {
            self.state = PieceState::Missing;
            for block in self.blocks.iter_mut() {
                block.state = BlockState::Missing;
            }
        }
        self.hasher.reset();
        self.next_block_offset = 0;
        self.out_of_order_blocks.clear();
        valid
    }
}

#[derive(Debug)]
pub struct PieceManager {
    pieces: Vec<Piece>,
    torrent: Arc<Torrent>,
    piece_availability_cache: Option<Vec<usize>>,
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
        let mut pieces = Vec::new();
        for piece_index in 0..total_pieces {
            let piece_size = if piece_index == total_pieces - 1 {
                (total_size as usize) - (piece_index as usize * piece_length as usize)
            } else {
                piece_length as usize
            };
            let expected_hash = torrent.info.piece_hashes[piece_index];
            pieces.push(Piece::new(piece_index, piece_size, expected_hash));
        }
        let blocks_per_piece = piece_length as usize / BLOCK_SIZE;
        PieceManager {
            pieces: pieces,
            torrent: torrent,
            piece_availability_cache: None,
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
        if let Some(piece) = self.pieces.get_mut(index as usize) {
            if piece.mark_block_received(begin, data) {
                let _ = self.client_event_tx.publish(
                    consts::TOPIC_CLIENT_EVENT,
                    ClientEvent::WriteToDisk {
                        piece_index: piece.index,
                        begin: begin as usize,
                        data: data.to_vec(),
                    },
                );
                let _ = self.peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::SendToAll {
                        message: PeerMessage::Cancel((
                            piece.index as u32,
                            begin,
                            data.len() as u32,
                        )),
                    },
                );
                if matches!(piece.state, PieceState::Complete) {
                    if piece.verify() {
                        let _ = self.client_event_tx.publish(
                            consts::TOPIC_CLIENT_EVENT,
                            ClientEvent::PieceVerified {
                                piece_index: piece.index,
                            },
                        );
                        let _ = self.peer_event_tx.publish(
                            consts::TOPIC_PEER_EVENT,
                            PeerEvent::SendToAll {
                                message: PeerMessage::Have(piece.index as u32),
                            },
                        );
                    } else {
                        eprintln!("❌ Piece {} failed verification", piece.index);
                        let _ = self.client_event_tx.publish(
                            consts::TOPIC_CLIENT_EVENT,
                            ClientEvent::PieceVerificationFailure {
                                piece_index: piece.index,
                                data_size: data.len(),
                            },
                        );
                    }
                }
            }
        }
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
            .filter(|(_, piece)| {
                matches!(piece.state, PieceState::Missing | PieceState::Downloading)
            })
            .map(|(i, _)| i)
            .collect();

        candidates.sort_by_key(|&i| availability[i]);

        let (max_active_pieces, max_outstanding_per_piece) = self.calculate_throttle();
        let mut active_indicies = Vec::new();
        let mut downloading_count = self
            .pieces
            .iter()
            .filter(|piece| matches!(piece.state, PieceState::Downloading))
            .count();
        for &i in &candidates {
            if self.pieces[i].state == PieceState::Downloading {
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

            let piece = &mut self.pieces[index];
            let mut sent_this_round = 0;
            for block in piece.blocks.iter_mut() {
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

                if matches!(piece.state, PieceState::Missing) {
                    piece.state = PieceState::Downloading;
                }

                let _ = self.peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::Send {
                        addr: **peer,
                        message: PeerMessage::Request((
                            piece.index as u32,
                            block.begin,
                            block.length,
                        )),
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
