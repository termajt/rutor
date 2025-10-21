use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
    usize,
};

use crate::{
    consts::{self, ClientEvent, PeerEvent, PieceEvent},
    peer::{PeerManager, PeerMessage},
    picker::{PickerConfig, PiecePicker},
    pubsub::PubSub,
    torrent::Torrent,
};

#[derive(Debug)]
pub struct PieceManager {
    piece_availability_cache: Option<Vec<usize>>,
    peer_manager: Arc<PeerManager>,
    peer_event_tx: Arc<PubSub<PeerEvent>>,
    client_event_tx: Arc<PubSub<ClientEvent>>,
    blocks_per_piece: usize,
    picker: Mutex<PiecePicker>,
}

impl PieceManager {
    /// Creates a new `PieceManager` from the given torrent metadata.
    pub fn new(
        torrent: Arc<RwLock<Torrent>>,
        peer_manager: Arc<PeerManager>,
        peer_event_tx: Arc<PubSub<PeerEvent>>,
        client_event_tx: Arc<PubSub<ClientEvent>>,
    ) -> Self {
        let guard = torrent.read().unwrap();
        let total_size = guard.info.total_size;
        let piece_length = guard.info.piece_length;
        let picker_config = PickerConfig::default();
        let blocks_per_piece = piece_length as usize / picker_config.block_size;
        let picker = Mutex::new(PiecePicker::new(
            total_size,
            piece_length as usize,
            picker_config,
            torrent.clone(),
        ));
        PieceManager {
            piece_availability_cache: None,
            peer_manager: peer_manager,
            peer_event_tx: peer_event_tx,
            client_event_tx: client_event_tx,
            blocks_per_piece: blocks_per_piece,
            picker: picker,
        }
    }

    fn calculate_total_max_outstanding_requests(&self) -> usize {
        let connected_peers = self.peer_manager.connected_peers();
        let max_per_peer = std::cmp::min(self.blocks_per_piece, connected_peers / 2);
        let total = connected_peers * max_per_peer;
        std::cmp::max(1, total)
    }

    pub fn run_piece_selection_once(&self) {
        let max_outstanding_requests = self.calculate_total_max_outstanding_requests();
        let peers = self.peer_manager.get_unchoked_and_interested_peers();
        let mut picker = self.picker.lock().unwrap();
        picker.cleanup_timed_out_requests();
        let reqs = picker.pick_requests_for_peers(&peers, max_outstanding_requests);
        drop(picker);
        for (peer, reqs) in reqs {
            self.send_requests(&peer, &reqs);
        }
    }

    pub fn handle_event(&mut self, ev: Arc<PieceEvent>) {
        match &*ev {
            PieceEvent::BlockData {
                peer,
                piece_index,
                begin,
                data,
            } => {
                let mut picker = self.picker.lock().unwrap();
                let (cancel_peers, received) = picker.handle_block_received(
                    peer,
                    *piece_index as usize,
                    *begin as usize,
                    data,
                );
                let verified_result = picker.verify_piece(*piece_index as usize);
                drop(picker);
                self.send_cancels(&cancel_peers, piece_index, begin, data.len());
                if received {
                    let _ = self.client_event_tx.publish(
                        consts::TOPIC_CLIENT_EVENT,
                        ClientEvent::WriteToDisk {
                            piece_index: *piece_index as usize,
                            begin: *begin as usize,
                            data: data.to_vec(),
                        },
                    );
                }
                if let Ok(verified) = verified_result {
                    if verified {
                        let _ = self.peer_event_tx.publish(
                            consts::TOPIC_PEER_EVENT,
                            PeerEvent::SendToAll {
                                message: PeerMessage::Have(*piece_index),
                            },
                        );
                        let _ = self.client_event_tx.publish(
                            consts::TOPIC_CLIENT_EVENT,
                            ClientEvent::PieceVerified {
                                piece_index: *piece_index as usize,
                            },
                        );
                    } else {
                        eprintln!("❌ Piece {} failed verification", piece_index);
                        let _ = self.client_event_tx.publish(
                            consts::TOPIC_CLIENT_EVENT,
                            ClientEvent::PieceVerificationFailure {
                                piece_index: *piece_index as usize,
                                data_size: data.len(),
                            },
                        );
                    }
                }
            }
            PieceEvent::PieceAvailabilityChange { peer, bitfield } => {
                self.piece_availability_cache = None;
                let mut picker = self.picker.lock().unwrap();
                picker.update_peer_bitfield(peer, bitfield);
            }
            PieceEvent::PeerChokeChanged { addr: _, choked: _ } => {
                // Do nothing
            }
            PieceEvent::PeerDisconnected { peer } => {
                let mut picker = self.picker.lock().unwrap();
                picker.peer_disconnected(peer);
            }
        }
    }

    fn send_requests(&self, peer: &SocketAddr, reqs: &[(u32, u32, u32)]) {
        for req in reqs {
            let _ = self.peer_event_tx.publish(
                consts::TOPIC_PEER_EVENT,
                PeerEvent::Send {
                    addr: *peer,
                    message: PeerMessage::Request(*req),
                },
            );
        }
    }

    fn send_cancels(&self, peers: &[SocketAddr], piece_index: &u32, offset: &u32, length: usize) {
        for peer in peers {
            let _ = self.peer_event_tx.publish(
                consts::TOPIC_PEER_EVENT,
                PeerEvent::Send {
                    addr: *peer,
                    message: PeerMessage::Cancel((
                        *piece_index as u32,
                        *offset as u32,
                        length as u32,
                    )),
                },
            );
        }
    }
}
