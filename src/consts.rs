use std::net::SocketAddr;

use crate::peer::PeerMessage;

pub const TOPIC_CLIENT_EVENT: &str = &"client_event";
pub const TOPIC_PIECE_EVENT: &str = &"piece_event";
pub const TOPIC_PEER_EVENT: &str = &"peer_event";

#[derive(Debug)]
pub enum PieceEvent {
    BlockData {
        piece_index: u32,
        begin: u32,
        data: Vec<u8>,
    },
    PieceAvailabilityChange,
    PeerChokeChanged {
        addr: SocketAddr,
        choked: bool,
    },
}

#[derive(Debug)]
pub enum PeerEvent {
    SendToAll {
        message: PeerMessage,
    },
    Send {
        addr: SocketAddr,
        message: PeerMessage,
    },
    SocketData {
        addr: SocketAddr,
        data: Vec<u8>,
    },
    SocketDisconnect {
        addr: SocketAddr,
    },
    ConnectFailure {
        addr: SocketAddr,
    },
    NewPeers {
        peers: Vec<SocketAddr>,
    },
    PeerConnected {
        addr: SocketAddr,
        peer_id: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum ClientEvent {
    BytesDownloaded {
        data_size: usize,
    },
    PieceVerificationFailure {
        piece_index: usize,
        data_size: usize,
    },
    PeersChanged,
    PieceVerified {
        piece_index: usize,
        data: Vec<u8>,
    },
}
