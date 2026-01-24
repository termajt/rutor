use std::net::SocketAddr;

use crate::{bitfield::Bitfield, peer::PeerMessage, picker::PieceVerification};

pub const TOPIC_CLIENT_EVENT: &str = &"client_event";
pub const TOPIC_PIECE_EVENT: &str = &"piece_event";
pub const TOPIC_PEER_EVENT: &str = &"peer_event";

#[derive(Debug)]
pub enum PieceEvent {
    BlockData {
        peer: SocketAddr,
        piece_index: u32,
        begin: u32,
        data: Vec<u8>,
    },
    PieceAvailabilityChange {
        peer: SocketAddr,
        bitfield: Bitfield,
    },
    PeerChokeChanged {
        addr: SocketAddr,
        choked: bool,
    },
    PeerDisconnected {
        peer: SocketAddr,
    },
    Shutdown,
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
    Shutdown,
}

#[derive(Debug)]
pub enum ClientEvent {
    PieceVerificationFailure {
        piece_index: usize,
        data_size: usize,
    },
    PeersChanged,
    PieceVerified(PieceVerification),
    WriteToDisk {
        piece_index: usize,
        begin: usize,
        data: Vec<u8>,
    },
    Shutdown,
}

#[derive(Debug)]
pub enum SocketDataEvent {
    Data((SocketAddr, Vec<u8>)),
    Shutdown,
}
