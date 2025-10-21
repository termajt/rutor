use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, RwLock, mpsc::Sender},
    time::{Duration, Instant},
};

use crate::{
    bitfield::Bitfield,
    consts::{self, ClientEvent, PeerEvent, PieceEvent},
    pool::ThreadPool,
    pubsub::PubSub,
    socketmanager::{Command, Socket},
    torrent::Torrent,
};

/// Represents a message exchanged between BitTorrent peers
/// according to the standard peer wire protocol.
///
/// Each variant corresponds to one of the standard messages a peer
/// can send. Some messages carry additional data (like piece index
/// or a block of data), while others are simple notifications.
#[derive(Debug, Clone)]
pub enum PeerMessage {
    /// Tells the receiving peer that it is **choked**.
    /// No data will be sent until an `Unchoke` message is received.
    Choke,

    /// Tells the receiving peer that it is **unchoked** and may
    /// request pieces.
    Unchoke,

    /// Indicates that the sending peer is **interested** in downloading pieces.
    Interested,

    /// Indicates that the sending peer is **not interested** in downloading pieces.
    NotInterested,

    /// Announces that the sending peer has successfully downloaded
    /// the piece at `piece_index`.
    ///
    /// # Fields
    ///
    /// * `piece_index` - The index of the piece that has been downloaded.
    Have(u32),

    /// Sends the bitfield of the pieces the sending peer has.
    ///
    /// # Fields
    ///
    /// * `bitfield` - A `Bitfield` struct representing which pieces
    ///   the peer has.
    Bitfield(Bitfield),

    /// Requests a block of data from the receiving peer.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index being requested.
    /// * `begin` - Offset within the piece.
    /// * `length` - Length of the requested block in bytes.
    Request((u32, u32, u32)),

    /// Sends a block of data in response to a `Request` message.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index of the block.
    /// * `begin` - Offset within the piece.
    /// * `block` - The actual bytes of data being sent.
    Piece((u32, u32, Vec<u8>)),

    /// Cancels a previously sent `Request`.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index of the canceled block.
    /// * `begin` - Offset within the piece.
    /// * `length` - Length of the canceled block in bytes.
    Cancel((u32, u32, u32)),

    /// Announces the DHT listening port of the sending peer.
    ///
    /// # Fields
    ///
    /// * `port` - The port number the peer is listening on for DHT messages.
    Port(u16),

    KeepAlive,
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerMessage::Bitfield(bitfield) => {
                write!(f, "PeerMessage::Bitfield({})", bitfield.len())
            }
            PeerMessage::Choke => write!(f, "PeerMessage::Choke"),
            PeerMessage::Unchoke => write!(f, "PeerMessage::Unchoke"),
            PeerMessage::Interested => write!(f, "PeerMessage::Interested"),
            PeerMessage::NotInterested => write!(f, "PeerMessage::NotInterested"),
            PeerMessage::Have(i) => write!(f, "PeerMessage::Have({})", i),
            PeerMessage::Request((i, b, l)) => {
                write!(f, "PeerMessage::Request({}, {}, {})", i, b, l)
            }
            PeerMessage::Piece((i, b, d)) => {
                write!(f, "PeerMessage::Piece({}, {}, {})", i, b, d.len())
            }
            PeerMessage::Cancel((i, b, l)) => write!(f, "PeerMessage::Cancel({}, {}, {})", i, b, l),
            PeerMessage::Port(p) => write!(f, "PeerMessage::Port({})", p),
            PeerMessage::KeepAlive => write!(f, "PeerMessage::KeepAlive"),
        }
    }
}

impl PeerMessage {
    /// Parses a peer wire protocol message from a byte buffer.
    ///
    /// Returns:
    /// - `Ok(Some(msg))` when a full message was parsed.
    /// - `Ok(None)` if more data is needed.
    /// - `Err` if the buffer contains invalid or incomplete data.
    pub fn parse(
        buf: &mut Vec<u8>,
        total_pieces: usize,
    ) -> Result<Option<PeerMessage>, Box<dyn std::error::Error>> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let msg_len = u32::from_be_bytes(buf[..4].try_into()?) as usize;

        if msg_len == 0 {
            buf.drain(..4);
            return Ok(Some(PeerMessage::KeepAlive));
        }

        if buf.len() < 4 + msg_len {
            return Err("not enough data".into());
        }

        let msg_id = buf[4];
        let payload = &buf[5..4 + msg_len];
        let msg = match msg_id {
            0 => Some(PeerMessage::Choke),
            1 => Some(PeerMessage::Unchoke),
            2 => Some(PeerMessage::Interested),
            3 => Some(PeerMessage::NotInterested),
            4 => Some(PeerMessage::Have(u32::from_be_bytes(
                payload[..4].try_into()?,
            ))),
            5 => Some(PeerMessage::Bitfield(Bitfield::from_bytes(
                payload.to_vec(),
                total_pieces,
            ))),
            6 => {
                let index = u32::from_be_bytes(payload[0..4].try_into()?);
                let begin = u32::from_be_bytes(payload[4..8].try_into()?);
                let length = u32::from_be_bytes(payload[8..12].try_into()?);
                Some(PeerMessage::Request((index, begin, length)))
            }
            7 => {
                let index = u32::from_be_bytes(payload[0..4].try_into()?);
                let begin = u32::from_be_bytes(payload[4..8].try_into()?);
                let block = payload[8..].to_vec();
                Some(PeerMessage::Piece((index, begin, block)))
            }
            8 => {
                let index = u32::from_be_bytes(payload[0..4].try_into()?);
                let begin = u32::from_be_bytes(payload[4..8].try_into()?);
                let length = u32::from_be_bytes(payload[8..12].try_into()?);
                Some(PeerMessage::Cancel((index, begin, length)))
            }
            9 => Some(PeerMessage::Port(u16::from_be_bytes(
                payload[0..2].try_into()?,
            ))),
            _ => None,
        };
        buf.drain(..4 + msg_len);

        Ok(msg)
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            PeerMessage::Choke => Self::encode_simple(0),
            PeerMessage::Unchoke => Self::encode_simple(1),
            PeerMessage::Interested => Self::encode_simple(2),
            PeerMessage::NotInterested => Self::encode_simple(3),
            PeerMessage::Have(index) => {
                let mut buf = Vec::with_capacity(9);
                buf.extend_from_slice(&5u32.to_be_bytes());
                buf.push(4);
                buf.extend_from_slice(&index.to_be_bytes());
                buf
            }
            PeerMessage::Bitfield(bitfield) => {
                let bytes = bitfield.as_bytes();
                let total_len = 1 + bytes.len();
                let mut buf = Vec::with_capacity(4 + total_len);
                buf.extend_from_slice(&(total_len as u32).to_be_bytes());
                buf.push(5);
                buf.extend_from_slice(bytes);
                buf
            }
            PeerMessage::Request((index, begin, length))
            | PeerMessage::Cancel((index, begin, length)) => {
                let id = if matches!(self, PeerMessage::Request(_)) {
                    6
                } else {
                    8
                };
                let mut buf = Vec::with_capacity(17);
                buf.extend_from_slice(&13u32.to_be_bytes());
                buf.push(id);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf
            }
            PeerMessage::Piece((index, begin, block)) => {
                let total_len = 1 + 8 + block.len();
                let mut buf = Vec::with_capacity(4 + total_len);
                buf.extend_from_slice(&(total_len as u32).to_be_bytes());
                buf.push(7);
                buf.extend_from_slice(&index.to_be_bytes());
                buf.extend_from_slice(&begin.to_be_bytes());
                buf.extend_from_slice(block);
                buf
            }
            PeerMessage::Port(port) => {
                let mut buf = Vec::with_capacity(7);
                buf.extend_from_slice(&3u32.to_be_bytes());
                buf.push(9);
                buf.extend_from_slice(&port.to_be_bytes());
                buf
            }
            PeerMessage::KeepAlive => 0u32.to_be_bytes().to_vec(),
        }
    }

    fn encode_simple(id: u8) -> Vec<u8> {
        vec![0, 0, 0, 1, id]
    }

    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            PeerMessage::Piece(_) | PeerMessage::Request(_) | PeerMessage::Cancel(_)
        )
    }
}

/// Represents the connection status of a peer in the swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    New,
    Connecting,
    Connected,
    Failed,
    Disconnected,
}

/// Contains identifying and connection metadata for a single peer.
///
/// Tracks its address, last seen time, status, and optional peer ID
/// (set after a successful handshake).
#[derive(Debug, Clone)]
pub struct PeerInfo {
    addr: SocketAddr,
    last_seen: Instant,
    status: PeerStatus,
    failures: u8,
    peer_id: Option<Vec<u8>>,
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr && self.peer_id == other.peer_id
    }
}

impl Eq for PeerInfo {}

impl PeerInfo {
    /// Creates a new peer entry with status `PeerStatus::New`.
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr: addr,
            last_seen: Instant::now(),
            status: PeerStatus::New,
            failures: 0,
            peer_id: None,
        }
    }
}

impl Hash for PeerInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
        self.peer_id.hash(state);
    }
}

#[derive(Debug)]
pub struct PeerConnection {
    addr: SocketAddr,
    peer_id: [u8; 20],
    buffer: Vec<u8>,
    pub bitfield: Bitfield,
    pub choked: bool,
    interested: bool,
    pub am_interested: bool,
}

impl PeerConnection {
    fn new(addr: SocketAddr, id: Vec<u8>, total_pieces: usize) -> Self {
        let mut peer_id = [0u8; 20];
        peer_id[0..20].copy_from_slice(&id[..20]);
        Self {
            addr: addr,
            peer_id: peer_id,
            buffer: Vec::new(),
            bitfield: Bitfield::new(total_pieces),
            choked: true,
            interested: false,
            am_interested: false,
        }
    }

    fn handle_message(&mut self, msg: &PeerMessage) {
        match msg {
            PeerMessage::Choke => {
                self.choked = true;
            }
            PeerMessage::Unchoke => {
                self.choked = false;
            }
            PeerMessage::Interested => {
                self.interested = true;
            }
            PeerMessage::NotInterested => {
                self.interested = false;
            }
            PeerMessage::Have(index) => {
                self.bitfield.set(&(*index as usize), true);
            }
            PeerMessage::Bitfield(new_bitfield) => {
                self.bitfield.merge(new_bitfield);
            }
            _ => return,
        }
    }

    fn on_recv(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    fn parse_messages(&mut self, total_pieces: usize) -> Option<PeerMessage> {
        if self.buffer.len() == 0 {
            return None;
        }
        if let Ok(v) = PeerMessage::parse(&mut self.buffer, total_pieces) {
            v
        } else {
            None
        }
    }

    fn is_choked(&self) -> bool {
        self.choked
    }
}

impl PartialEq for PeerConnection {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr && self.peer_id == other.peer_id
    }
}

impl Eq for PeerConnection {}

impl Hash for PeerConnection {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
        self.peer_id.hash(state);
    }
}

#[derive(Debug)]
struct PeerManagerState {
    peers: HashMap<SocketAddr, PeerInfo>,
    connected: HashMap<SocketAddr, PeerConnection>,
    pending: usize,
}

impl PeerManagerState {
    fn new() -> Self {
        Self {
            peers: HashMap::new(),
            connected: HashMap::new(),
            pending: 0,
        }
    }

    fn available_capacity(&self, max_connections: usize) -> usize {
        max_connections.saturating_sub(self.connected.len() + self.pending)
    }

    fn next_peer(&mut self) -> Option<SocketAddr> {
        self.peers.values_mut().find_map(|p| {
            if matches!(p.status, PeerStatus::New | PeerStatus::Failed) {
                p.status = PeerStatus::Connecting;
                Some(p.addr)
            } else {
                None
            }
        })
    }
}

/// Manages all peers in the swarm for a single torrent.
///
/// The `PeerManager` handles discovering peers, initiating connections,
/// maintaining active sockets, and dispatching messages between threads.
/// It is shared between background worker threads and the main client.
#[derive(Debug, Clone)]
pub struct PeerManager {
    tpool: Arc<ThreadPool>,
    torrent: Arc<RwLock<Torrent>>,
    peer_event_tx: Arc<PubSub<PeerEvent>>,
    piece_event_tx: Arc<PubSub<PieceEvent>>,
    client_event_tx: Arc<PubSub<ClientEvent>>,
    max_connections: Arc<usize>,
    state: Arc<RwLock<PeerManagerState>>,
}

impl PeerManager {
    /// Creates a new `PeerManager` with the given maximum connection limit,
    /// thread pool, shutdown flag, and torrent metadata.
    pub fn new(
        max_connections: usize,
        tpool: Arc<ThreadPool>,
        torrent: Arc<RwLock<Torrent>>,
        peer_event_tx: Arc<PubSub<PeerEvent>>,
        piece_event_tx: Arc<PubSub<PieceEvent>>,
        client_event_tx: Arc<PubSub<ClientEvent>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            tpool: tpool,
            torrent: torrent,
            peer_event_tx: peer_event_tx,
            piece_event_tx: piece_event_tx,
            client_event_tx: client_event_tx,
            max_connections: Arc::new(max_connections),
            state: Arc::new(RwLock::new(PeerManagerState::new())),
        })
    }

    pub fn data_received(&self, addr: &SocketAddr, data: &[u8]) {
        let mut state = self.state.write().unwrap();
        let mut piece_events = Vec::new();
        let mut peer_events = Vec::new();
        if let Some(pc) = state.connected.get_mut(addr) {
            pc.on_recv(data);
            self.parse_handle_messages(pc, &mut piece_events, &mut peer_events);
        }
        drop(state);
        for event in piece_events {
            let _ = self
                .piece_event_tx
                .publish(consts::TOPIC_PIECE_EVENT, event);
        }
        for event in peer_events {
            let _ = self.peer_event_tx.publish(consts::TOPIC_PEER_EVENT, event);
        }
    }

    fn parse_handle_messages(
        &self,
        pc: &mut PeerConnection,
        piece_events: &mut Vec<PieceEvent>,
        peer_events: &mut Vec<PeerEvent>,
    ) {
        let torrent = self.torrent.read().unwrap();
        let total_pieces = torrent.info.piece_hashes.len();
        while let Some(message) = pc.parse_messages(total_pieces) {
            pc.handle_message(&message);
            match message {
                PeerMessage::Bitfield(_) | PeerMessage::Have(_) => {
                    let am_interested_now = torrent.info.bitfield_differs(&pc.bitfield);
                    if am_interested_now != pc.am_interested {
                        pc.am_interested = am_interested_now;
                        let message = if am_interested_now {
                            PeerMessage::Interested
                        } else {
                            PeerMessage::NotInterested
                        };
                        peer_events.push(PeerEvent::Send {
                            addr: pc.addr,
                            message: message,
                        });
                    }
                    piece_events.push(PieceEvent::PieceAvailabilityChange {
                        peer: pc.addr,
                        bitfield: pc.bitfield.clone(),
                    });
                }
                PeerMessage::Choke | PeerMessage::Unchoke => {
                    piece_events.push(PieceEvent::PeerChokeChanged {
                        addr: pc.addr,
                        choked: matches!(message, PeerMessage::Choke),
                    });
                }
                PeerMessage::Piece((index, begin, data)) => {
                    piece_events.push(PieceEvent::BlockData {
                        peer: pc.addr,
                        piece_index: index,
                        begin: begin,
                        data: data,
                    });
                }
                _ => {}
            }
        }
    }

    fn add_peers(&self, new_peers: &[SocketAddr]) {
        let mut state = self.state.write().unwrap();
        for addr in new_peers {
            if state.peers.contains_key(addr) || state.connected.contains_key(addr) {
                continue;
            }
            state.peers.insert(*addr, PeerInfo::new(*addr));
        }
        drop(state);
        self.send_peers_changed();
    }

    /// Marks a peer as successfully connected and initializes a `PeerConnection`.
    fn mark_connected(&self, addr: &SocketAddr, peer_id: &[u8]) {
        let mut state = self.state.write().unwrap();
        state.pending = state.pending.saturating_sub(1);
        if let Some(p) = state.peers.get_mut(&addr) {
            p.status = PeerStatus::Connected;
            p.last_seen = Instant::now();
            p.failures = 0;
        }
        let mut changed = false;
        if !state.connected.contains_key(&addr) {
            changed = true;
            let torrent = self.torrent.read().unwrap();
            state.connected.insert(
                *addr,
                PeerConnection::new(*addr, peer_id.to_vec(), torrent.info.piece_hashes.len()),
            );
        }
        drop(state);
        if changed {
            self.send_peers_changed();
            self.maybe_send_bitfield(addr);
        }
    }

    /// Marks a peer as having failed to connect, possibly removing it if it failed repeatedly.
    fn mark_failed(&self, addr: &SocketAddr) {
        let mut state = self.state.write().unwrap();
        state.pending = state.pending.saturating_sub(1);
        let mut changed = false;
        if let Some(p) = state.peers.get_mut(&addr) {
            p.status = PeerStatus::Failed;
            p.failures += 1;
            if p.failures >= 1 {
                state.peers.remove(&addr);
                changed = true;
            }
        }
        if let Some(_) = state.connected.remove(&addr) {
            changed = true;
        }
        drop(state);
        if changed {
            self.send_peers_changed();
        }
    }

    /// Removes a peer that has been disconnected.
    fn mark_disconnected(&self, addr: &SocketAddr) {
        let mut state = self.state.write().unwrap();
        let mut changed = false;
        if let Some(_) = state.peers.remove(addr) {
            changed = true;
        }

        if let Some(_) = state.connected.remove(addr) {
            changed = true;
        }
        drop(state);
        let _ = self.piece_event_tx.publish(
            consts::TOPIC_PIECE_EVENT,
            PieceEvent::PeerDisconnected { peer: *addr },
        );
        if changed {
            self.send_peers_changed();
        }
    }

    fn send_peers_changed(&self) {
        let _ = self
            .client_event_tx
            .publish(consts::TOPIC_CLIENT_EVENT, ClientEvent::PeersChanged);
    }

    /// Returns the number of currently connected peers.
    pub fn connected_peers(&self) -> usize {
        let state = self.state.read().unwrap();
        state.connected.len()
    }

    /// Returns the total number of known peers.
    pub fn peer_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.peers.len()
    }

    /// Returns the number of pending (in-progress) connections.
    pub fn get_pending_connections(&self) -> usize {
        let state = self.state.read().unwrap();
        state.pending
    }

    pub fn get_unchoked_and_interested_peers(&self) -> Vec<SocketAddr> {
        let state = self.state.read().unwrap();
        state
            .connected
            .iter()
            .filter(|(_, pcon)| !pcon.choked && pcon.am_interested)
            .map(|(addr, _)| *addr)
            .collect()
    }

    fn attempt_connect_peers(
        &self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        socket_tx: Arc<Sender<Command>>,
    ) {
        let to_connect;
        let peers_to_try: Vec<SocketAddr>;
        {
            let mut state = self.state.write().unwrap();
            to_connect = state.available_capacity(*self.max_connections);
            peers_to_try = (0..to_connect).filter_map(|_| state.next_peer()).collect();
            state.pending += peers_to_try.len();
        }
        for addr in peers_to_try {
            let peer_event_tx = self.peer_event_tx.clone();
            let socket_tx = socket_tx.clone();
            self.tpool
                .execute(move || match connect_peer(addr, &info_hash, &peer_id) {
                    Ok((peer_id, socket)) => {
                        let _ = socket_tx.send(Command::Add((socket, addr, None)));
                        let _ = peer_event_tx.publish(
                            consts::TOPIC_PEER_EVENT,
                            PeerEvent::PeerConnected {
                                addr: addr,
                                peer_id: peer_id,
                            },
                        );
                    }
                    Err(_) => {
                        let _ = peer_event_tx.publish(
                            consts::TOPIC_PEER_EVENT,
                            PeerEvent::ConnectFailure { addr: addr },
                        );
                    }
                });
        }
    }

    fn maybe_send_bitfield(&self, addr: &SocketAddr) {
        let mut event_opt = None;
        let state = self.state.read().unwrap();
        if let Some(pc) = state.connected.get(addr) {
            let torrent = self.torrent.read().unwrap();
            if torrent.info.has_any_pieces() {
                event_opt = Some(PeerEvent::Send {
                    addr: pc.addr,
                    message: PeerMessage::Bitfield(torrent.info.bitfield()),
                });
            }
        }
        drop(state);
        if let Some(event) = event_opt {
            let _ = self.peer_event_tx.publish(consts::TOPIC_PEER_EVENT, event);
        }
    }

    fn get_message_to_socket(
        &self,
        pcon: &PeerConnection,
        message: &PeerMessage,
    ) -> Option<Command> {
        let check_choked = !matches!(message, PeerMessage::Interested);
        if check_choked && pcon.is_choked() {
            return None;
        }
        let encoded = message.encode();
        let is_critical = message.is_critical();
        Some(Command::Send(pcon.addr, encoded, is_critical))
    }

    pub fn handle_event(
        &self,
        ev: Arc<PeerEvent>,
        my_peer_id: [u8; 20],
        socket_tx: Arc<Sender<Command>>,
    ) {
        match &*ev {
            PeerEvent::SendToAll { message } => {
                let mut messages = Vec::new();
                let state = self.state.read().unwrap();
                for pcon in state.connected.values() {
                    if let Some(msg) = self.get_message_to_socket(pcon, message) {
                        messages.push(msg);
                    }
                }
                drop(state);
                for msg in messages {
                    let _ = socket_tx.send(msg);
                }
            }
            PeerEvent::Send { addr, message } => {
                let mut message_opt = None;
                let state = self.state.read().unwrap();
                if let Some(pcon) = state.connected.get(addr) {
                    message_opt = self.get_message_to_socket(pcon, message);
                }
                drop(state);
                if let Some(msg) = message_opt {
                    let _ = socket_tx.send(msg);
                }
            }
            PeerEvent::SocketDisconnect { addr } => {
                self.mark_disconnected(addr);
                let torrent = self.torrent.read().unwrap();
                let info_hash = torrent.info_hash;
                drop(torrent);
                self.attempt_connect_peers(info_hash, my_peer_id, socket_tx);
            }
            PeerEvent::ConnectFailure { addr } => {
                self.mark_failed(addr);
                let torrent = self.torrent.read().unwrap();
                let info_hash = torrent.info_hash;
                drop(torrent);
                self.attempt_connect_peers(info_hash, my_peer_id, socket_tx);
            }
            PeerEvent::NewPeers { peers } => {
                self.add_peers(peers);
                let torrent = self.torrent.read().unwrap();
                let info_hash = torrent.info_hash;
                drop(torrent);
                self.attempt_connect_peers(info_hash, my_peer_id, socket_tx);
            }
            PeerEvent::PeerConnected { addr, peer_id } => {
                self.mark_connected(addr, peer_id);
                let torrent = self.torrent.read().unwrap();
                let info_hash = torrent.info_hash;
                drop(torrent);
                self.attempt_connect_peers(info_hash, my_peer_id, socket_tx);
            }
        }
    }
}

fn connect_peer(
    addr: SocketAddr,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> Result<(Vec<u8>, Socket), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;

    let mut handshake = Vec::with_capacity(68);
    handshake.push(19);
    handshake.extend_from_slice(b"BitTorrent protocol");
    handshake.extend_from_slice(&[0u8; 8]);
    handshake.extend_from_slice(info_hash);
    handshake.extend_from_slice(peer_id);

    stream.write_all(&mut handshake)?;

    let mut buf = [0u8; 68];
    stream.read_exact(&mut buf)?;

    if buf[0] != 19 || &buf[1..20] != b"BitTorrent protocol" {
        return Err("invalid handshake".into());
    }
    if &buf[28..48] != info_hash {
        return Err("info_hash mismatch".into());
    }

    let remote_peer_id = &buf[48..68];
    Ok((remote_peer_id.to_vec(), Socket::Client((stream, addr))))
}
