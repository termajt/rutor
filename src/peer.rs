use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use crate::{
    bitfield::Bitfield,
    bytespeed::ByteSpeed,
    consts::{self, ClientEvent, PeerEvent, PieceEvent},
    pool::ThreadPool,
    pubsub::PubSub,
    queue::Sender,
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
    am_interested: bool,
    speed: ByteSpeed,
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
            speed: ByteSpeed::new(Duration::from_secs(4), Duration::from_millis(500)),
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
            PeerMessage::Piece((_, _, data)) => {
                self.speed.update(data.len());
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

    pub fn get_download_speed(&self) -> f64 {
        self.speed.avg.max(1024.0)
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

/// Manages all peers in the swarm for a single torrent.
///
/// The `PeerManager` handles discovering peers, initiating connections,
/// maintaining active sockets, and dispatching messages between threads.
/// It is shared between background worker threads and the main client.
#[derive(Debug, Clone)]
pub struct PeerManager {
    tpool: Arc<ThreadPool>,
    torrent: Arc<Torrent>,
    peer_event_tx: Arc<PubSub<PeerEvent>>,
    piece_event_tx: Arc<PubSub<PieceEvent>>,
    client_event_tx: Arc<PubSub<ClientEvent>>,
    socket_tx: Arc<Sender<Command>>,
    peers: Arc<RwLock<HashMap<SocketAddr, PeerInfo>>>,
    pub connected: Arc<RwLock<HashMap<SocketAddr, PeerConnection>>>,
    max_connections: Arc<usize>,
    pending_connections: Arc<Mutex<usize>>,
}

impl PeerManager {
    /// Creates a new `PeerManager` with the given maximum connection limit,
    /// thread pool, shutdown flag, and torrent metadata.
    pub fn new(
        max_connections: usize,
        tpool: Arc<ThreadPool>,
        torrent: Arc<Torrent>,
        peer_event_tx: Arc<PubSub<PeerEvent>>,
        piece_event_tx: Arc<PubSub<PieceEvent>>,
        client_event_tx: Arc<PubSub<ClientEvent>>,
        socket_tx: Arc<Sender<Command>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            tpool: tpool,
            torrent: torrent,
            peer_event_tx: peer_event_tx,
            piece_event_tx: piece_event_tx,
            client_event_tx: client_event_tx,
            socket_tx: socket_tx,
            peers: Arc::new(RwLock::new(HashMap::new())),
            connected: Arc::new(RwLock::new(HashMap::new())),
            max_connections: Arc::new(max_connections),
            pending_connections: Arc::new(Mutex::new(0)),
        })
    }

    pub fn get_download_speeds(&self) -> HashMap<SocketAddr, f64> {
        let mut map = HashMap::new();
        let connected = self.connected.read().unwrap();
        for (addr, pconn) in connected.iter() {
            map.insert(*addr, pconn.get_download_speed());
        }
        map
    }

    pub fn get_download_speed(&self, addr: &SocketAddr) -> Option<f64> {
        let connected = self.connected.read().unwrap();
        if let Some(pconn) = connected.get(addr) {
            Some(pconn.get_download_speed())
        } else {
            None
        }
    }

    fn data_received(&self, addr: &SocketAddr, data: &[u8]) {
        let mut connected = self.connected.write().unwrap();
        if let Some(pc) = connected.get_mut(addr) {
            pc.on_recv(data);
        }
        drop(connected);
        self.parse_handle_messages();
    }

    fn parse_handle_messages(&self) {
        let total_pieces = self.torrent.info.piece_hashes.len();
        let mut connected = self.connected.write().unwrap();
        for pc in connected.values_mut() {
            while let Some(message) = pc.parse_messages(total_pieces) {
                pc.handle_message(&message);
                match message {
                    PeerMessage::Bitfield(_) | PeerMessage::Have(_) => {
                        self.update_interest(pc);
                        let _ = self.piece_event_tx.publish(
                            consts::TOPIC_PIECE_EVENT,
                            PieceEvent::PieceAvailabilityChange {
                                peer: pc.addr,
                                bitfield: pc.bitfield.clone(),
                            },
                        );
                    }
                    PeerMessage::Choke | PeerMessage::Unchoke => {
                        let _ = self.piece_event_tx.publish(
                            consts::TOPIC_PIECE_EVENT,
                            PieceEvent::PeerChokeChanged {
                                addr: pc.addr,
                                choked: matches!(message, PeerMessage::Choke),
                            },
                        );
                    }
                    PeerMessage::Piece((index, begin, data)) => {
                        let _ = self.piece_event_tx.publish(
                            consts::TOPIC_PIECE_EVENT,
                            PieceEvent::BlockData {
                                peer: pc.addr,
                                piece_index: index,
                                begin: begin,
                                data: data,
                            },
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    fn update_interest(&self, pconn: &mut PeerConnection) {
        let am_interested_now = self.torrent.info.bitfield_differs(&pconn.bitfield);
        if am_interested_now != pconn.am_interested {
            pconn.am_interested = am_interested_now;

            let message = if am_interested_now {
                PeerMessage::Interested
            } else {
                PeerMessage::NotInterested
            };

            let _ = self.peer_event_tx.publish(
                consts::TOPIC_PEER_EVENT,
                PeerEvent::Send {
                    addr: pconn.addr,
                    message: message,
                },
            );
        }
    }

    fn add_peers(&self, new_peers: &[SocketAddr]) {
        let mut peers = self.peers.write().unwrap();
        let connected = self.connected.read().unwrap();
        for addr in new_peers {
            if peers.contains_key(addr) || connected.contains_key(addr) {
                continue;
            }
            peers.insert(*addr, PeerInfo::new(*addr));
        }
        drop(peers);
        self.send_peers_changed();
    }

    /// Selects the next available peer to attempt a connection.
    fn next_peer_to_connect(&self) -> Option<PeerInfo> {
        let connected = self.connected.read().unwrap();
        if connected.len() >= *self.max_connections {
            return None;
        }
        drop(connected);
        let mut peers = self.peers.write().unwrap();
        peers
            .values_mut()
            .find(|p| match p.status {
                PeerStatus::New => true,
                PeerStatus::Failed => true,
                _ => false,
            })
            .map(|p| {
                p.status = PeerStatus::Connecting;
                p.clone()
            })
    }

    /// Marks a peer as successfully connected and initializes a `PeerConnection`.
    fn mark_connected(&self, addr: &SocketAddr, peer_id: &[u8], total_pieces: usize) {
        let mut pending_connections = self.pending_connections.lock().unwrap();
        *pending_connections = pending_connections.saturating_sub(1);
        drop(pending_connections);
        let mut peers = self.peers.write().unwrap();
        if let Some(p) = peers.get_mut(&addr) {
            p.status = PeerStatus::Connected;
            p.last_seen = Instant::now();
            p.failures = 0;
        }
        drop(peers);
        let mut connected = self.connected.write().unwrap();
        if !connected.contains_key(&addr) {
            connected.insert(
                *addr,
                PeerConnection::new(*addr, peer_id.to_vec(), total_pieces),
            );
        }
        drop(connected);
        self.send_peers_changed();
        self.maybe_send_bitfield(addr);
    }

    /// Marks a peer as having failed to connect, possibly removing it if it failed repeatedly.
    fn mark_failed(&self, addr: &SocketAddr) {
        let mut pending_connections = self.pending_connections.lock().unwrap();
        *pending_connections = pending_connections.saturating_sub(1);
        drop(pending_connections);
        let mut peers = self.peers.write().unwrap();
        if let Some(p) = peers.get_mut(&addr) {
            p.status = PeerStatus::Failed;
            p.failures += 1;
            if p.failures >= 1 {
                peers.remove(&addr);
            }
        }
        drop(peers);

        let mut connected = self.connected.write().unwrap();
        connected.remove(&addr);
        drop(connected);
        self.send_peers_changed();
    }

    /// Removes a peer that has been disconnected.
    fn mark_disconnected(&self, addr: &SocketAddr) {
        let mut peers = self.peers.write().unwrap();
        peers.remove(addr);
        drop(peers);

        let mut connected = self.connected.write().unwrap();
        connected.remove(addr);
        drop(connected);
        let _ = self.piece_event_tx.publish(
            consts::TOPIC_PIECE_EVENT,
            PieceEvent::PeerDisconnected { peer: *addr },
        );
        self.send_peers_changed();
    }

    fn send_peers_changed(&self) {
        let _ = self
            .client_event_tx
            .publish(consts::TOPIC_CLIENT_EVENT, ClientEvent::PeersChanged);
    }

    /// Returns the number of currently connected peers.
    pub fn connected_peers(&self) -> usize {
        let connected = self.connected.read().unwrap();
        connected.len()
    }

    /// Returns the total number of known peers.
    pub fn peer_count(&self) -> usize {
        let peers = self.peers.read().unwrap();
        peers.len()
    }

    /// Returns the number of pending (in-progress) connections.
    pub fn get_pending_connections(&self) -> usize {
        let pending_connections = self.pending_connections.lock().unwrap();
        *pending_connections
    }

    fn attempt_connect_peers(&self, info_hash: [u8; 20], peer_id: [u8; 20]) {
        let to_connect: usize;
        {
            let connected = self.connected.read().unwrap();
            let pending_connections = self.pending_connections.lock().unwrap();
            let max_connections = *self.max_connections;
            if connected.len() >= max_connections || *pending_connections >= max_connections {
                return;
            }
            to_connect = max_connections.saturating_sub(connected.len() + *pending_connections);
        }
        if to_connect == 0 {
            return;
        }
        let mut c = Vec::new();
        for _ in 0..to_connect {
            if let Some(p) = self.next_peer_to_connect() {
                c.push(p);
            } else {
                break;
            }
        }
        if c.is_empty() {
            return;
        }
        for p in c {
            let connected = self.connected.clone();
            let socket_tx = self.socket_tx.clone();
            let peer_event_tx = self.peer_event_tx.clone();
            self.tpool.execute(move || {
                let connected = connected.read().unwrap();
                if connected.contains_key(&p.addr) {
                    return;
                }
                drop(connected);
                match connect_peer(p.addr, &info_hash, &peer_id) {
                    Ok((peer_id, socket)) => {
                        let _ = socket_tx.send(Command::Add((socket, p.addr, None)));
                        let _ = peer_event_tx.publish(
                            consts::TOPIC_PEER_EVENT,
                            PeerEvent::PeerConnected {
                                addr: p.addr,
                                peer_id: peer_id,
                            },
                        );
                    }
                    Err(_) => {
                        let _ = peer_event_tx.publish(
                            consts::TOPIC_PEER_EVENT,
                            PeerEvent::ConnectFailure { addr: p.addr },
                        );
                    }
                }
            });
        }
    }

    pub fn peers_with_piece(&self, index: usize) -> Vec<SocketAddr> {
        let connected = self.connected.read().unwrap();
        connected
            .iter()
            .filter_map(|(addr, pc)| {
                if pc.bitfield.get(&index) {
                    Some(*addr)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn is_choked(&self, addr: &SocketAddr) -> bool {
        let connected = self.connected.read().unwrap();
        if let Some(pc) = connected.get(addr) {
            return pc.is_choked();
        }
        return true;
    }

    fn maybe_send_bitfield(&self, addr: &SocketAddr) {
        let connected = self.connected.read().unwrap();
        if let Some(pc) = connected.get(addr) {
            if self.torrent.info.has_any_pieces() {
                let _ = self.peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::Send {
                        addr: pc.addr,
                        message: PeerMessage::Bitfield(self.torrent.info.bitfield()),
                    },
                );
            }
        }
    }

    fn send_message_to_socket(&self, pcon: &PeerConnection, message: &PeerMessage) {
        let check_choked = !matches!(message, PeerMessage::Interested);
        if check_choked && pcon.is_choked() {
            return;
        }
        let encoded = message.encode();
        let is_critical = message.is_critical();
        let _ = self
            .socket_tx
            .send(Command::Send(pcon.addr, encoded, is_critical));
    }

    pub fn handle_event(&self, ev: Arc<PeerEvent>, peer_id: [u8; 20]) {
        match &*ev {
            PeerEvent::SendToAll { message } => {
                let connected = self.connected.read().unwrap();
                for pcon in connected.values() {
                    self.send_message_to_socket(pcon, message);
                }
            }
            PeerEvent::Send { addr, message } => {
                let connected = self.connected.read().unwrap();
                if let Some(pcon) = connected.get(addr) {
                    self.send_message_to_socket(pcon, message);
                }
            }
            PeerEvent::SocketData { addr, data } => {
                self.data_received(addr, &data);
            }
            PeerEvent::SocketDisconnect { addr } => {
                self.mark_disconnected(addr);
                let info_hash = self.torrent.info_hash;
                self.attempt_connect_peers(info_hash, peer_id);
            }
            PeerEvent::ConnectFailure { addr } => {
                self.mark_failed(addr);
                let info_hash = self.torrent.info_hash;
                self.attempt_connect_peers(info_hash, peer_id);
            }
            PeerEvent::NewPeers { peers } => {
                self.add_peers(peers);
                let info_hash = self.torrent.info_hash;
                self.attempt_connect_peers(info_hash, peer_id);
            }
            PeerEvent::PeerConnected { addr, peer_id } => {
                let total_pieces = self.torrent.info.piece_hashes.len();
                self.mark_connected(addr, peer_id, total_pieces);
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
