use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    os::fd::RawFd,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    bitfield::Bitfield,
    pool::ThreadPool,
    queue::{Queue, Receiver, Sender},
    socketmanager::{Command, Socket, SocketManager},
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
enum PeerState {
    Choked,
    Unchoked,
    Interested,
    NotInterested,
}

#[derive(Debug)]
struct PeerConnection {
    addr: SocketAddr,
    peer_id: [u8; 20],
    buffer: Mutex<Vec<u8>>,
    bitfield: Mutex<Bitfield>,
    state: Mutex<PeerState>,
}

impl PeerConnection {
    fn new(addr: SocketAddr, id: Vec<u8>, total_pieces: usize) -> Self {
        let mut peer_id = [0u8; 20];
        peer_id[0..20].copy_from_slice(&id[..20]);
        Self {
            addr: addr,
            peer_id: peer_id,
            buffer: Mutex::new(Vec::new()),
            bitfield: Mutex::new(Bitfield::new(total_pieces)),
            state: Mutex::new(PeerState::Choked),
        }
    }

    fn handle_message(&self, msg: &PeerMessage) {
        match msg {
            PeerMessage::Choke => {
                let mut state = self.state.lock().unwrap();
                *state = PeerState::Choked;
            }
            PeerMessage::Unchoke => {
                let mut state = self.state.lock().unwrap();
                *state = PeerState::Unchoked;
            }
            PeerMessage::Interested => {
                let mut state = self.state.lock().unwrap();
                *state = PeerState::Interested;
            }
            PeerMessage::NotInterested => {
                let mut state = self.state.lock().unwrap();
                *state = PeerState::NotInterested;
            }
            PeerMessage::Have(index) => {
                let mut bitfield = self.bitfield.lock().unwrap();
                bitfield.set(*index as usize, true);
            }
            PeerMessage::Bitfield(new_bitfield) => {
                let mut bitfield = self.bitfield.lock().unwrap();
                bitfield.merge(new_bitfield);
            }
            PeerMessage::Request((_index, _begin, _length)) => todo!(),
            PeerMessage::Piece((_index, _begin, _block)) => todo!(),
            PeerMessage::Cancel((_index, _begin, _length)) => todo!(),
            PeerMessage::Port(_port) => return,
            PeerMessage::KeepAlive => return,
        }
    }

    fn on_recv(&self, data: &[u8]) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.extend_from_slice(data);
    }

    fn parse_messages(&self, msg_sender: &Sender<(SocketAddr, PeerMessage)>, total_pieces: usize) {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.len() == 0 {
            return;
        }
        loop {
            if let Ok(msg) = PeerMessage::parse(&mut buffer, total_pieces) {
                match msg {
                    Some(msg) => {
                        if let Err(e) = msg_sender.send((self.addr, msg)) {
                            eprintln!("failed to send peer msg: {e}");
                        }
                    }
                    None => break,
                }
            } else {
                break;
            }
        }
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
    inner: Arc<Mutex<Inner>>,
    tpool: Arc<ThreadPool>,
    socket_manager: Arc<Mutex<SocketManager>>,
    shutdown: Arc<AtomicBool>,
    socket_tx: Sender<(Vec<u8>, SocketAddr, RawFd)>,
    pub socket_rx: Receiver<(Vec<u8>, SocketAddr, RawFd)>,
    socket_command_tx: Arc<Sender<Command>>,
    pub socket_command_rx: Receiver<Command>,
    connect_tx: Arc<Sender<(SocketAddr, PeerStatus)>>,
    pub connect_rx: Receiver<(SocketAddr, PeerStatus)>,
    peer_msg_tx: Sender<(SocketAddr, PeerMessage)>,
    pub peer_msg_rx: Receiver<(SocketAddr, PeerMessage)>,
    torrent: Arc<Torrent>,
}

#[derive(Debug)]
struct Inner {
    peers: HashMap<SocketAddr, PeerInfo>,
    connected: HashMap<SocketAddr, PeerConnection>,
    max_connections: usize,
    pending_connections: usize,
}

impl PeerManager {
    /// Creates a new `PeerManager` with the given maximum connection limit,
    /// thread pool, shutdown flag, and torrent metadata.
    pub fn new(
        max_connections: usize,
        tpool: Arc<ThreadPool>,
        shutdown: Arc<AtomicBool>,
        torrent: Arc<Torrent>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (socket_tx, socket_rx) = Queue::new(None);
        let (command_tx, command_rx) = Queue::new(None);
        let (connect_tx, connect_rx) = Queue::new(None);
        let (peer_msg_tx, peer_msg_rx) = Queue::new(None);
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                peers: HashMap::new(),
                connected: HashMap::new(),
                max_connections: max_connections,
                pending_connections: 0,
            })),
            tpool: tpool,
            socket_manager: Arc::new(Mutex::new(SocketManager::new()?)),
            shutdown: shutdown,
            socket_tx: socket_tx,
            socket_rx: socket_rx,
            socket_command_tx: Arc::new(command_tx),
            socket_command_rx: command_rx,
            connect_tx: Arc::new(connect_tx),
            connect_rx: connect_rx,
            peer_msg_tx: peer_msg_tx,
            peer_msg_rx: peer_msg_rx,
            torrent: torrent,
        })
    }

    /// Processes socket events and removes disconnected peers.
    pub fn handle_connections(&self) -> Result<(), Box<dyn std::error::Error>> {
        let disconnects = {
            let mut socket_manager = self.socket_manager.lock().unwrap();
            socket_manager.run_once(&self.socket_command_rx, &self.socket_tx)?
        };
        for socket in disconnects {
            match socket {
                Socket::Client((_, addr)) => {
                    self.mark_disconnected(addr);
                }
                _ => continue,
            }
        }
        Ok(())
    }

    /// Handles incoming network data from connected peers.
    pub fn run_handle_data(&self) {
        while !self.shutdown.load(Ordering::Relaxed)
            && let Ok((data, addr, _fd)) = self.socket_rx.try_recv()
        {
            let inner = self.inner.lock().unwrap();
            if let Some(peer_conn) = inner.connected.get(&addr) {
                peer_conn.on_recv(&data);
            }
        }
    }

    /// Parses messages from peer buffers and enqueues them into the message channel.
    pub fn run_parse_messages(&self) {
        let inner = self.inner.lock().unwrap();
        for p in inner.connected.values() {
            p.parse_messages(&self.peer_msg_tx, self.torrent.info.piece_hashes.len());
        }
    }

    /// Handles a parsed message from a peer by updating connection state.
    pub fn handle_message(&self, addr: SocketAddr, message: PeerMessage) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(p) = inner.connected.get_mut(&addr) {
            p.handle_message(&message);
        }
    }

    /// Adds newly discovered peers to the pool of known peers.
    pub fn add_peers(&self, new_peers: &[SocketAddr]) {
        let mut inner = self.inner.lock().unwrap();
        for addr in new_peers {
            inner
                .peers
                .entry(*addr)
                .or_insert_with(|| PeerInfo::new(*addr));
        }
    }

    /// Selects the next available peer to attempt a connection.
    pub fn next_peer_to_connect(&self) -> Option<PeerInfo> {
        let mut inner = self.inner.lock().unwrap();
        if inner.connected.len() >= inner.max_connections {
            return None;
        }
        inner
            .peers
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
    pub fn mark_connected(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        let peer_id_opt = {
            if let Some(p) = inner.peers.get_mut(&addr) {
                p.status = PeerStatus::Connected;
                p.last_seen = Instant::now();
                p.peer_id.clone()
            } else {
                return;
            }
        };
        if let Some(peer_id) = peer_id_opt {
            inner.connected.insert(
                addr,
                PeerConnection::new(addr, peer_id, self.torrent.info.piece_hashes.len()),
            );
        }
    }

    /// Marks a peer as having failed to connect, possibly removing it if it failed repeatedly.
    pub fn mark_failed(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(p) = inner.peers.get_mut(&addr) {
            p.status = PeerStatus::Failed;
            if p.failures >= 3 {
                inner.peers.remove(&addr);
            }
        }
        inner.connected.remove(&addr);
    }

    /// Removes a peer that has been disconnected.
    pub fn mark_disconnected(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.peers.remove(&addr);
        inner.connected.remove(&addr);
    }

    /// Returns the number of currently connected peers.
    pub fn connected_peers(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.connected.len()
    }

    /// Returns the total number of known peers.
    pub fn peer_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.peers.len()
    }

    /// Returns the number of pending (in-progress) connections.
    pub fn get_pending_connections(&self) -> usize {
        let state = self.inner.lock().unwrap();
        state.pending_connections
    }

    /// Attempts to connect to peers concurrently, performing the BitTorrent handshake.
    pub fn connect_peers(&self, info_hash: [u8; 20], peer_id: [u8; 20]) {
        while !self.shutdown.load(Ordering::Relaxed)
            && let Some(p) = self.next_peer_to_connect()
        {
            {
                let mut inner = self.inner.lock().unwrap();
                if inner.connected.len() >= inner.max_connections
                    || inner.pending_connections >= inner.max_connections
                {
                    break;
                }
                inner.pending_connections += 1;
            }
            let socket_command_sender = self.socket_command_tx.clone();
            let addr = p.addr;
            let inner = self.inner.clone();
            let tx = self.connect_tx.clone();
            self.tpool
                .execute(move || match connect_peer(addr, &info_hash, &peer_id) {
                    Ok((peer_id, socket)) => {
                        {
                            let mut inner = inner.lock().unwrap();
                            inner.pending_connections -= 1;
                            if let Some(pinfo) = inner.peers.get_mut(&addr) {
                                pinfo.peer_id = Some(peer_id);
                                pinfo.status = PeerStatus::Connected;
                            }
                        }
                        if let Err(e) = socket_command_sender.send(Command::Add(socket)) {
                            eprintln!("failed to send add socket command: {e}");
                        }
                        if let Err(e) = tx.send((addr, PeerStatus::Connected)) {
                            eprintln!("failed to send socket connection result: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("connection failed to {}: {e}", addr);
                        {
                            let mut inner = inner.lock().unwrap();
                            inner.pending_connections -= 1;
                            if let Some(pinfo) = inner.peers.get_mut(&addr) {
                                pinfo.status = PeerStatus::Failed;
                                pinfo.failures += 1;
                                if pinfo.failures >= 3 {
                                    inner.peers.remove(&addr);
                                }
                            }
                        }
                        if let Err(e) = tx.send((addr, PeerStatus::Failed)) {
                            eprintln!("failed to send socket connection result: {e}");
                        }
                    }
                });
        }
    }

    /// Gracefully stops the peer manager by closing communication channels.
    pub fn stop(&self) {
        self.connect_tx.close();
        self.socket_tx.close();
        self.socket_command_tx.close();
    }
}

fn connect_peer(
    addr: SocketAddr,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> Result<(Vec<u8>, Socket), Box<dyn std::error::Error>> {
    println!("connecting to: {addr}...");
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
    println!("connection and handshake successful for: {addr}!");
    Ok((remote_peer_id.to_vec(), Socket::Client((stream, addr))))
}
