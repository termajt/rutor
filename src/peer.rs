use std::{
    collections::{HashMap, HashSet},
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
    Have { piece_index: u32 },

    /// Sends the bitfield of the pieces the sending peer has.
    ///
    /// # Fields
    ///
    /// * `bitfield` - A `Bitfield` struct representing which pieces
    ///   the peer has.
    Bitfield { bitfield: Bitfield },

    /// Requests a block of data from the receiving peer.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index being requested.
    /// * `begin` - Offset within the piece.
    /// * `length` - Length of the requested block in bytes.
    Request { index: u32, begin: u32, length: u32 },

    /// Sends a block of data in response to a `Request` message.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index of the block.
    /// * `begin` - Offset within the piece.
    /// * `block` - The actual bytes of data being sent.
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },

    /// Cancels a previously sent `Request`.
    ///
    /// # Fields
    ///
    /// * `index` - Piece index of the canceled block.
    /// * `begin` - Offset within the piece.
    /// * `length` - Length of the canceled block in bytes.
    Cancel { index: u32, begin: u32, length: u32 },

    /// Announces the DHT listening port of the sending peer.
    ///
    /// # Fields
    ///
    /// * `port` - The port number the peer is listening on for DHT messages.
    Port { port: u16 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    New,
    Connecting,
    Connected,
    Failed,
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    pub last_seen: Instant,
    pub status: PeerStatus,
    pub failures: u8,
    pub peer_id: Option<Vec<u8>>,
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr && self.peer_id == other.peer_id
    }
}

impl Eq for PeerInfo {}

impl PeerInfo {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr: addr,
            last_seen: Instant::now(),
            status: PeerStatus::New,
            failures: 0,
            peer_id: None,
        }
    }

    pub fn with_peer_id(addr: SocketAddr, peer_id: Vec<u8>) -> Self {
        Self {
            addr: addr,
            last_seen: Instant::now(),
            status: PeerStatus::Connected,
            failures: 0,
            peer_id: Some(peer_id),
        }
    }
}

impl Hash for PeerInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
        self.peer_id.hash(state);
    }
}

#[derive(Debug, Clone)]
pub struct PeerManager {
    inner: Arc<Mutex<Inner>>,
    tpool: Arc<ThreadPool>,
    socket_manager: Arc<Mutex<SocketManager>>,
    shutdown: Arc<AtomicBool>,
    socket_tx: Sender<(Vec<u8>, SocketAddr, RawFd)>,
    socket_rx: Receiver<(Vec<u8>, SocketAddr, RawFd)>,
    socket_command_tx: Arc<Sender<Command>>,
    socket_command_rx: Receiver<Command>,
    connect_tx: Arc<Sender<(SocketAddr, PeerStatus)>>,
    connect_rx: Receiver<(SocketAddr, PeerStatus)>,
}

#[derive(Debug)]
struct Inner {
    peers: HashMap<SocketAddr, PeerInfo>,
    connected: HashSet<SocketAddr>,
    max_connections: usize,
    pending_connections: usize,
}

impl PeerManager {
    pub fn new(
        max_connections: usize,
        tpool: Arc<ThreadPool>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (socket_tx, socket_rx) = Queue::new(None);
        let (command_tx, command_rx) = Queue::new(None);
        let (connect_tx, connect_rx) = Queue::new(None);
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                peers: HashMap::new(),
                connected: HashSet::new(),
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
        })
    }

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

    pub fn add_peers(&self, new_peers: &[SocketAddr]) {
        let mut inner = self.inner.lock().unwrap();
        for addr in new_peers {
            inner
                .peers
                .entry(*addr)
                .or_insert_with(|| PeerInfo::new(*addr));
        }
    }

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

    pub fn next_peer_message(&self) -> Option<(Vec<u8>, SocketAddr, i32)> {
        if let Ok(data) = self.socket_rx.try_recv() {
            return Some(data);
        }
        None
    }

    pub fn mark_connected(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(p) = inner.peers.get_mut(&addr) {
            p.status = PeerStatus::Connected;
            p.last_seen = Instant::now();
            inner.connected.insert(addr);
        }
    }

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

    pub fn mark_disconnected(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.peers.remove(&addr);
        inner.connected.remove(&addr);
    }

    pub fn connected_peers(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.connected.len()
    }

    pub fn peer_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.peers.len()
    }

    pub fn get_pending_connections(&self) -> usize {
        let state = self.inner.lock().unwrap();
        state.pending_connections
    }

    pub fn connect_peers(
        &self,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
    ) -> &Receiver<(SocketAddr, PeerStatus)> {
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
                for (k, v) in inner.peers.iter() {
                    println!(
                        "{k} => {} (fail={}, status={:?})",
                        v.addr, v.failures, v.status
                    );
                }
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
        &self.connect_rx
    }

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
