use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, mpsc::Sender},
    time::Duration,
    usize,
};

use mio::{Events, Poll};
use mio::{Interest, Token, net::TcpStream as MioTcpStream};
use threadpool::ThreadPool;

use crate::{
    engine::{Event, IoTask, PeerIoTask, ShutdownState},
    peer::{MessageHandle, Peer, PeerMessage},
    picker::PiecePicker,
    rate_limiter::RateLimiter,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const FIRST_PEER_TOKEN: usize = 1;
const TCP_ATTEMPTS: usize = 3;
const HANDSHAKE_ATTEMPTS: usize = 3;
const ATTEMPT_BACKOFF: Duration = Duration::from_millis(300);
const BASE_POLL_TIMEOUT: Duration = Duration::from_millis(5);
const MAX_POLL_TIMEOUT: Duration = Duration::from_millis(100);
const IDLE_BACKOFF_STEP: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub struct ConnectionManager {
    pending_peers: HashSet<SocketAddr>,
    available_peers: Vec<SocketAddr>,
    connected_peers: HashMap<SocketAddr, Peer>,
    peer_io_tx: Sender<PeerIoTask>,
    event_tx: Sender<Event>,
    connector_pool: ThreadPool,
    io_tx: Sender<IoTask>,
    poll: Poll,
    events: Events,
    next_token: usize,
    token_to_addr: HashMap<Token, SocketAddr>,
    max_peers: usize,
    poll_timeout: Duration,
    idle_ticks: u32,
    picker: PiecePicker,
    torrent_size: u64,
    shutdown_state: Arc<ShutdownState>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    total_pieces: usize,
    read_limiter: RateLimiter,
    write_limiter: RateLimiter,
}

impl ConnectionManager {
    pub fn new(
        peer_io_tx: Sender<PeerIoTask>,
        event_tx: Sender<Event>,
        io_tx: Sender<IoTask>,
        picker: PiecePicker,
        torrent_size: u64,
        shutdown_state: Arc<ShutdownState>,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        total_pieces: usize,
        read_limit_bytes_per_sec: usize,
        write_limit_bytes_per_sec: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            pending_peers: HashSet::new(),
            available_peers: Vec::new(),
            connected_peers: HashMap::new(),
            peer_io_tx,
            event_tx,
            connector_pool: create_connector_pool(),
            io_tx,
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            next_token: FIRST_PEER_TOKEN,
            token_to_addr: HashMap::new(),
            max_peers: 0,
            poll_timeout: BASE_POLL_TIMEOUT,
            idle_ticks: 0,
            picker,
            torrent_size,
            shutdown_state,
            info_hash,
            peer_id,
            total_pieces,
            read_limiter: RateLimiter::new(read_limit_bytes_per_sec),
            write_limiter: RateLimiter::new(write_limit_bytes_per_sec),
        })
    }

    pub fn join(&self) {
        self.connector_pool.join();
    }

    fn adaptive_max_peers(&self, total_size: u64) -> usize {
        let base_peers = if total_size < 100 * 1024 * 1024 {
            16
        } else if total_size < 500 * 1024 * 1024 {
            32
        } else {
            50
        };
        if self.connected_peers.is_empty() {
            return base_peers;
        }

        let swarm_speed: f64 = self.connected_peers.values().map(|p| p.speed()).sum();

        let extra_peers = if swarm_speed < 500_000.0 { 5 } else { 0 };

        (base_peers + extra_peers).min(50)
    }

    fn cleanup_peer(&mut self, addr: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        self.pending_peers.remove(addr);
        if let Some(mut peer) = self.connected_peers.remove(addr) {
            self.picker
                .on_peer_disconnected(peer.addr(), peer.bitfield());
            self.token_to_addr.remove(peer.token());
            self.poll.registry().deregister(&mut peer.stream)?;
        }

        Ok(())
    }

    fn maybe_adjust_peers(&mut self) {
        let desired_peers = self.adaptive_max_peers(self.torrent_size);
        self.max_peers = desired_peers;
        let active_peers = self.connected_peers.len() + self.pending_peers.len();

        if active_peers < desired_peers {
            let mut left = desired_peers.saturating_sub(active_peers);
            while left > 0 && !self.available_peers.is_empty() {
                if let Some(peer) = self.available_peers.pop() {
                    if self.pending_peers.contains(&peer)
                        || self.connected_peers.contains_key(&peer)
                    {
                        continue;
                    }
                    self.pending_peers.insert(peer);
                    let _ = self.peer_io_tx.send(PeerIoTask::ConnectPeer { addr: peer });
                    left -= 1;
                } else {
                    break;
                }
            }
        } else if active_peers > desired_peers {
            let mut to_drop = active_peers - desired_peers;
            let mut slow_peers: Vec<_> = self
                .connected_peers
                .values()
                .map(|p| (*p.addr(), p.speed()))
                .collect();
            slow_peers.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            for (addr, _) in slow_peers {
                if to_drop == 0 {
                    break;
                }
                match self.cleanup_peer(&addr) {
                    Ok(()) => {
                        to_drop -= 1;
                    }
                    Err(e) => {
                        eprintln!("failed to cleanup peer: {} :: {}", addr, e);
                    }
                }
            }
        }
    }

    pub fn max_peers(&self) -> usize {
        self.max_peers
    }

    fn on_new_peers(&mut self, peers: Vec<SocketAddr>) {
        for peer in peers {
            if self.pending_peers.contains(&peer) || self.connected_peers.contains_key(&peer) {
                continue;
            }
            self.available_peers.push(peer);
        }
    }

    fn connect_peer(&self, addr: SocketAddr) {
        if !self.shutdown_state.is_running() {
            return;
        }
        let peer_io_tx = self.peer_io_tx.clone();
        let shutdown_state = self.shutdown_state.clone();
        let info_hash = self.info_hash;
        let peer_id = self.peer_id;
        self.connector_pool.execute(move || {
            let mut stream = match tcp_connect(&addr, shutdown_state.clone()) {
                Ok(s) => s,
                Err(e) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                        addr,
                        reason: e.to_string(),
                    });
                    return;
                }
            };
            if !shutdown_state.is_running() {
                return;
            }
            if let Err(e) = peer_handshake(
                &addr,
                &mut stream,
                &info_hash,
                &peer_id,
                shutdown_state.clone(),
            ) {
                let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                    addr,
                    reason: e.to_string(),
                });
                return;
            }
            if !shutdown_state.is_running() {
                return;
            }
            if let Err(e) = stream.set_nonblocking(true) {
                let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                    addr,
                    reason: format!("set_nonblocking failed: {}", e),
                });
                return;
            }
            let _ = peer_io_tx.send(PeerIoTask::PeerConnected { addr, stream });
        });
    }

    fn enqueue_peer_message(&mut self, addr: &SocketAddr, msg: PeerMessage) {
        let peer = match self.connected_peers.get_mut(addr) {
            Some(p) => p,
            None => return,
        };
        peer.enqueue_message(&msg);
    }

    fn on_peer_disconnected(
        &mut self,
        addr: SocketAddr,
        _reason: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(e) = self.cleanup_peer(&addr) {
            eprintln!("failed to cleanup peer: {} :: {}", addr, e);
            return Err(e);
        }
        Ok(())
    }

    fn on_peer_connected(
        &mut self,
        addr: SocketAddr,
        stream: TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.pending_peers.remove(&addr);
        if self.connected_peers.contains_key(&addr) {
            return Ok(());
        }

        let mut mio_stream = MioTcpStream::from_std(stream);
        let token = Token(self.next_token);
        self.next_token += 1;

        self.poll.registry().register(
            &mut mio_stream,
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;

        let peer = Peer::new(mio_stream, self.total_pieces, token)?;
        self.token_to_addr.insert(token, addr);
        self.connected_peers.insert(addr, peer);

        Ok(())
    }

    fn on_peer_connect_failed(
        &mut self,
        addr: SocketAddr,
        _reason: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(e) = self.cleanup_peer(&addr) {
            eprintln!("failed to cleanup peer: {} :: {}", addr, e);
            return Err(e);
        }
        Ok(())
    }

    pub fn poll_peers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.poll.poll(&mut self.events, Some(self.poll_timeout))?;

        let mut activity = false;
        for ev in self.events.iter() {
            let token = ev.token();

            let addr = match self.token_to_addr.get(&token) {
                Some(a) => *a,
                None => continue,
            };

            let conn = match self.connected_peers.get_mut(&addr) {
                Some(c) => c,
                None => continue,
            };

            if ev.is_readable() {
                match conn.socket_read(&mut self.read_limiter, 8) {
                    Ok(n) => {
                        if n > 0 {
                            activity = true;
                        }
                    }
                    Err(e) => {
                        eprintln!("{} failed to read from socket: {}", conn.addr(), e);
                        let _ = self.peer_io_tx.send(PeerIoTask::PeerDisconnected {
                            addr: *conn.addr(),
                            reason: e.to_string(),
                        });
                    }
                }
            }

            if ev.is_writable() {
                match conn.socket_write(&mut self.write_limiter) {
                    Ok(n) => {
                        if n > 0 {
                            activity = true;
                        }
                    }
                    Err(e) => {
                        eprintln!("{} failed to write to socket: {}", conn.addr(), e);
                        let _ = self.peer_io_tx.send(PeerIoTask::PeerDisconnected {
                            addr: *conn.addr(),
                            reason: e.to_string(),
                        });
                    }
                }
            }

            conn.maybe_send_keepalive();
        }

        if activity {
            self.poll_timeout = BASE_POLL_TIMEOUT;
            self.idle_ticks = 0;
        } else {
            self.idle_ticks = self.idle_ticks.saturating_add(1);

            let backoff = IDLE_BACKOFF_STEP * self.idle_ticks;
            self.poll_timeout = (BASE_POLL_TIMEOUT + backoff).min(MAX_POLL_TIMEOUT);
        }

        for peer in self.connected_peers.values() {
            if peer.can_drain() {
                let _ = self
                    .peer_io_tx
                    .send(PeerIoTask::Drain { addr: *peer.addr() });
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, addr: SocketAddr, message_handle: Arc<MessageHandle>) {
        match message_handle.as_ref() {
            MessageHandle::Piece {
                piece,
                offset,
                data,
            } => {
                if let Some(status) = self.picker.on_block_received(&addr, *piece, *offset) {
                    if status.received {
                        let _ = self.io_tx.send(IoTask::WriteToDisk {
                            piece: *piece,
                            offset: *offset,
                            data: data.clone(),
                            complete: status.complete,
                        });
                    }
                    let _ = self
                        .peer_io_tx
                        .send(PeerIoTask::RequestBlocksForPeer { addr });
                }
            }
            MessageHandle::Bitfield { bitfield } | MessageHandle::Have { bitfield } => {
                let _ = self.event_tx.send(Event::CompareBitfield {
                    addr,
                    bitfield: bitfield.clone(),
                });
            }
            MessageHandle::Unchoke => {
                let _ = self
                    .peer_io_tx
                    .send(PeerIoTask::RequestBlocksForPeer { addr });
            }
        }
    }

    fn maybe_update_interest(&mut self, addr: SocketAddr, bitfield_interested: bool) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        if peer.maybe_update_interest(bitfield_interested) {
            let _ = self
                .peer_io_tx
                .send(PeerIoTask::RequestBlocksForPeer { addr });
        }
    }

    fn request_blocks_for_peer(&mut self, addr: SocketAddr) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        if peer.is_choked() || !peer.am_interested() {
            return;
        }
        for req in self.picker.pick_blocks_for_peer(
            peer.addr(),
            peer.bitfield(),
            peer.max_inflight_piece_blocks(),
        ) {
            peer.enqueue_message(&PeerMessage::Request((
                req.piece as u32,
                req.offset as u32,
                req.length as u32,
            )));
        }
    }

    fn maybe_request_blocks(&mut self) {
        let mut peer_addrs: Vec<SocketAddr> = self
            .connected_peers
            .iter()
            .filter(|(_, p)| !p.is_choked() && p.am_interested())
            .map(|(addr, _)| *addr)
            .collect();

        peer_addrs.sort_by(|a, b| {
            let speed_a = self.connected_peers[a].speed();
            let speed_b = self.connected_peers[b].speed();
            speed_b
                .partial_cmp(&speed_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for addr in peer_addrs {
            self.request_blocks_for_peer(addr);
        }
    }

    fn drain_peer(&mut self, addr: &SocketAddr) {
        let peer = match self.connected_peers.get_mut(addr) {
            Some(p) => p,
            None => return,
        };

        let result = peer.drain_messages(|msg| {
            let _ = self.peer_io_tx.send(PeerIoTask::MessageHandle {
                addr: *addr,
                message_handle: Arc::new(msg),
            });
        });
        if let Err(e) = result {
            eprintln!("{} failed to drain: {}", addr, e);
        }
    }

    fn reap_block_timeouts(&mut self) {
        let addrs: Vec<_> = self.connected_peers.keys().cloned().collect();
        for addr in addrs {
            self.reap_block_timeouts_for_peer(&addr);
        }
    }

    pub fn reap_block_timeouts_for_peer(&mut self, addr: &SocketAddr) {
        let peer = match self.connected_peers.get_mut(addr) {
            Some(p) => p,
            None => return,
        };
        peer.expire_requests();
        self.picker.reap_timeouts_for_peer(addr, peer.timeout());
    }

    pub fn connected_peers(&self) -> usize {
        self.connected_peers.len()
    }

    pub fn available_peers(&self) -> usize {
        self.available_peers.len()
    }

    pub fn total_speed_down(&self) -> f64 {
        self.connected_peers.values().map(|p| p.speed()).sum()
    }

    pub fn handle_event(&mut self, ev: PeerIoTask) {
        match ev {
            PeerIoTask::NewPeers { peers } => {
                self.on_new_peers(peers);
            }
            PeerIoTask::ConnectPeer { addr } => {
                self.connect_peer(addr);
            }
            PeerIoTask::PeerConnected { addr, stream } => {
                if let Err(e) = self.on_peer_connected(addr, stream) {
                    eprintln!("failed to add connected peer {} :: {}", addr, e);
                }
            }
            PeerIoTask::PeerConnectFailed { addr, reason } => {
                if let Err(e) = self.on_peer_connect_failed(addr, reason) {
                    eprintln!("failed to handle peer {} connect failure: {}", addr, e);
                }
            }
            PeerIoTask::MaybeConnectPeers => {
                self.maybe_adjust_peers();
            }
            PeerIoTask::PeerDisconnected { addr, reason } => {
                if let Err(e) = self.on_peer_disconnected(addr, reason) {
                    eprintln!("failed to handle peer {} disconnect: {}", addr, e);
                }
            }
            PeerIoTask::Stop => return,
            PeerIoTask::MaybeUpdateInterest {
                addr,
                bitfield_interested,
            } => {
                self.maybe_update_interest(addr, bitfield_interested);
            }
            PeerIoTask::SendMessage { addr, msg } => {
                self.enqueue_peer_message(&addr, msg);
            }
            PeerIoTask::RequestBlocksForPeer { addr } => {
                self.request_blocks_for_peer(addr);
            }
            PeerIoTask::MaybeRequestBlocks => {
                self.maybe_request_blocks();
            }
            PeerIoTask::PeriodicReap => {
                self.reap_block_timeouts();
            }
            PeerIoTask::PieceVerified { piece } => {
                self.picker.on_piece_verified(piece);
            }
            PeerIoTask::PieceVerificationFailed { piece } => {
                self.picker.on_piece_verification_failure(piece);
            }
            PeerIoTask::StatsUpdate => {
                let _ = self.event_tx.send(Event::PeerIoStats {
                    connected_peers: self.connected_peers(),
                    max_peers: self.max_peers(),
                    blocks_inflight: self.picker.in_flight(),
                    available_peers: self.available_peers(),
                    total_speed_down: self.total_speed_down(),
                    opportunistic_downloaded: self.picker.opportunistic_downloaded_bytes(),
                });
            }
            PeerIoTask::MessageHandle {
                addr,
                message_handle,
            } => {
                self.handle_message(addr, message_handle);
            }
            PeerIoTask::Drain { addr } => {
                self.drain_peer(&addr);
            }
        }
    }
}

fn send_handshake(
    stream: &mut TcpStream,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> Result<(), io::Error> {
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
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid handshake",
        ));
    }
    if &buf[28..48] != info_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "info_hash mismatch",
        ));
    }
    Ok(())
}

fn tcp_connect(addr: &SocketAddr, shutdown_state: Arc<ShutdownState>) -> io::Result<TcpStream> {
    let mut last_err = None;

    for attempt in 1..=TCP_ATTEMPTS {
        if !shutdown_state.is_running() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "shutdown in progress",
            ));
        }
        match TcpStream::connect_timeout(addr, CONNECT_TIMEOUT) {
            Ok(stream) => {
                stream.set_read_timeout(Some(CONNECT_TIMEOUT))?;
                stream.set_write_timeout(Some(CONNECT_TIMEOUT))?;
                return Ok(stream);
            }
            Err(e) => match e.kind() {
                io::ErrorKind::TimedOut
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::Interrupted => {
                    last_err = Some(e);
                    let backoff = ATTEMPT_BACKOFF * attempt as u32;
                    std::thread::sleep(backoff);
                    continue;
                }
                _ => return Err(e),
            },
        }
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("tcp connect retries exhausted for {}", addr),
        )
    }))
}

fn peer_handshake(
    addr: &SocketAddr,
    stream: &mut TcpStream,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    shutdown_state: Arc<ShutdownState>,
) -> io::Result<()> {
    let mut last_err = None;

    for attempt in 1..=HANDSHAKE_ATTEMPTS {
        if !shutdown_state.is_running() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "shutdown in progress",
            ));
        }
        match send_handshake(stream, info_hash, peer_id) {
            Ok(()) => return Ok(()),
            Err(e) => match e.kind() {
                io::ErrorKind::TimedOut
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::Interrupted => {
                    last_err = Some(e);
                    let backoff = ATTEMPT_BACKOFF * attempt as u32;
                    std::thread::sleep(backoff);
                    continue;
                }
                _ => return Err(e),
            },
        }
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("handshake retries exhausted for {}", addr),
        )
    }))
}

fn create_connector_pool() -> ThreadPool {
    ThreadPool::with_name("connector_pool".to_string(), 8)
}
