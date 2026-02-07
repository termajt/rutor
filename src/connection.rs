use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, mpsc::Sender},
    time::{Duration, Instant},
};

use mio::{Events, Poll};
use mio::{Interest, Token, net::TcpStream as MioTcpStream};
use threadpool::ThreadPool;

use crate::{
    bitfield::Bitfield,
    bytespeed::ByteSpeed,
    engine::{Event, IoTask, PeerIoTask},
    event::ManualResetEvent,
    peer::PeerMessage,
    picker::{self, PiecePicker},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_READ_BUF: usize = 2 * 1024 * 1024;
const INITIAL_READ_BUF: usize = 8 * 1024;
const MAX_PIPELINE: usize = 128;
const INITIAL_PIPELINE: usize = 16;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
const INITIAL_RTT: Duration = Duration::from_millis(150);
const ALPHA: f64 = 0.2;
const FIRST_PEER_TOKEN: usize = 1;
const TCP_ATTEMPTS: usize = 3;
const HANDSHAKE_ATTEMPTS: usize = 3;
const ATTEMPT_BACKOFF: Duration = Duration::from_millis(300);
const MAX_MESSAGE_LEN: usize = 1024 * 1024;
const BASE_POLL_TIMEOUT: Duration = Duration::from_millis(5);
const MAX_POLL_TIMEOUT: Duration = Duration::from_millis(100);
const IDLE_BACKOFF_STEP: Duration = Duration::from_millis(10);

#[derive(Debug)]
enum MessageHandleResult {
    PieceHandler {
        request_more: bool,
        decrement_inflight: bool,
    },
}

#[derive(Debug)]
struct PeerConnection {
    addr: SocketAddr,
    stream: MioTcpStream,
    read_buf: VecDeque<u8>,
    write_queue: VecDeque<Vec<u8>>,
    bitfield: Bitfield,
    am_choked: bool,
    interested: bool,
    am_interested: bool,
    in_flight_requests: HashMap<(usize, usize), Instant>,
    speed: ByteSpeed,
    assumed_rtt: Duration,
    max_pipeline: usize,
    last_keepalive: Instant,
    token: Token,
}

impl PeerConnection {
    fn new(addr: SocketAddr, stream: MioTcpStream, total_pieces: usize, token: Token) -> Self {
        let speed = ByteSpeed::new(Duration::from_secs(1), true, ALPHA);
        Self {
            addr,
            stream,
            read_buf: VecDeque::with_capacity(INITIAL_READ_BUF),
            write_queue: VecDeque::new(),
            bitfield: Bitfield::new(total_pieces),
            am_choked: true,
            interested: false,
            am_interested: false,
            in_flight_requests: HashMap::new(),
            speed,
            assumed_rtt: INITIAL_RTT,
            max_pipeline: INITIAL_PIPELINE,
            last_keepalive: Instant::now(),
            token,
        }
    }

    fn update_rtt(&mut self, sample: Duration) {
        let new = self.assumed_rtt.as_secs_f64() * (1.0 - ALPHA) + sample.as_secs_f64() * ALPHA;

        self.assumed_rtt =
            Duration::from_secs_f64(new).clamp(Duration::from_millis(50), Duration::from_secs(2));
    }

    fn effective_rtt(&self) -> Duration {
        self.assumed_rtt
    }

    fn maybe_send_keepalive(&mut self) {
        if self.last_keepalive.elapsed() > KEEPALIVE_INTERVAL {
            if self.write_queue.is_empty() {
                self.enqueue_message(PeerMessage::KeepAlive);
            }
            self.last_keepalive = Instant::now();
        }
    }

    fn desired_pipeline(&self) -> usize {
        let speed = self.speed.avg_speed;
        if speed <= f64::EPSILON {
            // Brand-new or stalled peer: probe gently
            return INITIAL_PIPELINE;
        }
        let rtt = self.effective_rtt();
        let bytes_in_flight = speed * rtt.as_secs_f64();
        let blocks = (bytes_in_flight / picker::BLOCK_SIZE as f64).ceil() as usize;

        let pipeline = blocks.clamp(INITIAL_PIPELINE, MAX_PIPELINE);
        pipeline
    }

    fn record_request(&mut self, piece: usize, offset: usize) {
        self.in_flight_requests
            .insert((piece, offset), Instant::now());
    }

    fn enqueue_message(&mut self, msg: PeerMessage) {
        match msg {
            PeerMessage::Request((piece, offset, _)) => {
                self.record_request(piece as usize, offset as usize);
            }
            _ => {}
        }
        self.write_queue.push_back(msg.encode());
    }

    fn poll_reads(&mut self) -> io::Result<usize> {
        let mut buf = [0u8; 4096];
        let mut total_read = 0;
        const MAX_CHUNKS_PER_POLL: usize = 8;
        let mut chunks_read = 0;
        while chunks_read < MAX_CHUNKS_PER_POLL {
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "peer closed",
                    ));
                }
                Ok(n) => {
                    if self.read_buf.len() + n > MAX_READ_BUF {
                        return Err(io::Error::new(io::ErrorKind::Other, "read buffer overflow"));
                    }
                    if self.read_buf.len() + n > self.read_buf.capacity() {
                        let new_capacity = (self.read_buf.len() + n).min(MAX_READ_BUF);
                        self.read_buf
                            .reserve(new_capacity - self.read_buf.capacity());
                    }
                    self.read_buf.extend(&buf[..n]);
                    total_read += n;
                    chunks_read += 1;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        Ok(total_read)
    }

    fn drain_messages(
        &mut self,
        peer_io_tx: &Sender<PeerIoTask>,
        total_pieces: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        while self.read_buf.len() >= 4 {
            let len_bytes: Vec<u8> = self.read_buf.iter().take(4).copied().collect();
            let len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;

            if len > MAX_MESSAGE_LEN {
                return Err(
                    io::Error::new(io::ErrorKind::InvalidData, "peer message too large").into(),
                );
            }
            if self.read_buf.len() < 4 + len {
                break;
            }

            self.read_buf.drain(0..4);
            let payload: Vec<u8> = self.read_buf.drain(0..len).collect();
            if let Ok(msg) = self.parse_peer_message(&payload, total_pieces) {
                let _ = peer_io_tx.send(PeerIoTask::PeerMessage {
                    addr: self.addr,
                    msg,
                });
            }
        }

        Ok(())
    }

    fn poll_writes(&mut self) -> io::Result<usize> {
        let mut total_written = 0;
        while let Some(front) = self.write_queue.front_mut() {
            match self.stream.write(front) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "peer closed",
                    ));
                }
                Ok(n) => {
                    total_written += n;
                    if n == front.len() {
                        self.write_queue.pop_front();
                    } else {
                        front.drain(0..n);
                        break;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        Ok(total_written)
    }

    fn parse_peer_message(
        &self,
        payload: &[u8],
        total_pieces: usize,
    ) -> Result<PeerMessage, Box<dyn std::error::Error>> {
        if payload.is_empty() {
            return Ok(PeerMessage::KeepAlive);
        }

        let id = payload[0];
        let data = &payload[1..];

        let msg = match id {
            0 => PeerMessage::Choke,
            1 => PeerMessage::Unchoke,
            2 => PeerMessage::Interested,
            3 => PeerMessage::NotInterested,
            4 => PeerMessage::Have(u32::from_be_bytes(data[..4].try_into()?)),
            5 => PeerMessage::Bitfield(Bitfield::from_bytes(data.to_vec(), total_pieces)),
            6 => {
                let index = u32::from_be_bytes(data[0..4].try_into()?);
                let begin = u32::from_be_bytes(data[4..8].try_into()?);
                let length = u32::from_be_bytes(data[8..12].try_into()?);
                PeerMessage::Request((index, begin, length))
            }
            7 => {
                let index = u32::from_be_bytes(data[0..4].try_into()?);
                let begin = u32::from_be_bytes(data[4..8].try_into()?);
                let block = data[8..].to_vec();
                PeerMessage::Piece((index, begin, block))
            }
            8 => {
                let index = u32::from_be_bytes(data[0..4].try_into()?);
                let begin = u32::from_be_bytes(data[4..8].try_into()?);
                let length = u32::from_be_bytes(data[8..12].try_into()?);
                PeerMessage::Cancel((index, begin, length))
            }
            9 => PeerMessage::Port(u16::from_be_bytes(data[0..2].try_into()?)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown message id: {}", id),
                )
                .into());
            }
        };

        Ok(msg)
    }

    fn handle_message(
        &mut self,
        msg: PeerMessage,
        event_tx: &Sender<Event>,
        io_tx: &Sender<IoTask>,
        picker: &mut PiecePicker,
    ) -> Option<MessageHandleResult> {
        let mut result = None;
        match msg {
            PeerMessage::Choke => {
                self.am_choked = true;
                self.write_queue.clear();
                self.in_flight_requests.clear();
            }
            PeerMessage::Unchoke => self.am_choked = false,
            PeerMessage::Interested => self.interested = true,
            PeerMessage::NotInterested => self.interested = false,
            PeerMessage::Have(piece) => {
                let piece = piece as usize;
                self.bitfield.set(&piece, true);
                picker.on_peer_have(&piece);
                let _ = event_tx.send(Event::CompareBitfield {
                    addr: self.addr,
                    bitfield: self.bitfield.clone(),
                });
            }
            PeerMessage::Bitfield(bitfield) => {
                self.bitfield = bitfield;
                picker.on_peer_bitfield(&self.bitfield);
                let _ = event_tx.send(Event::CompareBitfield {
                    addr: self.addr,
                    bitfield: self.bitfield.clone(),
                });
            }
            PeerMessage::Request(_) => todo!(),
            PeerMessage::Piece((piece, offset, data)) => {
                let piece = piece as usize;
                let offset = offset as usize;
                let mut decrement_global = false;
                if let Some(status) = picker.on_block_received(&self.addr, piece, offset) {
                    if status.received {
                        self.speed.update(data.len());
                        if let Some(sent_at) = self.in_flight_requests.remove(&(piece, offset)) {
                            self.update_rtt(sent_at.elapsed());
                            decrement_global = true;
                        }
                        self.update_pipeline();
                        let _ = io_tx.send(IoTask::WriteToDisk {
                            piece,
                            offset,
                            data,
                        });
                    }
                    if status.complete {
                        let _ = io_tx.send(IoTask::DiskVerifyPiece {
                            piece,
                            expected_hash: status.expected_hash,
                        });
                    }
                }
                result = Some(MessageHandleResult::PieceHandler {
                    request_more: true,
                    decrement_inflight: decrement_global,
                });
            }
            PeerMessage::Cancel(_) => todo!(),
            PeerMessage::Port(_) => todo!(),
            PeerMessage::KeepAlive => {}
        }

        result
    }

    fn update_pipeline(&mut self) {
        let desired = self.desired_pipeline();

        if desired > self.max_pipeline {
            self.max_pipeline += 1;
        } else {
            self.max_pipeline = desired;
        }
    }

    fn maybe_update_interest(
        &mut self,
        peer_io_tx: &Sender<PeerIoTask>,
        bitfield_interested: bool,
    ) {
        let was_interested = self.am_interested;
        self.am_interested = bitfield_interested;
        if was_interested != self.am_interested {
            let msg = {
                if self.am_interested {
                    PeerMessage::Interested
                } else {
                    PeerMessage::NotInterested
                }
            };
            let _ = peer_io_tx.send(PeerIoTask::SendMessage {
                addr: self.addr,
                msg,
            });
        }
        if self.am_interested {
            let _ = peer_io_tx.send(PeerIoTask::RequestBlocksForPeer { addr: self.addr });
        }
    }

    fn block_timeout(&self) -> Duration {
        const MIN_TIMEOUT: Duration = Duration::from_secs(3);
        const MAX_TIMEOUT: Duration = Duration::from_secs(30);

        const SAFETY_FACTOR: f64 = 4.0;

        if self.speed.avg_speed <= f64::EPSILON {
            return Duration::from_secs(10);
        }

        let expected = picker::BLOCK_SIZE as f64 / self.speed.avg_speed;
        let timeout_secs = expected * SAFETY_FACTOR;

        Duration::from_secs_f64(timeout_secs).clamp(MIN_TIMEOUT, MAX_TIMEOUT)
    }

    fn reap_block_timeouts(
        &mut self,
        picker: &mut PiecePicker,
        timeout_opt: Option<Duration>,
    ) -> usize {
        let timeout = match timeout_opt {
            Some(t) => t,
            None => self.block_timeout(),
        };
        let freed = picker.reap_timeouts_for_peer(&self.addr, timeout);
        let count = freed.len();
        for (piece, offset) in freed {
            self.in_flight_requests.remove(&(piece, offset));
        }

        count
    }
}

#[derive(Debug)]
pub struct ConnectionManager {
    pending_peers: HashSet<SocketAddr>,
    available_peers: Vec<SocketAddr>,
    connected_peers: HashMap<SocketAddr, PeerConnection>,
    peer_io_tx: Sender<PeerIoTask>,
    event_tx: Sender<Event>,
    tpool: Arc<ThreadPool>,
    io_tx: Sender<IoTask>,
    poll: Poll,
    events: Events,
    next_token: usize,
    token_to_addr: HashMap<Token, SocketAddr>,
    max_peers: usize,
    poll_timeout: Duration,
    idle_ticks: u32,
    global_in_flight: usize,
    picker: PiecePicker,
    torrent_size: u64,
    stop_signal: Arc<ManualResetEvent>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    total_pieces: usize,
}

impl ConnectionManager {
    pub fn new(
        peer_io_tx: Sender<PeerIoTask>,
        tpool: Arc<ThreadPool>,
        event_tx: Sender<Event>,
        io_tx: Sender<IoTask>,
        picker: PiecePicker,
        torrent_size: u64,
        stop_signal: Arc<ManualResetEvent>,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        total_pieces: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            pending_peers: HashSet::new(),
            available_peers: Vec::new(),
            connected_peers: HashMap::new(),
            peer_io_tx,
            event_tx,
            tpool,
            io_tx,
            poll: Poll::new()?,
            events: Events::with_capacity(1024),
            next_token: FIRST_PEER_TOKEN,
            token_to_addr: HashMap::new(),
            max_peers: 0,
            poll_timeout: BASE_POLL_TIMEOUT,
            idle_ticks: 0,
            global_in_flight: 0,
            picker,
            torrent_size,
            stop_signal,
            info_hash,
            peer_id,
            total_pieces,
        })
    }

    fn adaptive_max_peers(&self, total_size: u64) -> usize {
        let base_peers = if total_size < 100 * 1024 * 1024 {
            16
        } else if total_size < 500 * 1024 * 1024 {
            32
        } else if total_size < 2 * 1024 * 1024 * 1024 {
            50
        } else {
            80
        };

        let swarm_speed: f64 = self
            .connected_peers
            .values()
            .map(|p| p.speed.avg_speed)
            .sum();

        let extra_peers = if swarm_speed < 500_000.0 { 5 } else { 0 };

        (base_peers + extra_peers).min(100)
    }

    fn cleanup_peer(&mut self, addr: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        self.pending_peers.remove(addr);
        if let Some(mut peer) = self.connected_peers.remove(addr) {
            let freed = peer.reap_block_timeouts(&mut self.picker, Some(Duration::ZERO));
            self.global_in_flight = self.global_in_flight.saturating_sub(freed);
            self.picker.on_peer_disconnected(&peer.addr, &peer.bitfield);
            self.token_to_addr.remove(&peer.token);
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
                .map(|p| (p.addr, p.speed.avg_speed))
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
        if self.stop_signal.is_set() {
            return;
        }
        let peer_io_tx = self.peer_io_tx.clone();
        let stop_signal = self.stop_signal.clone();
        let info_hash = self.info_hash;
        let peer_id = self.peer_id;
        self.tpool.execute(move || {
            let mut stream = match tcp_connect(&addr, stop_signal.clone()) {
                Ok(s) => s,
                Err(e) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                        addr,
                        reason: e.to_string(),
                    });
                    return;
                }
            };
            if stop_signal.is_set() {
                return;
            }
            if let Err(e) = peer_handshake(
                &addr,
                &mut stream,
                &info_hash,
                &peer_id,
                stop_signal.clone(),
            ) {
                let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                    addr,
                    reason: e.to_string(),
                });
                return;
            }
            if stop_signal.is_set() {
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
        peer.enqueue_message(msg);
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

        self.token_to_addr.insert(token, addr);
        self.connected_peers.insert(
            addr,
            PeerConnection::new(addr, mio_stream, self.total_pieces, token),
        );

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

    pub fn poll_peers(&mut self) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
        self.poll.poll(&mut self.events, Some(self.poll_timeout))?;

        let mut activity = false;
        let mut drains_needed = Vec::new();
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
                match conn.poll_reads() {
                    Ok(read) if read > 0 => activity = true,
                    Ok(_) => {}
                    Err(e) => {
                        let _ = self.peer_io_tx.send(PeerIoTask::PeerDisconnected {
                            addr: conn.addr,
                            reason: e.to_string(),
                        });
                        continue;
                    }
                }
                if !conn.read_buf.is_empty() {
                    drains_needed.push(conn.addr);
                }
            }

            if ev.is_writable() {
                match conn.poll_writes() {
                    Ok(wrote) if wrote > 0 => activity = true,
                    Ok(_) => {}
                    Err(e) => {
                        let _ = self.peer_io_tx.send(PeerIoTask::PeerDisconnected {
                            addr: conn.addr,
                            reason: e.to_string(),
                        });
                        continue;
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

        Ok(drains_needed)
    }

    pub fn drain_peer_messages(&mut self, addrs: Vec<SocketAddr>, total_pieces: usize) {
        for addr in addrs {
            let conn = match self.connected_peers.get_mut(&addr) {
                Some(p) => p,
                None => return,
            };
            if let Err(e) = conn.drain_messages(&self.peer_io_tx, total_pieces) {
                eprintln!("failed to drain messages for: {} :: {}", conn.addr, e);
            }
        }
    }

    fn handle_peer_message(&mut self, addr: SocketAddr, msg: PeerMessage) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        if let Some(result) =
            peer.handle_message(msg, &self.event_tx, &self.io_tx, &mut self.picker)
        {
            match result {
                MessageHandleResult::PieceHandler {
                    request_more,
                    decrement_inflight,
                } => {
                    if decrement_inflight {
                        self.global_in_flight = self.global_in_flight.saturating_sub(1);
                    }
                    if request_more {
                        self.request_blocks_for_peer(addr);
                    }
                }
            }
        }
    }

    fn maybe_update_interest(&mut self, addr: SocketAddr, bitfield_interested: bool) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        peer.maybe_update_interest(&self.peer_io_tx, bitfield_interested);
    }

    fn request_blocks_for_peer(&mut self, addr: SocketAddr) {
        let global_max_inflight = self.calculate_global_max_inflight();
        let free_global = global_max_inflight.saturating_sub(self.global_in_flight);
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        if peer.am_choked || !peer.am_interested {
            return;
        }

        let free_peer = peer
            .max_pipeline
            .saturating_sub(peer.in_flight_requests.len());
        let free = free_peer.min(free_global);
        if free == 0 {
            return;
        }

        let requests = self
            .picker
            .pick_blocks_for_peer(&addr, &peer.bitfield, free);
        for req in requests {
            peer.record_request(req.piece, req.offset);
            peer.enqueue_message(PeerMessage::Request((
                req.piece as u32,
                req.offset as u32,
                req.length as u32,
            )));
            self.global_in_flight += 1;
        }
    }

    fn calculate_global_max_inflight(&mut self) -> usize {
        let base = 16;
        let active_peers = self.connected_peers.len().max(1);

        let avg_speed: f64 = self
            .connected_peers
            .values()
            .map(|p| p.speed.avg_speed)
            .sum::<f64>()
            / active_peers as f64;

        let speed_factor = (avg_speed / 50_000.0).clamp(0.5, 2.0);

        let inflight = (base as f64 * active_peers as f64 * speed_factor) as usize;

        inflight.clamp(32, 1024)
    }

    fn maybe_request_blocks(&mut self) {
        for peer in self.connected_peers.values() {
            let _ = self
                .peer_io_tx
                .send(PeerIoTask::RequestBlocksForPeer { addr: peer.addr });
        }
    }

    fn reap_block_timeouts(&mut self) {
        let addrs: Vec<_> = self.connected_peers.keys().cloned().collect();
        for addr in addrs {
            self.reap_block_timeouts_for_peer(&addr);
        }
    }

    pub fn reap_block_timeouts_for_peer(&mut self, addr: &SocketAddr) {
        let freed = match self.connected_peers.get_mut(addr) {
            Some(p) => p.reap_block_timeouts(&mut self.picker, None),
            None => {
                let freed = self.picker.reap_timeouts_for_peer(addr, Duration::ZERO);
                freed.len()
            }
        };
        self.global_in_flight = self.global_in_flight.saturating_sub(freed)
    }

    pub fn connected_peers(&self) -> usize {
        self.connected_peers.len()
    }

    pub fn available_peers(&self) -> usize {
        self.available_peers.len()
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
            PeerIoTask::PeerMessage { addr, msg } => {
                self.handle_peer_message(addr, msg);
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
            PeerIoTask::PeriodicReap {
                should_enter_endgame,
            } => {
                self.reap_block_timeouts();
                if should_enter_endgame {
                    self.picker.enter_endgame();
                }
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
                });
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

fn tcp_connect(addr: &SocketAddr, stop_signal: Arc<ManualResetEvent>) -> io::Result<TcpStream> {
    let mut last_err = None;

    for attempt in 1..=TCP_ATTEMPTS {
        if stop_signal.is_set() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "stop was signaled",
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
                    stop_signal.wait_timeout(backoff);
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
    stop_signal: Arc<ManualResetEvent>,
) -> io::Result<()> {
    let mut last_err = None;

    for attempt in 1..=HANDSHAKE_ATTEMPTS {
        if stop_signal.is_set() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "stop was signaled",
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
                    stop_signal.wait_timeout(backoff);
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
