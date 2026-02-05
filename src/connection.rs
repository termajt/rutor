use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, mpsc::Sender},
    time::{Duration, Instant},
};

use threadpool::ThreadPool;

use crate::{
    bitfield::Bitfield,
    bytespeed::ByteSpeed,
    engine::{Event, IoTask, PeerIoTask},
    peer::PeerMessage,
    picker::{self, PiecePicker},
};

pub const MAX_PEERS: usize = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_READ_BUF: usize = 512 * 1024;
const MAX_PIPELINE: usize = 32;
const INITIAL_PIPELINE: usize = 4;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
const INITIAL_RTT: Duration = Duration::from_millis(150);
const ALPHA: f64 = 0.2;

#[derive(Debug)]
struct PeerConnection {
    addr: SocketAddr,
    stream: TcpStream,
    read_buf: Vec<u8>,
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
}

impl PeerConnection {
    fn new(addr: SocketAddr, stream: TcpStream, total_pieces: usize) -> Self {
        let speed = ByteSpeed::new(Duration::from_secs(1), true, ALPHA);
        Self {
            addr,
            stream,
            read_buf: Vec::with_capacity(8 * 1024),
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
        self.write_queue.push_back(msg.encode());
    }

    fn poll_reads(&mut self, peer_io_tx: &Sender<PeerIoTask>, total_pieces: usize) -> bool {
        let mut did_work = false;
        let mut buf = [0u8; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerDisconnected {
                        addr: self.addr,
                        reason: "peer closed".to_string(),
                    });
                    break;
                }
                Ok(n) => {
                    did_work = true;
                    if self.read_buf.len() > MAX_READ_BUF {
                        let _ = peer_io_tx.send(PeerIoTask::PeerDisconnected {
                            addr: self.addr,
                            reason: "read buffer overflow".to_string(),
                        });
                        break;
                    }
                    self.read_buf.extend_from_slice(&buf[..n]);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerDisconnected {
                        addr: self.addr,
                        reason: e.to_string(),
                    });
                    break;
                }
            }
        }

        if did_work {
            loop {
                if self.read_buf.len() < 4 {
                    break;
                }

                let len = u32::from_be_bytes(self.read_buf[0..4].try_into().unwrap()) as usize;

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
        }

        did_work
    }

    fn poll_writes(&mut self, peer_io_tx: &Sender<PeerIoTask>) -> bool {
        let mut did_work = false;
        while let Some(front) = self.write_queue.front_mut() {
            match self.stream.write(front) {
                Ok(0) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerDisconnected {
                        addr: self.addr,
                        reason: "peer closed".to_string(),
                    });
                    break;
                }
                Ok(n) => {
                    did_work = true;
                    if n == front.len() {
                        self.write_queue.pop_front();
                    } else {
                        front.drain(0..n);
                        break;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerDisconnected {
                        addr: self.addr,
                        reason: e.to_string(),
                    });
                    break;
                }
            }
        }

        did_work
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
    ) {
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
                self.bitfield.set(&(piece as usize), true);
                let _ = event_tx.send(Event::CompareBitfield {
                    addr: self.addr,
                    bitfield: self.bitfield.clone(),
                });
            }
            PeerMessage::Bitfield(bitfield) => {
                self.bitfield = bitfield;
                let _ = event_tx.send(Event::CompareBitfield {
                    addr: self.addr,
                    bitfield: self.bitfield.clone(),
                });
            }
            PeerMessage::Request(_) => todo!(),
            PeerMessage::Piece((piece, offset, data)) => {
                self.speed.update(data.len());
                let piece = piece as usize;
                let offset = offset as usize;
                if let Some(sent_at) = self.in_flight_requests.remove(&(piece, offset)) {
                    self.update_rtt(sent_at.elapsed());
                }
                self.update_pipeline();
                if let Some(status) = picker.on_block_received(piece, offset) {
                    if status.received {
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
                let _ = event_tx.send(Event::PeerNeedsBlocks { addr: self.addr });
            }
            PeerMessage::Cancel(_) => todo!(),
            PeerMessage::Port(_) => todo!(),
            PeerMessage::KeepAlive => {}
        }
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
        event_tx: &Sender<Event>,
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
            let _ = event_tx.send(Event::PeerNeedsBlocks { addr: self.addr });
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
}

impl ConnectionManager {
    pub fn new(
        peer_io_tx: Sender<PeerIoTask>,
        tpool: Arc<ThreadPool>,
        event_tx: Sender<Event>,
        io_tx: Sender<IoTask>,
    ) -> Self {
        Self {
            pending_peers: HashSet::new(),
            available_peers: Vec::new(),
            connected_peers: HashMap::new(),
            peer_io_tx,
            event_tx,
            tpool,
            io_tx,
        }
    }

    pub fn on_new_peers(&mut self, peers: Vec<SocketAddr>) {
        for peer in peers {
            if self.pending_peers.contains(&peer) || self.connected_peers.contains_key(&peer) {
                continue;
            }
            self.available_peers.push(peer);
        }
    }

    pub fn maybe_connect_peers(&mut self) {
        if self.available_peers.is_empty() {
            return;
        }
        let mut left = MAX_PEERS
            .saturating_sub(self.pending_peers.len())
            .saturating_sub(self.connected_peers.len());
        while left > 0 {
            let peer = match self.available_peers.pop() {
                Some(p) => p,
                None => return,
            };
            if self.pending_peers.contains(&peer) || self.connected_peers.contains_key(&peer) {
                continue;
            }
            self.pending_peers.insert(peer);
            let _ = self.peer_io_tx.send(PeerIoTask::ConnectPeer { addr: peer });
            left = left.saturating_sub(1);
        }
    }

    pub fn connect_peer(&self, addr: SocketAddr, info_hash: [u8; 20], peer_id: [u8; 20]) {
        let peer_io_tx = self.peer_io_tx.clone();
        self.tpool.execute(
            move || match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                Ok(mut stream) => {
                    if let Err(e) = send_handshake(&mut stream, &info_hash, &peer_id) {
                        let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                            addr,
                            reason: e.to_string(),
                        });
                        return;
                    }
                    if let Err(e) = stream.set_nonblocking(true) {
                        let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                            addr,
                            reason: e.to_string(),
                        });
                        return;
                    }
                    let _ = peer_io_tx.send(PeerIoTask::PeerConnected { addr, stream });
                }
                Err(e) => {
                    let _ = peer_io_tx.send(PeerIoTask::PeerConnectFailed {
                        addr,
                        reason: e.to_string(),
                    });
                }
            },
        );
    }

    pub fn enqueue_peer_message(&mut self, addr: &SocketAddr, msg: PeerMessage) {
        let peer = match self.connected_peers.get_mut(addr) {
            Some(p) => p,
            None => return,
        };
        peer.enqueue_message(msg);
    }

    pub fn on_peer_disconnected(&mut self, addr: SocketAddr, reason: String) {
        eprintln!("peer disconnected: {} :: {}", addr, reason);
        self.pending_peers.remove(&addr);
        self.connected_peers.remove(&addr);
        let _ = self.peer_io_tx.send(PeerIoTask::ReapPeerBlocks {
            addr,
            timeout: Duration::ZERO,
        });
        let _ = self.event_tx.send(Event::PeersUpdated {
            connected_peers: self.connected_peers.len(),
        });
    }

    pub fn on_peer_connected(&mut self, addr: SocketAddr, stream: TcpStream, total_pieces: usize) {
        self.pending_peers.remove(&addr);
        if self.connected_peers.contains_key(&addr) {
            return;
        }
        self.connected_peers
            .insert(addr, PeerConnection::new(addr, stream, total_pieces));
        let _ = self.event_tx.send(Event::PeersUpdated {
            connected_peers: self.connected_peers.len(),
        });
    }

    pub fn on_peer_connect_failed(&mut self, addr: SocketAddr, reason: String) {
        eprintln!("peer {} connect failure: {}", addr, reason);
        self.pending_peers.remove(&addr);
        self.connected_peers.remove(&addr);
        let _ = self.event_tx.send(Event::PeersUpdated {
            connected_peers: self.connected_peers.len(),
        });
    }

    pub fn poll_peers(&mut self, total_pieces: usize) -> bool {
        let mut did_work = false;
        for conn in self.connected_peers.values_mut() {
            if conn.poll_reads(&self.peer_io_tx, total_pieces) {
                did_work = true;
            }
            if conn.poll_writes(&self.peer_io_tx) {
                did_work = true;
            }
            conn.maybe_send_keepalive();
        }

        did_work
    }

    pub fn handle_peer_message(
        &mut self,
        addr: SocketAddr,
        msg: PeerMessage,
        picker: &mut PiecePicker,
    ) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        peer.handle_message(msg, &self.event_tx, &self.io_tx, picker);
    }

    pub fn maybe_update_interest(&mut self, addr: SocketAddr, bitfield_interested: bool) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        peer.maybe_update_interest(&self.peer_io_tx, bitfield_interested, &self.event_tx);
    }

    pub fn request_blocks_for_peer(
        &mut self,
        addr: SocketAddr,
        picker: &mut PiecePicker,
        bitfield: &Bitfield,
    ) {
        let peer = match self.connected_peers.get_mut(&addr) {
            Some(p) => p,
            None => return,
        };
        if peer.am_choked || !peer.am_interested {
            return;
        }
        let free = peer
            .max_pipeline
            .saturating_sub(peer.in_flight_requests.len());
        if free == 0 {
            return;
        }
        let requests = picker.pick_blocks_for_peer(&addr, &peer.bitfield, bitfield, free);
        for req in requests {
            peer.record_request(req.piece, req.offset);
            let _ = self.peer_io_tx.send(PeerIoTask::SendMessage {
                addr,
                msg: PeerMessage::Request((req.piece as u32, req.offset as u32, req.length as u32)),
            });
        }
    }

    pub fn maybe_request_blocks(&mut self) {
        for peer in self.connected_peers.values() {
            let _ = self
                .event_tx
                .send(Event::PeerNeedsBlocks { addr: peer.addr });
        }
    }

    pub fn reap_block_timeouts(&mut self, picker: &mut PiecePicker) {
        for peer in self.connected_peers.values_mut() {
            let freed = picker.reap_timeouts_for_peer(&peer.addr, peer.block_timeout());
            for (piece, offset) in freed {
                peer.in_flight_requests.remove(&(piece, offset));
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
