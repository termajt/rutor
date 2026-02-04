use std::{
    fmt,
    io::{self, ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant},
};

use crate::{bitfield::Bitfield, engine::Event, picker::PiecePicker};

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
const MAX_READ_BUF: usize = 512 * 1024;

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
    ///    /// * `bitfield` - A `Bitfield` struct representing which pieces
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
}

#[derive(Debug)]
pub struct PeerStats {
    pub bytes_received: usize,
    pub avg_speed: f64,
    pub last_update: Instant,
}

impl PeerStats {
    pub fn new() -> Self {
        Self {
            bytes_received: 0,
            avg_speed: 0.0,
            last_update: Instant::now(),
        }
    }

    pub fn update(&mut self, bytes: usize) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();

        if elapsed >= 1.0 {
            let instant_rate = self.bytes_received as f64 / elapsed;

            const ALPHA: f64 = 0.1;
            if self.avg_speed == 0.0 {
                self.avg_speed = instant_rate;
            } else {
                self.avg_speed = self.avg_speed * (1.0 - ALPHA) + instant_rate * ALPHA;
            }
            self.bytes_received = 0;
            self.last_update = now;
        } else {
            self.bytes_received += bytes;
        }
    }
}

#[derive(Debug)]
pub struct PeerIoState {
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
}

impl PeerIoState {
    pub fn new() -> Self {
        Self {
            read_buf: Vec::with_capacity(8 * 1024),
            write_buf: Vec::with_capacity(8 * 1024),
        }
    }
}

#[derive(Debug)]
pub struct Peer {
    pub bitfield: Bitfield,
    pub addr: SocketAddr,
    pub choked: bool,
    pub am_choked: bool,
    pub interested: bool,
    pub am_interested: bool,
    pub inflight_requests: usize,
    pub max_pipeline: usize,
    pub outbound_queue: Sender<PeerMessage>,
    pub stats: PeerStats,
}

impl Peer {
    pub fn new(
        addr: SocketAddr,
        total_pieces: usize,
        outbound_queue: Sender<PeerMessage>,
        max_pipeline: usize,
    ) -> Self {
        Self {
            bitfield: Bitfield::new(total_pieces),
            addr,
            choked: true,
            am_choked: true,
            interested: false,
            am_interested: false,
            inflight_requests: 0,
            max_pipeline,
            outbound_queue,
            stats: PeerStats::new(),
        }
    }

    pub fn desired_pipeline(&self, block_size: usize) -> usize {
        if self.stats.avg_speed <= f64::EPSILON {
            // Brand-new or stalled peer: probe gently
            return 4;
        }
        let blocks_per_sec = self.stats.avg_speed / block_size as f64;

        blocks_per_sec.ceil().clamp(1.0, 32.0) as usize
    }

    pub fn try_request_from_peer(
        &mut self,
        picker: &mut PiecePicker,
        torrent_bitfield: &Bitfield,
        event_tx: &Sender<Event>,
    ) {
        if self.am_choked || !self.am_interested {
            return;
        }
        let free = self.max_pipeline.saturating_sub(self.inflight_requests);
        if free == 0 {
            return;
        }
        for req in picker.pick_blocks_for_peer(&self.addr, &self.bitfield, torrent_bitfield, free) {
            self.inflight_requests += 1;
            let _ = event_tx.send(Event::SendPeerMessage {
                addr: self.addr,
                message: PeerMessage::Request((
                    req.piece as u32,
                    req.offset as u32,
                    req.length as u32,
                )),
            });
        }
    }
}

pub fn peer_io_loop(
    addr: SocketAddr,
    mut stream: TcpStream,
    event_tx: Sender<Event>,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    total_pieces: usize,
    messages_rx: Receiver<PeerMessage>,
) {
    if let Err(e) = send_handshake(&mut stream, info_hash, peer_id) {
        let _ = event_tx.send(Event::PeerDisconnected {
            addr,
            reason: e.to_string(),
        });
        return;
    }

    if let Err(e) = stream.set_nonblocking(true) {
        eprintln!("failed to set nonblocking for: {}", addr);
        let _ = event_tx.send(Event::PeerDisconnected {
            addr,
            reason: e.to_string(),
        });
        return;
    }
    let _ = event_tx.send(Event::PeerHandshakeComplete { addr });

    let mut state = PeerIoState::new();
    let mut did_work;
    let mut idle_sleep = Duration::from_millis(1);
    let mut last_keepalive = Instant::now();
    loop {
        did_work = false;
        // READ
        match read_peer_messages_nonblocking(&mut stream, &mut state, total_pieces) {
            Ok(msgs) => {
                if !msgs.is_empty() {
                    did_work = true;
                    for msg in msgs {
                        let _ = event_tx.send(Event::PeerMessage { addr, message: msg });
                    }
                }
            }
            Err(e) => {
                let _ = event_tx.send(Event::PeerDisconnected {
                    addr,
                    reason: e.to_string(),
                });
                break;
            }
        }

        while let Ok(msg) = messages_rx.try_recv() {
            let mut encoded = msg.encode();
            state.write_buf.append(&mut encoded);
        }

        match flush_writes(&mut stream, &mut state) {
            Ok(written) => {
                if written > 0 {
                    did_work = true;
                }
            }
            Err(e) => {
                let _ = event_tx.send(Event::PeerDisconnected {
                    addr,
                    reason: e.to_string(),
                });
                break;
            }
        }

        if last_keepalive.elapsed() >= KEEPALIVE_INTERVAL {
            if state.write_buf.is_empty() {
                let _ = state
                    .write_buf
                    .extend_from_slice(&PeerMessage::KeepAlive.encode());
            }
            last_keepalive = Instant::now();
        }

        if state.read_buf.len() > MAX_READ_BUF {
            let _ = event_tx.send(Event::PeerDisconnected {
                addr,
                reason: "read buffer exceeded maximum allowed size".to_string(),
            });
            break;
        }

        if did_work {
            idle_sleep = Duration::from_millis(1);
        } else {
            std::thread::sleep(idle_sleep);
            idle_sleep = (idle_sleep * 2).min(Duration::from_millis(20));
        }
    }
}

fn send_handshake(
    stream: &mut TcpStream,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}

fn flush_writes(
    stream: &mut TcpStream,
    state: &mut PeerIoState,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut written = 0;
    while !state.write_buf.is_empty() {
        match stream.write(&state.write_buf) {
            Ok(0) => {
                return Err(io::Error::new(ErrorKind::WriteZero, "peer closed connection").into());
            }
            Ok(n) => {
                written += n;
                state.write_buf.drain(0..n);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(written)
}

fn read_peer_messages_nonblocking(
    stream: &mut TcpStream,
    state: &mut PeerIoState,
    total_pieces: usize,
) -> Result<Vec<PeerMessage>, Box<dyn std::error::Error>> {
    let mut temp = [0u8; 4096];

    loop {
        match stream.read(&mut temp) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "peer closed connection",
                )
                .into());
            }
            Ok(n) => {
                state.read_buf.extend_from_slice(&temp[..n]);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }

    let mut messages = Vec::new();

    loop {
        if state.read_buf.len() < 4 {
            break;
        }

        let len = u32::from_be_bytes(state.read_buf[0..4].try_into()?) as usize;

        if state.read_buf.len() < 4 + len {
            break;
        }

        state.read_buf.drain(0..4);

        let payload: Vec<u8> = state.read_buf.drain(0..len).collect();

        let msg = parse_peer_message(&payload, total_pieces)?;
        messages.push(msg);
    }

    Ok(messages)
}

fn parse_peer_message(
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
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("unknown message id: {}", id),
            )
            .into());
        }
    };

    Ok(msg)
}
