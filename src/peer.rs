use std::{
    collections::VecDeque,
    fmt,
    io::{self, Read, Write},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use mio::{Token, net::TcpStream};

use crate::{
    bitfield::Bitfield,
    bytespeed::ByteSpeed,
    inflight::InFlight,
    pending_requests::{BlockKey, PendingRequests},
    picker,
    rate_limiter::RateLimiter,
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
    Piece((u32, u32, Arc<Vec<u8>>)),

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
                write!(f, "PeerMessage::Bitfield({:?})", bitfield)
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

    fn parse(
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
                PeerMessage::Piece((index, begin, Arc::new(block)))
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
}

#[derive(Debug)]
pub enum MessageHandle {
    Piece {
        piece: usize,
        offset: usize,
        data: Arc<Vec<u8>>,
    },
    Bitfield {
        bitfield: Arc<Bitfield>,
    },
    Have {
        bitfield: Arc<Bitfield>,
    },
    Unchoke,
}

#[derive(Debug)]
pub struct Peer {
    addr: SocketAddr,
    pub stream: TcpStream,
    read_buf: Vec<u8>,
    write_queue: VecDeque<Vec<u8>>,
    bitfield: Bitfield,
    am_choked: bool,
    interested: bool,
    am_interested: bool,
    inflight: InFlight,
    pending: PendingRequests,
    token: Token,
    speed: ByteSpeed,
    last_keepalive: Instant,
    timeout: Duration,
    rtt: Option<Duration>,
    rttvar: Option<Duration>,
    rtt_alpha: f64,
    max_inflight_piece_blocks: usize,
}

const READ_CHUNK: usize = 16 * 1024;
const KEEPALIVE_INTERVAL: Duration = Duration::from_mins(2);
const MIN_RTT: Duration = Duration::from_millis(20);

impl Peer {
    pub fn new(stream: TcpStream, total_pieces: usize, token: Token) -> io::Result<Self> {
        let addr = stream.peer_addr()?;
        let bitfield = Bitfield::new(total_pieces);
        let now = Instant::now();
        Ok(Self {
            addr,
            stream,
            read_buf: Vec::with_capacity(64 * 1024),
            write_queue: VecDeque::new(),
            bitfield,
            am_choked: true,
            interested: false,
            am_interested: false,
            inflight: InFlight::default(),
            pending: PendingRequests::new(),
            token,
            speed: ByteSpeed::new(Duration::from_millis(200), true, 0.02),
            last_keepalive: now,
            timeout: Duration::from_secs(3),
            rtt: None,
            rttvar: None,
            rtt_alpha: 0.2,
            max_inflight_piece_blocks: 32,
        })
    }

    fn update_adaptive_timeout(&mut self) {
        const MIN_TIMEOUT: Duration = Duration::from_secs(1);
        const MAX_TIMEOUT: Duration = Duration::from_secs(10);

        let rtt = self.rtt.unwrap_or(MIN_RTT);
        let rttvar = self.rttvar.unwrap_or(rtt / 2);
        let desired_timeout = rtt + rttvar.mul_f64(4.0);
        let timeout = desired_timeout.clamp(MIN_TIMEOUT, MAX_TIMEOUT);
        self.timeout = timeout;
    }

    fn update_max_inflight_piece_blocks(&mut self) {
        const MIN_BLOCKS: usize = 32;
        const MAX_BLOCKS: usize = 512;
        let rtt = self.rtt.unwrap_or(MIN_RTT);
        let rttvar = self.rttvar.unwrap_or(rtt / 2);
        let effective_rtt = rtt + rttvar.mul_f64(0.5);
        let speed = self.speed.avg_speed.max(1.0);
        let max_inflight_piece_blocks =
            ((speed * effective_rtt.as_secs_f64()) / picker::BLOCK_SIZE as f64).ceil() as usize;
        self.max_inflight_piece_blocks = max_inflight_piece_blocks.clamp(MIN_BLOCKS, MAX_BLOCKS)
    }

    fn update(&mut self, sample: Option<Duration>, bytes: usize) {
        self.update_speed(bytes);
        self.update_rtt(sample);
        self.update_adaptive_timeout();
        self.update_max_inflight_piece_blocks();
    }

    pub fn max_inflight_piece_blocks(&self) -> usize {
        self.max_inflight_piece_blocks
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn bitfield(&self) -> &Bitfield {
        &self.bitfield
    }

    pub fn token(&self) -> &Token {
        &self.token
    }

    pub fn speed(&self) -> f64 {
        self.speed.avg_speed
    }

    pub fn enqueue_message(&mut self, message: &PeerMessage) {
        match message {
            PeerMessage::Request((piece, offset, len)) => {
                self.inflight.add(*len as usize);
                self.pending.insert(BlockKey {
                    piece: *piece,
                    offset: *offset,
                    length: *len,
                });
            }
            _ => {}
        }
        let encoded = message.encode();
        self.write_queue.push_back(encoded);
    }

    pub fn socket_write(&mut self, limiter: &mut RateLimiter) -> io::Result<usize> {
        let mut budget = limiter.allow(self.max_inflight_piece_blocks * picker::BLOCK_SIZE);
        let mut written = 0;
        while budget > 0 {
            let Some(front) = self.write_queue.front_mut() else {
                break;
            };

            let to_write = budget.min(front.len());

            match self.stream.write(&front[..to_write]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "peer closed",
                    ));
                }
                Ok(n) => {
                    written += n;
                    budget -= n;
                    front.drain(..n);
                    if front.is_empty() {
                        self.write_queue.pop_front();
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        Ok(written)
    }

    pub fn socket_read(
        &mut self,
        limiter: &mut RateLimiter,
        max_reads: usize,
    ) -> io::Result<usize> {
        const DEFAULT_MAX_TO_READ: usize = 128 * 1024;
        let max_to_read = limiter.allow(DEFAULT_MAX_TO_READ);
        if max_to_read == 0 {
            eprintln!("{} is not allowed to read at the moment", self.addr);
            return Ok(0);
        }
        let mut total = 0;
        let mut reads = 0;
        let mut buf = [0u8; READ_CHUNK];
        let mut budget = max_to_read;
        while budget > 0 && reads < max_reads && total < max_to_read {
            let remaining_bytes = max_to_read.saturating_sub(total);
            let to_read = budget.min(buf.len()).min(remaining_bytes);
            match self.stream.read(&mut buf[..to_read]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "peer closed",
                    ));
                }
                Ok(n) => {
                    self.read_buf.extend_from_slice(&buf[..n]);
                    total += n;
                    budget -= n;
                    reads += 1;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        Ok(total)
    }

    pub fn can_drain(&self) -> bool {
        self.read_buf.len() >= 4
    }

    pub fn drain_messages<F>(
        &mut self,
        mut message_handle: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(MessageHandle),
    {
        const DEFAULT_MAX_BYTES_PER_READ: usize = 64 * 1024;
        let mut offset = 0;
        let buf_len = self.read_buf.len();
        let mut bytes_read = 0;
        let mut messages_read = 0;
        let max_bytes_per_read =
            (self.max_inflight_piece_blocks * picker::BLOCK_SIZE).min(DEFAULT_MAX_BYTES_PER_READ);
        let max_messages_per_read = (self.max_inflight_piece_blocks / 2).clamp(16, 128);
        while messages_read < max_messages_per_read && bytes_read < max_bytes_per_read {
            if buf_len.saturating_sub(offset) < 4 {
                break;
            }

            let len = u32::from_be_bytes(self.read_buf[offset..offset + 4].try_into()?) as usize;
            if buf_len.saturating_sub(offset) < 4 + len {
                break;
            }

            let payload_start = offset + 4;
            let payload_end = payload_start + len;

            let payload = &self.read_buf[payload_start..payload_end];
            let msg = PeerMessage::parse(payload, self.bitfield.len())?;

            if let Some(handle) = self.handle_message(msg) {
                message_handle(handle);
            }

            offset = payload_end;
            messages_read += 1;
            bytes_read += 4 + len;
        }

        if offset > 0 {
            self.read_buf.drain(..offset);
        }

        Ok(())
    }

    pub fn expire_requests(&mut self) -> Vec<BlockKey> {
        let expired = self.pending.expire(self.timeout);
        for key in &expired {
            self.inflight.complete(key.length as usize);
        }
        expired
    }

    fn handle_message(&mut self, message: PeerMessage) -> Option<MessageHandle> {
        match message {
            PeerMessage::Choke => self.am_choked = true,
            PeerMessage::Unchoke => {
                self.am_choked = false;
                return Some(MessageHandle::Unchoke);
            }
            PeerMessage::Interested => self.interested = true,
            PeerMessage::NotInterested => self.interested = false,
            PeerMessage::Have(piece) => {
                let piece = piece as usize;
                self.bitfield.set(&piece, true);
                return Some(MessageHandle::Have {
                    bitfield: Arc::new(self.bitfield.clone()),
                });
            }
            PeerMessage::Bitfield(bitfield) => {
                self.bitfield.merge(&bitfield);
                return Some(MessageHandle::Bitfield {
                    bitfield: Arc::new(self.bitfield.clone()),
                });
            }
            PeerMessage::Request((piece, offset, len)) => {
                eprintln!("{} REQUEST {} {} {}", self.addr, piece, offset, len)
            }
            PeerMessage::Piece((piece, offset, data)) => {
                let len = data.len();
                let key = BlockKey {
                    piece,
                    offset,
                    length: len as u32,
                };

                let mut sample_opt = None;
                if let Some(p) = self.pending.remove(&key) {
                    sample_opt = Some(Instant::now() - p.requested_at);
                    self.inflight.complete(len);
                }

                self.update(sample_opt, len);

                return Some(MessageHandle::Piece {
                    piece: piece as usize,
                    offset: offset as usize,
                    data,
                });
            }
            PeerMessage::Cancel((piece, offset, len)) => {
                eprintln!("{} CANCEL {} {} {}", self.addr, piece, offset, len)
            }
            PeerMessage::Port(port) => eprintln!("{} PORT {}", self.addr, port),
            PeerMessage::KeepAlive => eprintln!("{} KEEPALIVE", self.addr),
        }

        None
    }

    pub fn maybe_send_keepalive(&mut self) {
        if self.last_keepalive.elapsed() >= KEEPALIVE_INTERVAL {
            eprintln!("sending keepalive to {}", self.addr);
            self.enqueue_message(&PeerMessage::KeepAlive);
            self.last_keepalive = Instant::now();
        }
    }

    pub fn maybe_update_interest(&mut self, bitfield_interested: bool) -> bool {
        let was_interested = self.am_interested;
        self.am_interested = bitfield_interested;
        if was_interested != self.am_interested {
            if self.am_interested {
                self.enqueue_message(&PeerMessage::Interested);
                return true;
            }
        }
        false
    }

    fn update_speed(&mut self, bytes: usize) {
        self.speed.update(bytes);
    }

    fn update_rtt(&mut self, sample: Option<Duration>) {
        const MIN_RTT: Duration = Duration::from_millis(20);
        const MAX_RTT: Duration = Duration::from_secs(2);
        let sample = match sample {
            Some(s) => s.clamp(MIN_RTT, MAX_RTT),
            None => return,
        };
        match (self.rtt, self.rttvar) {
            (Some(rtt), Some(rttvar)) => {
                let alpha = self.rtt_alpha;
                let beta = 0.25;

                let rtt_secs = rtt.as_secs_f64();
                let sample_secs = sample.as_secs_f64();

                let new_rtt = alpha * sample_secs + (1.0 - alpha) * rtt_secs;

                let deviation = (sample_secs - rtt_secs).abs();
                let new_rttvar = beta * deviation + (1.0 - beta) * rttvar.as_secs_f64();

                self.rtt = Some(Duration::from_secs_f64(new_rtt));
                self.rttvar = Some(Duration::from_secs_f64(new_rttvar));
            }
            _ => {
                self.rtt = Some(sample);
                self.rttvar = Some(sample / 2);
            }
        }
    }

    pub fn is_choked(&self) -> bool {
        self.am_choked
    }

    pub fn am_interested(&self) -> bool {
        self.am_interested
    }
}
