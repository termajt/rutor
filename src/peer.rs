use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use mio::Token;

use crate::{
    BLOCK_SIZE, PeerMessage,
    bitfield::Bitfield,
    engine::{Tick, duration_to_ticks},
};

const PEER_IDLE_TIMEOUT: Tick = duration_to_ticks(Duration::from_mins(2));
const HANDSHAKE_TIMEOUT: Tick = duration_to_ticks(Duration::from_secs(10));
const KEEPALIVE_INTERVAL: Tick = duration_to_ticks(Duration::from_mins(2));

#[derive(Debug)]
pub enum PeerAction {
    Close {
        token: Token,
    },
    PeerBitfield {
        token: Token,
        bitfield: Bitfield,
    },
    PeerHave {
        token: Token,
        piece: u32,
    },
    PeerConnected {
        token: Token,
    },
    HandshakeSuccess {
        token: Token,
    },
    PickBlocks {
        token: Token,
        count: usize,
    },
    Write {
        token: Token,
    },
    BlockReceived {
        token: Token,
        piece: u32,
        offset: u32,
        data: Bytes,
    },
    TimeoutBlocks {
        blocks: Vec<(u32, u32)>,
    },
    RequestBlocks,
    Choked {
        token: Token,
    },
    RequestMissingBlocks {
        token: Token,
        bitfield: Bitfield,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum PeerState {
    Handshaking,
    Active,
}

const RATE_ALPHA: f64 = 0.2;

#[derive(Debug)]
struct PeerStats {
    downloaded_bytes: u64,
    uploaded_bytes: u64,
    download_rate: f64,
    upload_rate: f64,
    last_rate_tick: Tick,
    rtt_ema: Option<f64>,
}

impl PeerStats {
    fn new() -> Self {
        Self {
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            download_rate: 0.0,
            upload_rate: 0.0,
            last_rate_tick: Tick(0),
            rtt_ema: None,
        }
    }
}

const MIN_OUTSTANDING: usize = 2;
const MAX_OUTSTANDING: usize = 32;

#[derive(Debug)]
struct Peer {
    addr: SocketAddr,
    token: Token,
    choked: bool,
    interested: bool,
    peer_choked: bool,
    peer_interested: bool,
    bitfield: Option<Bitfield>,
    peer_id: Option<[u8; 20]>,
    incoming: BytesMut,
    outgoing: VecDeque<Bytes>,
    handshake_deadline: Tick,
    last_activity: Tick,
    last_outbound: Tick,
    state: PeerState,
    stats: PeerStats,
    pending_requests: HashMap<(u32, u32), Tick>,
}

impl Peer {
    fn new(addr: SocketAddr, token: Token) -> Self {
        Self {
            addr,
            token,
            choked: true,
            interested: false,
            peer_choked: true,
            peer_interested: false,
            bitfield: None,
            peer_id: None,
            incoming: BytesMut::with_capacity(64 * 1024),
            outgoing: VecDeque::new(),
            handshake_deadline: Tick(0),
            last_activity: Tick(0),
            last_outbound: Tick(0),
            state: PeerState::Handshaking,
            stats: PeerStats::new(),
            pending_requests: HashMap::new(),
        }
    }

    fn start_handshake(&mut self, now: Tick) {
        self.state = PeerState::Handshaking;
        self.handshake_deadline = now + HANDSHAKE_TIMEOUT;
    }

    fn can_request_blocks(&self) -> bool {
        if self.choked || !self.interested {
            return false;
        }
        self.pending_requests.len() < self.desired_outstanding_requests()
    }

    fn desired_outstanding_requests(&self) -> usize {
        let rtt = match self.stats.rtt_ema {
            Some(rtt) => rtt.max(0.1),
            None => return 4,
        };

        let bw = self.stats.download_rate.max(1.0);

        let bdp_bytes = bw * rtt;
        let blocks = (bdp_bytes / BLOCK_SIZE as f64).ceil() as usize;

        blocks.clamp(MIN_OUTSTANDING, MAX_OUTSTANDING)
    }

    fn get_request_block_count(&self) -> usize {
        self.desired_outstanding_requests()
            .saturating_sub(self.pending_requests.len())
    }

    fn calculate_timeout(&self) -> Tick {
        let expected_secs = if let Some(rtt) = self.stats.rtt_ema {
            let bw = self.stats.download_rate.max(1.0);
            let block_time = BLOCK_SIZE as f64 / bw;
            block_time + rtt * 1.5
        } else {
            10.0
        };

        duration_to_ticks(Duration::from_secs_f64(expected_secs))
    }

    fn remove_timed_out_requests(&mut self, now: Tick) -> Vec<(u32, u32)> {
        let timeout = self.calculate_timeout();
        let mut timed_out = Vec::new();
        self.pending_requests.retain(|&(piece, offset), sent_at| {
            if now >= *sent_at + timeout {
                timed_out.push((piece, offset));
                false
            } else {
                true
            }
        });
        timed_out
    }
}

#[derive(Debug)]
pub struct PeerManager {
    peers: HashMap<Token, Peer>,
    total_pieces: usize,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
}

impl PeerManager {
    pub fn new(total_pieces: usize, info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Self {
            peers: HashMap::new(),
            total_pieces,
            info_hash,
            peer_id,
        }
    }

    fn build_handshake(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(68);

        buf.put_u8(19);
        buf.extend_from_slice(b"BitTorrent protocol");
        buf.extend_from_slice(&[0u8; 8]);
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);

        buf.freeze()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn on_tick(&mut self, now: Tick, endgame: bool) -> Vec<PeerAction> {
        let mut actions = Vec::new();

        for peer in self.peers.values_mut() {
            update_peer_rate(now, peer);
            match peer.state {
                PeerState::Handshaking => {
                    if now >= peer.handshake_deadline {
                        eprintln!("<> {} handshake timeout", peer.addr);
                        actions.push(PeerAction::Close { token: peer.token });
                    }
                }
                PeerState::Active => {
                    if now >= peer.last_activity + PEER_IDLE_TIMEOUT {
                        eprintln!("<> {} idle timeout", peer.addr);
                        actions.push(PeerAction::Close { token: peer.token });
                        continue;
                    }

                    if now >= peer.last_outbound + KEEPALIVE_INTERVAL {
                        eprintln!("< {} keepalive", peer.addr);
                        push_write_peer_msg(peer, &PeerMessage::KeepAlive, &mut actions, now);
                    }

                    let timeout_blocks = peer.remove_timed_out_requests(now);
                    if !timeout_blocks.is_empty() {
                        actions.push(PeerAction::TimeoutBlocks {
                            blocks: timeout_blocks,
                        });
                        if !endgame {
                            if peer.can_request_blocks() {
                                actions.push(PeerAction::PickBlocks {
                                    token: peer.token,
                                    count: peer.get_request_block_count(),
                                });
                            }
                        }
                    }
                    if endgame && peer.can_request_blocks() {
                        if let Some(bf) = &peer.bitfield {
                            actions.push(PeerAction::RequestMissingBlocks {
                                token: peer.token,
                                bitfield: bf.clone(),
                            });
                        }
                    }
                }
            }
        }

        actions
    }

    pub fn on_request_missing_blocks(
        &mut self,
        token: Token,
        missing_blocks: Vec<(u32, u32)>,
    ) -> Vec<PeerAction> {
        let peer = match self.peers.get_mut(&token) {
            Some(p) => p,
            None => return vec![],
        };
        if peer.can_request_blocks() {
            let count = missing_blocks.len().min(peer.get_request_block_count());
            if count > 0 {
                return vec![PeerAction::PickBlocks { token, count }];
            } else {
                eprintln!(
                    ">>> {} cannot request blocks, choked={}, interested={}, desired_count={}, inflight={}, missing_block_count={}",
                    peer.addr,
                    peer.choked,
                    peer.interested,
                    peer.desired_outstanding_requests(),
                    peer.pending_requests.len(),
                    missing_blocks.len()
                )
            }
        } else {
            eprintln!(
                "> {} cannot request blocks, choked={}, interested={}, desired_count={}, inflight={}, missing_block_count={}",
                peer.addr,
                peer.choked,
                peer.interested,
                peer.desired_outstanding_requests(),
                peer.pending_requests.len(),
                missing_blocks.len()
            )
        }

        vec![]
    }

    pub fn on_request_blocks(&mut self) -> Vec<PeerAction> {
        let mut peers: Vec<_> = self
            .peers
            .values()
            .filter(|p| p.can_request_blocks())
            .collect();
        peers.sort_by(|a, b| {
            peer_score(b)
                .partial_cmp(&peer_score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut actions = Vec::new();
        for peer in peers {
            actions.push(PeerAction::PickBlocks {
                token: peer.token,
                count: peer.get_request_block_count(),
            });
        }

        actions
    }

    pub fn on_cancel_block(
        &mut self,
        piece: u32,
        offset: u32,
        length: u32,
        now: Tick,
    ) -> Vec<PeerAction> {
        let mut actions = Vec::new();
        let key = (piece, offset);
        let msg = PeerMessage::Cancel((piece, offset, length));
        for peer in self.peers.values_mut() {
            if peer.pending_requests.remove(&key).is_some() {
                push_write_peer_msg(peer, &msg, &mut actions, now);
            }
        }

        actions
    }

    pub fn on_data(&mut self, token: Token, data: &[u8], now: Tick) -> Vec<PeerAction> {
        let peer = match self.peers.get_mut(&token) {
            Some(p) => p,
            None => return vec![],
        };

        if data.len() > 0 {
            peer.last_activity = now;
        }

        peer.incoming.extend_from_slice(data);

        match peer.state {
            PeerState::Handshaking => match try_parse_handshake(peer, &self.info_hash) {
                Ok(completed) => {
                    if completed {
                        return vec![PeerAction::HandshakeSuccess { token }];
                    }
                    return vec![];
                }
                Err(e) => {
                    eprintln!("{} handshake failed: {}", peer.addr, e);
                    return vec![PeerAction::Close { token }];
                }
            },
            PeerState::Active => parse_messages(peer, self.total_pieces, now),
        }
    }

    pub fn on_connected(&mut self, addr: SocketAddr, token: Token, now: Tick) -> Vec<PeerAction> {
        let mut peer = Peer::new(addr, token);

        peer.start_handshake(now);
        peer.last_activity = now;

        peer.outgoing.push_back(self.build_handshake());
        self.peers.insert(token, peer);

        vec![
            PeerAction::PeerConnected { token },
            PeerAction::Write { token },
        ]
    }

    pub fn push_write(&mut self, token: &Token, data: Bytes, now: Tick) -> Vec<PeerAction> {
        let peer = match self.peers.get_mut(token) {
            Some(p) => p,
            None => return vec![],
        };
        let mut actions = Vec::new();

        push_write_peer(peer, data, &mut actions, now);

        actions
    }

    pub fn on_close(&mut self, token: &Token) -> bool {
        self.peers.remove(token).is_some()
    }

    pub fn collect_outgoing(&mut self, token: &Token) -> Vec<Bytes> {
        let peer = match self.peers.get_mut(token) {
            Some(p) => p,
            None => return vec![],
        };
        if peer.outgoing.is_empty() {
            return vec![];
        }

        let mut data = Vec::with_capacity(peer.outgoing.len());
        while let Some(msg) = peer.outgoing.pop_front() {
            data.push(msg);
        }

        data
    }

    pub fn on_update_interest(
        &mut self,
        token: Token,
        interested: bool,
        now: Tick,
    ) -> Vec<PeerAction> {
        let peer = match self.peers.get_mut(&token) {
            Some(p) => p,
            None => return vec![],
        };
        let mut actions = Vec::new();
        let was_interested = peer.interested;
        peer.interested = interested;
        if was_interested != peer.interested {
            let msg = if peer.interested {
                PeerMessage::Interested
            } else {
                PeerMessage::NotInterested
            };
            push_write_peer_msg(peer, &msg, &mut actions, now);
            if peer.can_request_blocks() {
                actions.push(PeerAction::PickBlocks {
                    token,
                    count: peer.get_request_block_count(),
                });
            }
        }

        actions
    }

    pub fn get_bitfield(&self, token: &Token) -> Option<Bitfield> {
        let peer = match self.peers.get(token) {
            Some(p) => p,
            None => return None,
        };

        peer.bitfield.clone()
    }

    pub fn enqueue_messages(
        &mut self,
        token: Token,
        messages: &[PeerMessage],
        now: Tick,
    ) -> Vec<PeerAction> {
        let mut actions = Vec::new();
        let peer = match self.peers.get_mut(&token) {
            Some(p) => p,
            None => return actions,
        };
        for msg in messages {
            push_write_peer_msg(peer, msg, &mut actions, now);
        }

        actions
    }
}

fn try_parse_handshake(peer: &mut Peer, info_hash: &[u8; 20]) -> Result<bool, io::Error> {
    if peer.incoming.len() < 68 {
        return Ok(false);
    }

    let pstrlen = peer.incoming[0] as usize;
    let total_len = 1 + pstrlen + 8 + 20 + 20;
    if peer.incoming.len() < total_len {
        return Ok(false);
    }

    let mut handshake = peer.incoming.split_to(total_len);

    let pstrlen = handshake.get_u8();
    if pstrlen != 19 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid pstr length {}", pstrlen),
        ));
    }

    let mut protocol = vec![0u8; pstrlen as usize];
    handshake.copy_to_slice(&mut protocol);

    if &protocol != b"BitTorrent protocol" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid protocol {:?}", protocol),
        ));
    }

    // reserved bytes
    handshake.advance(8);

    let mut received_info_hash = [0u8; 20];
    handshake.copy_to_slice(&mut received_info_hash);

    if &received_info_hash != info_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid info hash {:?}", received_info_hash),
        ));
    }

    let mut peer_id = [0u8; 20];
    handshake.copy_to_slice(&mut peer_id);

    peer.peer_id = Some(peer_id);
    peer.state = PeerState::Active;

    Ok(true)
}

fn parse_messages(peer: &mut Peer, total_pieces: usize, now: Tick) -> Vec<PeerAction> {
    const MAX_MESSAGE_LEN: usize = 1 << 20;
    let mut actions = Vec::new();
    loop {
        if peer.incoming.len() < 4 {
            break;
        }

        let mut header = peer.incoming.clone();
        let len = header.get_u32() as usize;

        if len > MAX_MESSAGE_LEN {
            eprintln!("{} invalid message length", peer.addr);
            return vec![PeerAction::Close { token: peer.token }];
        }

        if peer.incoming.len() < 4 + len {
            break;
        }

        peer.incoming.advance(4);

        if len == 0 {
            handle_message(peer, PeerMessage::KeepAlive, now, &mut actions);
            continue;
        }

        let payload = peer.incoming.split_to(len);

        match PeerMessage::parse(payload.freeze(), total_pieces) {
            Ok(msg) => {
                handle_message(peer, msg, now, &mut actions);
            }
            Err(e) => {
                eprintln!("{} invalid message: {}", peer.addr, e);
                actions.push(PeerAction::Close { token: peer.token });
                break;
            }
        }
    }

    actions
}

fn handle_message(peer: &mut Peer, msg: PeerMessage, now: Tick, actions: &mut Vec<PeerAction>) {
    match msg {
        PeerMessage::Choke => {
            peer.choked = true;
            peer.pending_requests.clear();
            peer.outgoing.clear();
            actions.push(PeerAction::Choked { token: peer.token });
        }
        PeerMessage::Unchoke => {
            peer.choked = false;
            if peer.can_request_blocks() {
                actions.push(PeerAction::PickBlocks {
                    token: peer.token,
                    count: peer.get_request_block_count(),
                });
            }
        }
        PeerMessage::Interested => {
            peer.peer_interested = true;
        }
        PeerMessage::NotInterested => {
            peer.peer_interested = false;
        }
        PeerMessage::Have(piece) => {
            if let Some(bitfield) = &mut peer.bitfield {
                bitfield.set(&(piece as usize), true);
                actions.push(PeerAction::PeerHave {
                    token: peer.token,
                    piece,
                });
            }
        }
        PeerMessage::Bitfield(bitfield) => {
            if let Some(bf) = &mut peer.bitfield {
                bf.merge_safe(&bitfield);
            } else {
                peer.bitfield = Some(bitfield.clone());
            }
            actions.push(PeerAction::PeerBitfield {
                token: peer.token,
                bitfield: bitfield.clone(),
            });
        }
        PeerMessage::Request(_) => todo!(),
        PeerMessage::Piece((piece, offset, data)) => {
            if let Some(sent_at) = peer.pending_requests.remove(&(piece, offset)) {
                let rtt = (now - sent_at).as_f64();

                peer.stats.rtt_ema = Some(match peer.stats.rtt_ema {
                    Some(old) => 0.2 * rtt + 0.8 * old,
                    None => rtt,
                });
            }

            let len = data.len() as u64;
            peer.stats.downloaded_bytes += len;

            actions.push(PeerAction::BlockReceived {
                token: peer.token,
                piece,
                offset,
                data,
            });
            if peer.can_request_blocks() {
                actions.push(PeerAction::PickBlocks {
                    token: peer.token,
                    count: peer.get_request_block_count(),
                });
            }
        }
        PeerMessage::Cancel(_) => todo!(),
        PeerMessage::Port(_) => todo!(),
        PeerMessage::KeepAlive => {}
    }
}

fn push_write_peer_msg(
    peer: &mut Peer,
    msg: &PeerMessage,
    actions: &mut Vec<PeerAction>,
    now: Tick,
) {
    match msg {
        PeerMessage::Request((piece, offset, _)) => {
            peer.pending_requests.insert((*piece, *offset), now);
        }
        PeerMessage::Choke => {
            peer.peer_choked = true;
        }
        PeerMessage::Unchoke => {
            peer.peer_choked = false;
        }
        _ => {}
    }
    let data = msg.encode();
    push_write_peer(peer, data, actions, now);
}

fn push_write_peer(peer: &mut Peer, data: Bytes, actions: &mut Vec<PeerAction>, now: Tick) {
    peer.stats.uploaded_bytes += data.len() as u64;
    peer.outgoing.push_back(data);
    peer.last_outbound = now;
    actions.push(PeerAction::Write { token: peer.token });
}

fn update_rate(bytes: u64, dt_ticks: Tick, rate: &mut f64) {
    let dt = dt_ticks.as_secs_f64();
    if dt > 0.0 {
        let inst = bytes as f64 / dt;
        *rate = RATE_ALPHA * inst + (1.0 - RATE_ALPHA) * *rate;
    }
}

fn update_peer_rate(now: Tick, peer: &mut Peer) {
    let dt = now - peer.stats.last_rate_tick;
    if dt >= duration_to_ticks(Duration::from_secs(1)) {
        update_rate(
            peer.stats.downloaded_bytes,
            dt,
            &mut peer.stats.download_rate,
        );
        peer.stats.downloaded_bytes = 0;

        update_rate(peer.stats.uploaded_bytes, dt, &mut peer.stats.upload_rate);
        peer.stats.uploaded_bytes = 0;

        peer.stats.last_rate_tick = now;
    }
}

fn peer_score(peer: &Peer) -> f64 {
    let bw = peer.stats.download_rate;
    let rtt = peer.stats.rtt_ema.unwrap_or(1000.0);

    bw / rtt.max(1.0)
}
