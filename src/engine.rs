use std::{
    collections::VecDeque,
    fmt,
    net::SocketAddr,
    ops::{Add, AddAssign, Rem, RemAssign, Sub, SubAssign},
    time::{Duration, Instant},
};

use bytes::Bytes;
use crossbeam::channel::{Receiver, RecvTimeoutError, Sender};
use mio::Token;
use rand::{Rng, distr::Alphanumeric};

use crate::{
    announce::{AnnounceEvent, AnnounceManager, TrackerResponse},
    bitfield::Bitfield,
    disk::{DiskError, DiskJob, DiskManager},
    net::{NetEvent, NetManager},
    picker::{PiecePicker, PieceReceiveStatus},
    torrent::Torrent,
};

#[derive(Debug, Clone)]
pub struct EngineStats {
    pub complete_pieces: usize,
    pub total_pieces: usize,
    pub peers: usize,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub left: u64,
    pub inflight_blocks: usize,
    pub files: Vec<(String, u64, u64, u64)>,
}

impl EngineStats {
    pub fn new(total_size: u64, total_pieces: usize, files: Vec<(String, u64, u64, u64)>) -> Self {
        Self {
            complete_pieces: 0,
            total_pieces,
            peers: 0,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            left: total_size,
            inflight_blocks: 0,
            files,
        }
    }
}

pub const TICK_DURATION: Duration = Duration::from_millis(500);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tick(pub u64);

impl Tick {
    pub fn as_f64(&self) -> f64 {
        self.0 as f64
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.0 as f64 * TICK_DURATION.as_secs_f64()
    }
}

impl AddAssign for Tick {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl Add for Tick {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl SubAssign for Tick {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0
    }
}

impl Sub for Tick {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Rem for Tick {
    type Output = Tick;

    fn rem(self, rhs: Self) -> Self::Output {
        Tick(self.0 % rhs.0)
    }
}

impl RemAssign for Tick {
    fn rem_assign(&mut self, rhs: Self) {
        self.0 %= rhs.0
    }
}

impl fmt::Display for Tick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

const FLUSH_INTERVAL: Tick = duration_to_ticks(Duration::from_secs(60));
// const PIECE_BLOCK_TIMEOUT: Tick = duration_to_ticks(Duration::from_secs(10));

#[derive(Debug)]
pub enum EngineEvent {
    Tick {
        current_tick: Tick,
    },
    PeerConnected {
        addr: SocketAddr,
    },
    PeerDisconnected {
        addr: SocketAddr,
        token: Token,
        bitfield: Option<Bitfield>,
    },
    AnnounceResponse {
        response: TrackerResponse,
    },
    PeerHandshakeSuccess {
        addr: SocketAddr,
    },
    PeerBitfield {
        addr: SocketAddr,
        bitfield: Bitfield,
    },
    PeerHave {
        addr: SocketAddr,
        piece: u32,
    },
    PeerCount {
        count: usize,
    },
    PickBlocks {
        token: Token,
        bitfield: Bitfield,
        count: usize,
    },
    PieceBlock {
        piece: u32,
        offset: u32,
        data: Bytes,
    },
    BlockWritten {
        result: Result<(), DiskError>,
    },
    PieceVerification {
        result: Result<(u32, bool), DiskError>,
    },
    ReadBlock {
        result: Result<(u32, u32, Bytes), DiskError>,
    },
    TimeoutBlocks {
        blocks: Vec<(u32, u32)>,
    },
    VerifyPiece {
        piece: u32,
        hash: [u8; 20],
    },
    Complete,
    PeerChoked {
        token: Token,
    },
    RequestMissingBlocks {
        token: Token,
        bitfield: Bitfield,
    },
}

#[derive(Debug)]
pub struct Engine {
    disk_mgr: DiskManager,
    disk_tx: Option<Sender<DiskJob>>,
    disk_rx: Receiver<DiskJob>,
    net_mgr: NetManager,
    net_tx: Option<Sender<NetEvent>>,
    net_rx: Receiver<NetEvent>,
    announce_mgr: AnnounceManager,
    announce_tx: Option<Sender<AnnounceEvent>>,
    announce_rx: Receiver<AnnounceEvent>,
    engine_tx: Option<Sender<EngineEvent>>,
    engine_rx: Receiver<EngineEvent>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    total_size: u64,
    stats: EngineStats,
    piece_picker: PiecePicker,
    piece_hashes: Vec<[u8; 20]>,
    max_read_bytes_per_sec: usize,
    max_write_bytes_per_sec: usize,
    complete: bool,
    verify_queue: VecDeque<(u32, [u8; 20])>,
    verify_inflight: bool,
}

impl Engine {
    pub fn new(
        torrent: Torrent,
        max_read_bytes_per_sec: usize,
        max_write_bytes_per_sec: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let disk_mgr = DiskManager::new(&torrent)?;
        let (disk_tx, disk_rx) = crossbeam::channel::unbounded();

        let net_mgr: NetManager = NetManager::new();
        let (net_tx, net_rx) = crossbeam::channel::unbounded();

        let announce_mgr = AnnounceManager::new(&torrent.announce, &torrent.announce_list);
        let (announce_tx, announce_rx) = crossbeam::channel::unbounded();

        let total_pieces = torrent.info.piece_hashes.len();
        let total_size = torrent.info.total_size;
        let piece_length = torrent.info.piece_length;
        let piece_picker = PiecePicker::new(total_pieces, total_size, piece_length);

        let (engine_tx, engine_rx) = crossbeam::channel::unbounded();

        let info_hash = torrent.info_hash;
        let peer_id = generate_peer_id();
        let piece_hashes = torrent.info.piece_hashes.clone();
        let file_progress = disk_mgr.file_progress();
        Ok(Self {
            disk_mgr,
            disk_tx: Some(disk_tx),
            disk_rx,
            net_mgr,
            net_tx: Some(net_tx),
            net_rx,
            announce_mgr,
            announce_tx: Some(announce_tx),
            announce_rx,
            engine_tx: Some(engine_tx),
            engine_rx,
            info_hash,
            peer_id,
            total_size,
            stats: EngineStats::new(total_size, total_pieces, file_progress),
            piece_picker,
            piece_hashes,
            max_read_bytes_per_sec,
            max_write_bytes_per_sec,
            complete: false,
            verify_queue: VecDeque::new(),
            verify_inflight: false,
        })
    }

    pub fn get_stats(&self) -> EngineStats {
        self.stats.clone()
    }

    pub fn get_rx(&self) -> Receiver<EngineEvent> {
        self.engine_rx.clone()
    }

    fn handle_event(&mut self, event: EngineEvent, now: Tick) {
        match event {
            EngineEvent::PeerConnected { addr } => {
                log::info!("{} connected", addr);
            }
            EngineEvent::PeerDisconnected {
                addr,
                token,
                bitfield,
            } => {
                log::info!("{} disconnected", addr);
                if let Some(bitfield) = bitfield {
                    self.piece_picker.remove_peer_bitfield(&bitfield);
                }
                let _ = self.piece_picker.requeue_blocks_for_peer(token);
            }
            EngineEvent::AnnounceResponse { response } => {
                log::debug!("tracker response, {} peers", response.peers.len());
                self.handle_announce_response(response);
            }
            EngineEvent::Tick { current_tick } => {
                self.handle_tick(current_tick);
            }
            EngineEvent::PeerHandshakeSuccess { addr } => {
                log::debug!("{} handshake success", addr);
            }
            EngineEvent::PeerBitfield { addr, bitfield } => {
                self.piece_picker.register_peer_bitfield(&bitfield);
                let interested = bitfield.is_interesting_to(self.piece_picker.bitfield());
                self.notify_interested(addr, interested);
            }
            EngineEvent::PeerCount { count } => {
                self.stats.peers = count;
            }
            EngineEvent::PeerHave { addr, piece } => {
                self.piece_picker.register_peer_have(piece);
                let piece = piece as usize;
                let has_piece = self.piece_picker.bitfield().get(&piece);
                self.notify_interested(addr, !has_piece);
            }
            EngineEvent::PickBlocks {
                token,
                bitfield,
                count,
            } => {
                if self.complete {
                    return;
                }
                let tx = match &self.net_tx {
                    Some(t) => t,
                    None => return,
                };
                let requests = self.piece_picker.pick_blocks(token, &bitfield, now, count);
                if !requests.is_empty() {
                    let _ = tx.send(NetEvent::SendPeerMessages {
                        token,
                        messages: requests,
                    });
                }
            }
            EngineEvent::PieceBlock {
                piece,
                offset,
                data,
            } => {
                let disk_tx = match self.disk_tx.clone() {
                    Some(d) => d,
                    None => return,
                };
                let engine_tx = match self.engine_tx.clone() {
                    Some(e) => e,
                    None => return,
                };
                self.on_piece_block(piece, offset, data, &disk_tx, &engine_tx);
            }
            EngineEvent::BlockWritten { result } => {
                if let Err(e) = result {
                    log::error!("failed to write block(s) to disk: {:?}", e);
                }
            }
            EngineEvent::PieceVerification { result } => {
                self.on_piece_verified(result);
            }
            EngineEvent::VerifyPiece { piece, hash } => {
                self.on_verify_ready(piece, hash);
            }
            EngineEvent::ReadBlock { result: _ } => todo!(),
            EngineEvent::TimeoutBlocks { blocks } => {
                self.piece_picker.requeue_blocks(blocks);
            }
            EngineEvent::Complete => {
                self.stop();
            }
            EngineEvent::PeerChoked { token } => {
                let _ = self.piece_picker.requeue_blocks_for_peer(token);
            }
            EngineEvent::RequestMissingBlocks { token, bitfield } => {
                if self.complete {
                    return;
                }
                let tx = match &self.net_tx {
                    Some(t) => t,
                    None => return,
                };
                let missing_blocks = self.piece_picker.missing_blocks_for_peer(&bitfield);
                let _ = tx.send(NetEvent::RequestMissingBlocks {
                    token,
                    missing_blocks,
                });
            }
        }
    }

    pub fn start<F>(&mut self, mut on_tick_callback: F)
    where
        F: FnMut(Tick, EngineStats),
    {
        self.disk_mgr.start(2, &self.disk_rx);
        if let Some(tx) = &self.engine_tx {
            self.net_mgr.start(
                &self.net_rx,
                self.piece_picker.bitfield().len(),
                self.info_hash,
                self.peer_id,
                tx,
                self.max_read_bytes_per_sec,
                self.max_write_bytes_per_sec,
            );
        }
        self.announce_mgr.start(&self.announce_rx);
        self.begin_loop(&mut on_tick_callback);
    }

    pub fn stop(&mut self) {
        if let Some(tx) = self.disk_tx.take() {
            let _ = tx.send(DiskJob::Flush);
        }
        if let Some(tx) = self.net_tx.take() {
            let _ = tx.send(NetEvent::Shutdown);
        }
        self.announce_tx.take();
        self.engine_tx.take();
    }

    pub fn join(&mut self) {
        self.disk_mgr.join();
        self.net_mgr.join();
        self.announce_mgr.join();
    }

    fn begin_loop<F>(&mut self, on_tick_callback: &mut F)
    where
        F: FnMut(Tick, EngineStats),
    {
        let mut tick = Tick(0);
        let mut next_tick_deadline = Instant::now() + TICK_DURATION;
        loop {
            let now = Instant::now();
            let timeout = if next_tick_deadline > now {
                next_tick_deadline - now
            } else {
                Duration::from_secs(0)
            };
            match self.engine_rx.recv_timeout(timeout) {
                Ok(ev) => {
                    self.handle_event(ev, tick);
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.handle_event(EngineEvent::Tick { current_tick: tick }, tick);
                    on_tick_callback(tick, self.get_stats());
                    tick += Tick(1);
                    next_tick_deadline += TICK_DURATION;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn notify_interested(&self, addr: SocketAddr, interested: bool) {
        if let Some(tx) = &self.net_tx {
            let _ = tx.send(NetEvent::UpdatePeerInterest { addr, interested });
        }
    }

    fn handle_tick(&mut self, tick: Tick) {
        self.check_announce();
        self.update_stats();
        let endgame = self.piece_picker.maybe_enter_endgame();
        /*
        if tick % PIECE_BLOCK_TIMEOUT == Tick(0) {
            self.piece_picker
                .requeue_timed_out_blocks(tick, PIECE_BLOCK_TIMEOUT);
        }
        */
        if let Some(tx) = &self.net_tx {
            let _ = tx.send(NetEvent::Tick { tick, endgame });
        }
        if tick % FLUSH_INTERVAL == Tick(0) {
            if let Some(tx) = &self.disk_tx {
                let _ = tx.send(DiskJob::Flush);
            }
        }
        self.check_complete();
    }

    fn check_complete(&mut self) {
        if self.complete {
            return;
        }
        if !self.piece_picker.bitfield().has_any_zero() {
            self.complete = true;
            if let Some(tx) = &self.engine_tx {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_secs(2));
                    let _ = tx.send(EngineEvent::Complete);
                });
            }
        }
    }

    fn update_stats(&mut self) {
        self.stats.complete_pieces = self.piece_picker.bitfield().count_ones();
        self.stats.downloaded_bytes = self.piece_picker.opportunistic_downloaded_bytes();
        if self.stats.downloaded_bytes > self.total_size {
            self.stats.downloaded_bytes = self.total_size;
        }
        self.stats.left = self.total_size.saturating_sub(self.stats.downloaded_bytes);
        self.stats.inflight_blocks = self.piece_picker.inflight_blocks();
        self.stats.files = self.disk_mgr.file_progress();
    }

    fn check_announce(&mut self) {
        let engine_tx = match &self.engine_tx {
            Some(e) => e,
            None => return,
        };
        let tx = match &self.announce_tx {
            Some(t) => t,
            None => return,
        };
        let tracker = match self.announce_mgr.next_due_tracker() {
            Some(t) => t,
            None => return,
        };
        tracker.announced();
        let _ = tx.send(AnnounceEvent::Announce {
            url: tracker.url.clone(),
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            uploaded: 0,
            downloaded: 0,
            left: self.total_size,
            respond_to: engine_tx.clone(),
        });
    }

    fn handle_announce_response(&self, response: TrackerResponse) {
        let net_tx = match &self.net_tx {
            Some(n) => n,
            None => return,
        };
        let _ = net_tx.send(NetEvent::NewPeers {
            addrs: response.peers,
        });
    }

    fn on_verify_ready(&mut self, piece: u32, hash: [u8; 20]) {
        self.verify_queue.push_back((piece, hash));
        self.try_start_verify();
    }

    fn try_start_verify(&mut self) {
        if self.verify_inflight {
            return;
        }

        if let Some((piece, hash)) = self.verify_queue.pop_front() {
            let disk_tx = match &self.disk_tx {
                Some(d) => d,
                None => return,
            };
            let engine_tx = match &self.engine_tx {
                Some(e) => e,
                None => return,
            };
            let _ = disk_tx.send(DiskJob::VerifyPiece {
                piece,
                expected_hash: hash,
                respond_to: engine_tx.clone(),
            });
            self.verify_inflight = true;
        }
    }

    fn on_piece_verified(&mut self, result: Result<(u32, bool), DiskError>) {
        self.verify_inflight = false;

        match result {
            Ok((piece, verified)) => {
                self.piece_picker.on_piece_verified(piece, verified);
                if verified {
                    log::debug!("piece {} verified", piece);
                } else {
                    log::warn!("piece {} failed verification", piece);
                }
            }
            Err(e) => {
                log::error!("failed to verify piece from disk: {:?}", e);
            }
        }

        self.try_start_verify();
    }

    fn on_piece_block(
        &mut self,
        piece: u32,
        offset: u32,
        data: Bytes,
        disk_tx: &Sender<DiskJob>,
        engine_tx: &Sender<EngineEvent>,
    ) {
        let length = data.len();
        let status = self.piece_picker.on_block_received(piece, offset);
        if status.contains(PieceReceiveStatus::BLOCK_RECEIVED) {
            self.disk_mgr.inner.start_write(piece);
            if status.contains(PieceReceiveStatus::PIECE_COMPLETE) {
                let hash = self.piece_hashes[piece as usize];
                let ready_for_verify = self.disk_mgr.inner.queue_verify(piece, hash);
                if ready_for_verify {
                    self.on_verify_ready(piece, hash);
                }
            }
            let _ = disk_tx.send(DiskJob::WriteBlock {
                piece,
                offset,
                data,
                respond_to: engine_tx.clone(),
            });
            if self.piece_picker.is_endgame() {
                if let Some(net_tx) = &self.net_tx {
                    let _ = net_tx.send(NetEvent::CancelBlock {
                        piece,
                        offset,
                        length: length as u32,
                    });
                }
            }
        }
    }
}

fn generate_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];

    let prefix = b"-RT1000-";
    peer_id[..prefix.len()].copy_from_slice(prefix);

    let mut rng = rand::rng();
    for byte in peer_id[prefix.len()..].iter_mut() {
        let c = rng.sample(Alphanumeric) as u8;
        *byte = c;
    }

    peer_id
}

pub const fn duration_to_ticks(duration: Duration) -> Tick {
    Tick((duration.as_millis() / TICK_DURATION.as_millis()) as u64)
}
