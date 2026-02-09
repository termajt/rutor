use std::{
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
        mpsc::{Receiver, Sender},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crate::{
    announce::{self, AnnounceManager},
    bitfield::Bitfield,
    connection::ConnectionManager,
    disk::DiskManager,
    peer::PeerMessage,
    picker::{self, PiecePicker},
    torrent::Torrent,
};

use rand::{Rng, distr::Alphanumeric};
use threadpool::ThreadPool;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
pub enum Event {
    Tick,
    Complete,
    Stop,
    CompareBitfield {
        addr: SocketAddr,
        bitfield: Bitfield,
    },
    DiskStats {
        files: Vec<(PathBuf, u64, u64)>,
    },
    PieceVerified {
        piece: usize,
    },
    PieceVerificationFailed {
        piece: usize,
    },
    PeerIoStats {
        connected_peers: usize,
        max_peers: usize,
        blocks_inflight: usize,
        available_peers: usize,
        total_speed_down: f64,
        opportunistic_downloaded: u64,
    },
}

#[derive(Debug)]
pub enum IoTask {
    WriteToDisk {
        piece: usize,
        offset: usize,
        data: Vec<u8>,
    },
    DiskVerifyPiece {
        piece: usize,
        expected_hash: [u8; 20],
    },
    Stop,
    CalculateFileStats {
        bitfield: Bitfield,
    },
}

#[derive(Debug)]
pub enum PeerIoTask {
    NewPeers {
        peers: Vec<SocketAddr>,
    },
    ConnectPeer {
        addr: SocketAddr,
    },
    PeerConnected {
        addr: SocketAddr,
        stream: TcpStream,
    },
    PeerConnectFailed {
        addr: SocketAddr,
        reason: String,
    },
    MaybeConnectPeers,
    PeerDisconnected {
        addr: SocketAddr,
        reason: String,
    },
    Stop,
    MaybeUpdateInterest {
        addr: SocketAddr,
        bitfield_interested: bool,
    },
    SendMessage {
        addr: SocketAddr,
        msg: PeerMessage,
    },
    RequestBlocksForPeer {
        addr: SocketAddr,
    },
    MaybeRequestBlocks,
    PeriodicReap,
    PieceVerified {
        piece: usize,
    },
    PieceVerificationFailed {
        piece: usize,
    },
    StatsUpdate,
}

#[derive(Debug)]
pub enum AnnounceIoTask {
    PerformAnnounce {
        url: String,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        uploaded: u64,
        downloaded: u64,
        left: u64,
    },
    Stop,
}

#[derive(Debug, Clone)]
pub struct TorrentStats {
    pub downloaded: u64,
    pub uploaded: u64,
    pub left: u64,
    pub files: Vec<(PathBuf, u64, u64)>,
    pub peers: usize,
    pub bitfield: Bitfield,
    pub max_peers: usize,
    pub blocks_inflight: usize,
    pub available_peers: usize,
    pub total_speed_down: f64,
}

impl TorrentStats {
    fn new(total_size: u64, files: Vec<(PathBuf, u64, u64)>, bitfield: Bitfield) -> Self {
        Self {
            downloaded: 0,
            uploaded: 0,
            left: total_size,
            files,
            peers: 0,
            bitfield,
            max_peers: 0,
            blocks_inflight: 0,
            available_peers: 0,
            total_speed_down: 0.0,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum ShutdownPhase {
    Running = 0,
    Completed = 1,
    Draining = 2,
    Terminated = 3,
}

#[derive(Debug)]
pub struct ShutdownState {
    phase: AtomicU8,
}

impl ShutdownState {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(ShutdownPhase::Running as u8),
        }
    }

    fn phase(&self) -> ShutdownPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => ShutdownPhase::Running,
            1 => ShutdownPhase::Completed,
            2 => ShutdownPhase::Draining,
            3 => ShutdownPhase::Terminated,
            _ => unreachable!("ShutdownPhase"),
        }
    }

    pub fn is_running(&self) -> bool {
        self.phase() == ShutdownPhase::Running
    }

    pub fn is_completed(&self) -> bool {
        self.phase() == ShutdownPhase::Completed
    }

    pub fn is_draining(&self) -> bool {
        self.phase() == ShutdownPhase::Draining
    }

    pub fn is_terminated(&self) -> bool {
        self.phase() == ShutdownPhase::Terminated
    }

    fn mark_completed(&self) -> bool {
        self.phase
            .compare_exchange(
                ShutdownPhase::Running as u8,
                ShutdownPhase::Completed as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn begin_draining(&self) -> bool {
        self.phase
            .compare_exchange(
                ShutdownPhase::Completed as u8,
                ShutdownPhase::Draining as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn terminate(&self) -> bool {
        self.phase
            .compare_exchange(
                ShutdownPhase::Draining as u8,
                ShutdownPhase::Terminated as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

#[derive(Debug)]
pub struct Engine {
    announce_mgr: AnnounceManager,
    event_tx: Sender<Event>,
    event_rx: Receiver<Event>,
    io_tx: Sender<IoTask>,
    peer_io_tx: Sender<PeerIoTask>,
    announce_io_tx: Sender<AnnounceIoTask>,
    shutdown_state: Arc<ShutdownState>,
    torrent: Torrent,
    peer_id: [u8; 20],
    stats: TorrentStats,
    last_peers_adjust: Option<Instant>,
    read_limit_bytes_per_sec: usize,
    write_limit_bytes_per_sec: usize,
    join_handles: Vec<JoinHandle<()>>,
}

impl Engine {
    pub fn new(
        torrent: Torrent,
        event_tx: Sender<Event>,
        event_rx: Receiver<Event>,
        io_tx: Sender<IoTask>,
        peer_io_tx: Sender<PeerIoTask>,
        announce_io_tx: Sender<AnnounceIoTask>,
        read_limit_bytes_per_sec: usize,
        write_limit_bytes_per_sec: usize,
    ) -> Self {
        let announce_mgr = AnnounceManager::new(&torrent);
        let bitfield = Bitfield::new(torrent.info.piece_hashes.len());
        let stats = TorrentStats::new(torrent.info.total_size, Vec::new(), bitfield);
        Self {
            announce_mgr,
            event_tx,
            event_rx,
            io_tx,
            peer_io_tx,
            announce_io_tx,
            shutdown_state: Arc::new(ShutdownState::new()),
            torrent,
            peer_id: generate_peer_id(),
            stats,
            last_peers_adjust: None,
            read_limit_bytes_per_sec,
            write_limit_bytes_per_sec,
            join_handles: Vec::new(),
        }
    }

    fn check_completion(&mut self) {
        if self.shutdown_state.is_completed() || self.shutdown_state.is_draining() {
            return;
        }

        if !self.stats.bitfield.has_any_zero() {
            self.stats.downloaded = self.torrent.info.total_size;
            self.stats.left = 0;

            if self.shutdown_state.mark_completed() {
                let _ = self.event_tx.send(Event::Complete);

                let event_tx = self.event_tx.clone();
                let shutdown_state = self.shutdown_state.clone();
                let handle = std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_secs(2));
                    if shutdown_state.is_completed() {
                        let _ = event_tx.send(Event::Stop);
                    }
                });
                self.join_handles.push(handle);
            }
        }
    }

    pub fn join(&mut self) {
        if self.shutdown_state.terminate() {
            eprintln!("terminating");
            while let Some(handle) = self.join_handles.pop() {
                if let Err(_) = handle.join() {
                    eprintln!("failed to join thread");
                }
            }
        }
    }

    pub fn poll(&self) -> Result<Event> {
        let ev = self.event_rx.recv()?;
        Ok(ev)
    }

    pub fn handle_event(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Tick => {
                self.on_tick();
            }
            Event::Complete => {}
            Event::Stop => {
                self.stop();
            }
            Event::CompareBitfield { addr, bitfield } => {
                let interested = bitfield.is_interesting_to(&self.stats.bitfield);
                let _ = self.peer_io_tx.send(PeerIoTask::MaybeUpdateInterest {
                    addr,
                    bitfield_interested: interested,
                });
            }
            Event::DiskStats { files } => {
                self.stats.files = files;
                self.check_completion();
            }
            Event::PieceVerified { piece } => {
                self.stats.bitfield.set(&piece, true);
                self.check_completion();
            }
            Event::PieceVerificationFailed { piece } => {
                self.stats.bitfield.set(&piece, false);
            }
            Event::PeerIoStats {
                connected_peers,
                max_peers,
                blocks_inflight,
                available_peers,
                total_speed_down,
                opportunistic_downloaded,
            } => {
                self.stats.peers = connected_peers;
                self.stats.max_peers = max_peers;
                self.stats.blocks_inflight = blocks_inflight;
                self.stats.available_peers = available_peers;
                self.stats.total_speed_down = total_speed_down;
                self.stats.downloaded = opportunistic_downloaded;
                self.stats.left = self
                    .torrent
                    .info
                    .total_size
                    .saturating_sub(opportunistic_downloaded);
            }
        }
        Ok(())
    }

    pub fn get_stats(&self) -> TorrentStats {
        self.stats.clone()
    }

    pub fn start(
        &mut self,
        io_rx: Receiver<IoTask>,
        peer_io_rx: Receiver<PeerIoTask>,
        announce_io_rx: Receiver<AnnounceIoTask>,
    ) {
        self.join_handles.push(self.start_disk_thread(io_rx));
        self.join_handles.push(self.start_tick_thread());
        self.join_handles
            .push(self.start_peer_io_thread(peer_io_rx));
        self.join_handles
            .push(self.start_announce_io_thread(announce_io_rx));
    }

    pub fn stop(&self) {
        if self.shutdown_state.begin_draining() {
            let _ = self.io_tx.send(IoTask::Stop);
            let _ = self.peer_io_tx.send(PeerIoTask::Stop);
            let _ = self.announce_io_tx.send(AnnounceIoTask::Stop);
        }
    }

    fn start_disk_thread(&self, io_rx: Receiver<IoTask>) -> JoinHandle<()> {
        let total_pieces = self.torrent.info.piece_hashes.len();
        let total_size = self.torrent.info.total_size;
        let piece_length = self.torrent.info.piece_length as usize;
        let files = self.torrent.info.files.clone();
        let event_tx = self.event_tx.clone();
        let peer_io_tx = self.peer_io_tx.clone();
        let shutdown_state = self.shutdown_state.clone();
        std::thread::spawn(move || {
            let mut disk_mgr = DiskManager::new(total_pieces, total_size, piece_length, files);
            let verify_pool = ThreadPool::with_name("piece_verify_pool".to_string(), 5);

            loop {
                if shutdown_state.is_draining() || shutdown_state.is_terminated() {
                    break;
                }
                match io_rx.recv() {
                    Ok(ev) => match ev {
                        IoTask::WriteToDisk {
                            piece,
                            offset,
                            data,
                        } => {
                            if let Err(e) = disk_mgr.write_to_disk(piece, offset, &data) {
                                eprintln!(
                                    "failed to write piece data to disk, index: {}, offset: {}, bytes: {} :: {}",
                                    piece,
                                    offset,
                                    data.len(),
                                    e
                                );
                                let _ = event_tx.send(Event::Stop);
                                continue;
                            }
                        }
                        IoTask::DiskVerifyPiece {
                            piece,
                            expected_hash,
                        } => {
                            if let Err(e) = disk_mgr.flush_piece(piece) {
                                eprintln!("flush failed before verify: {}", e);
                                let _ = event_tx.send(Event::Stop);
                                continue;
                            }
                            match disk_mgr.read_piece(piece) {
                                Ok(data) => {
                                    let event_tx = event_tx.clone();
                                    let peer_io_tx = peer_io_tx.clone();
                                    verify_pool.execute(move || {
                                        if picker::verify_piece(expected_hash, &data) {
                                            let _ = event_tx.send(Event::PieceVerified { piece });
                                            let _ = peer_io_tx
                                                .send(PeerIoTask::PieceVerified { piece });
                                        } else {
                                            let _ = event_tx
                                                .send(Event::PieceVerificationFailed { piece });
                                            let _ = peer_io_tx.send(
                                                PeerIoTask::PieceVerificationFailed { piece },
                                            );
                                        }
                                    });
                                }
                                Err(e) => {
                                    eprintln!(
                                        "failed to read piece {} data from disk: {}",
                                        piece, e
                                    );
                                    let _ = event_tx.send(Event::Stop);
                                    continue;
                                }
                            }
                        }
                        IoTask::Stop => {
                            eprintln!("flushing disk manager...");
                            if let Err(e) = disk_mgr.flush_all() {
                                eprintln!("failed to flush disk manager: {}", e);
                            }
                            if let Err(e) = disk_mgr.close() {
                                eprintln!("failed to close disk manager: {}", e);
                            }
                            return;
                        }
                        IoTask::CalculateFileStats { bitfield } => {
                            let files: Vec<(PathBuf, u64, u64)> = disk_mgr
                                .files()
                                .iter()
                                .enumerate()
                                .map(|(i, f)| {
                                    let downloaded = disk_mgr.verified_bytes_for_file(i, &bitfield);
                                    (f.path.clone(), downloaded, f.length)
                                })
                                .collect();
                            let _ = event_tx.send(Event::DiskStats { files });
                        }
                    },
                    Err(e) => {
                        eprintln!("disk io rx error: {}", e);
                        break;
                    }
                }
            }

            verify_pool.join();
        })
    }

    fn start_tick_thread(&self) -> JoinHandle<()> {
        let shutdown_state = self.shutdown_state.clone();
        let event_tx = self.event_tx.clone();
        std::thread::spawn(move || {
            while !shutdown_state.is_terminated() {
                if shutdown_state.is_running() || shutdown_state.is_completed() {
                    let _ = event_tx.send(Event::Tick);
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        })
    }

    fn start_peer_io_thread(&self, peer_io_rx: Receiver<PeerIoTask>) -> JoinHandle<()> {
        let peer_io_tx = self.peer_io_tx.clone();
        let total_pieces = self.torrent.info.piece_hashes.len();
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let piece_length = self.torrent.info.piece_length;
        let event_tx = self.event_tx.clone();
        let total_size = self.torrent.info.total_size;
        let piece_hashes = self.torrent.info.piece_hashes.clone();
        let io_tx = self.io_tx.clone();
        let shutdown_state = self.shutdown_state.clone();
        let read_limit_bytes_per_sec = self.read_limit_bytes_per_sec;
        let write_limit_bytes_per_sec = self.write_limit_bytes_per_sec;
        std::thread::spawn(move || {
            let picker = match PiecePicker::new(piece_length as usize, total_size, piece_hashes, 8)
            {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("failed to create piece picker: {}", e);
                    let _ = event_tx.send(Event::Stop);
                    return;
                }
            };
            let mut connection_mgr = match ConnectionManager::new(
                peer_io_tx,
                event_tx.clone(),
                io_tx,
                picker,
                total_size,
                shutdown_state.clone(),
                info_hash,
                peer_id,
                total_pieces,
                read_limit_bytes_per_sec,
                write_limit_bytes_per_sec,
            ) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("failed to create connection manager: {}", e);
                    let _ = event_tx.send(Event::Stop);
                    return;
                }
            };
            loop {
                if shutdown_state.is_terminated() {
                    break;
                }

                if shutdown_state.is_running() || shutdown_state.is_completed() {
                    // Block waiting for control OR timeout
                    while let Ok(ev) = peer_io_rx.try_recv() {
                        connection_mgr.handle_event(ev);
                    }

                    match connection_mgr.poll_peers() {
                        Ok(addrs) if !addrs.is_empty() => {
                            connection_mgr.drain_peer_messages(addrs);
                        }
                        _ => {}
                    }
                } else if shutdown_state.is_draining() {
                    break;
                }

                std::thread::sleep(Duration::from_millis(50));
            }
            connection_mgr.join();
        })
    }

    fn start_announce_io_thread(&self, announce_io_rx: Receiver<AnnounceIoTask>) -> JoinHandle<()> {
        let peer_io_tx = self.peer_io_tx.clone();
        let shutdown_state = self.shutdown_state.clone();
        std::thread::spawn(move || {
            loop {
                if shutdown_state.is_draining() || shutdown_state.is_terminated() {
                    break;
                }
                if let Ok(ev) = announce_io_rx.recv() {
                    match ev {
                        AnnounceIoTask::Stop => return,
                        AnnounceIoTask::PerformAnnounce {
                            url,
                            info_hash,
                            peer_id,
                            uploaded,
                            downloaded,
                            left,
                        } => {
                            eprintln!("fetching tracker: {}", url);
                            match announce::announce(
                                &url, &info_hash, &peer_id, 6881, uploaded, downloaded, left, None,
                                None,
                            ) {
                                Ok(response) => {
                                    eprintln!(
                                        "fetched tracker: {} :: {} peers",
                                        url,
                                        response.peers.len()
                                    );
                                    if !response.peers.is_empty() {
                                        let _ = peer_io_tx.send(PeerIoTask::NewPeers {
                                            peers: response.peers,
                                        });
                                    }
                                }
                                Err(e) => {
                                    eprintln!("tracker error for {} :: {}", url, e.to_string());
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn on_tick(&mut self) {
        match self.shutdown_state.phase() {
            ShutdownPhase::Running => {
                self.check_announce();
                self.handle_disk_io_tick();
                self.handle_peer_io_tick();
            }
            ShutdownPhase::Completed => {
                self.handle_disk_io_tick();
                self.handle_peer_io_tick();
            }
            _ => {}
        }
    }

    fn handle_disk_io_tick(&self) {
        let _ = self.io_tx.send(IoTask::CalculateFileStats {
            bitfield: self.stats.bitfield.clone(),
        });
    }

    fn handle_peer_io_tick(&mut self) {
        let _ = self.peer_io_tx.send(PeerIoTask::StatsUpdate);
        let _ = self.peer_io_tx.send(PeerIoTask::PeriodicReap);
        let _ = self.peer_io_tx.send(PeerIoTask::MaybeRequestBlocks);
        if let Some(last_adjust) = self.last_peers_adjust {
            if last_adjust.elapsed() >= Duration::from_secs(5) {
                self.last_peers_adjust = Some(Instant::now());
                let _ = self.peer_io_tx.send(PeerIoTask::MaybeConnectPeers);
            }
        } else {
            self.last_peers_adjust = Some(Instant::now());
            let _ = self.peer_io_tx.send(PeerIoTask::MaybeConnectPeers);
        }
    }

    fn check_announce(&mut self) {
        if self.stats.available_peers == 0 {
            if let Some(tracker) = self.announce_mgr.next_due_tracker() {
                tracker.announced();
                let _ = self.announce_io_tx.send(AnnounceIoTask::PerformAnnounce {
                    url: tracker.url.clone(),
                    info_hash: self.torrent.info_hash,
                    peer_id: self.peer_id,
                    uploaded: 0,
                    downloaded: self.stats.downloaded,
                    left: self.stats.left,
                });
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
