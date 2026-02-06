use std::{
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
    time::{Duration, Instant},
};

use crate::{
    announce::{self, AnnounceManager},
    bitfield::Bitfield,
    connection::ConnectionManager,
    disk::DiskManager,
    event::ManualResetEvent,
    peer::PeerMessage,
    picker::{self, PiecePicker},
    torrent::Torrent,
};

use rand::{Rng, distr::Alphanumeric};
use threadpool::ThreadPool;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MIN_ENDGAME_THRESHOLD: f64 = 0.01;
const MAX_ENDGAME_THRESHOLD: f64 = 0.05;

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
        total_downloaded: u64,
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
    PeerMessage {
        addr: SocketAddr,
        msg: PeerMessage,
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
    PeriodicReap {
        should_enter_endgame: bool,
    },
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
}

impl TorrentStats {
    fn new(
        total_size: u64,
        files: Vec<(PathBuf, u64, u64)>,
        downloaded: u64,
        uploaded: u64,
        peers: usize,
        bitfield: Bitfield,
        max_peers: usize,
        blocks_inflight: usize,
        available_peers: usize,
    ) -> Self {
        let left = total_size.saturating_sub(downloaded);
        Self {
            downloaded,
            uploaded,
            left,
            files,
            peers,
            bitfield,
            max_peers,
            blocks_inflight,
            available_peers,
        }
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
    tpool: Arc<ThreadPool>,
    stop_signal: Arc<ManualResetEvent>,
    torrent: Torrent,
    peer_id: [u8; 20],
    stats: TorrentStats,
    completed: bool,
    endgame_threshold: f64,
    last_peers_adjust: Option<Instant>,
    endgame: bool,
}

impl Engine {
    pub fn new(
        torrent: Torrent,
        event_tx: Sender<Event>,
        event_rx: Receiver<Event>,
        io_tx: Sender<IoTask>,
        peer_io_tx: Sender<PeerIoTask>,
        announce_io_tx: Sender<AnnounceIoTask>,
        tpool: Arc<ThreadPool>,
    ) -> Self {
        let announce_mgr = AnnounceManager::new(&torrent);
        let bitfield = Bitfield::new(torrent.info.piece_hashes.len());
        let stats = TorrentStats::new(
            torrent.info.total_size,
            Vec::new(),
            0,
            0,
            0,
            bitfield,
            0,
            0,
            0,
        );
        let endgame_threshold = endgame_threshold(torrent.info.total_size);
        Self {
            announce_mgr,
            event_tx,
            event_rx,
            io_tx,
            peer_io_tx,
            announce_io_tx,
            tpool,
            stop_signal: Arc::new(ManualResetEvent::new(false)),
            torrent,
            peer_id: generate_peer_id(),
            stats,
            completed: false,
            endgame_threshold,
            last_peers_adjust: None,
            endgame: false,
        }
    }

    fn check_completion(&mut self) {
        if self.completed {
            return;
        }

        if self.stats.left == 0 && !self.stats.bitfield.has_any_zero() {
            self.completed = true;
            let _ = self.event_tx.send(Event::Complete);
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
            Event::Complete | Event::Stop => {
                self.stop();
            }
            Event::CompareBitfield { addr, bitfield } => {
                let interested = bitfield.is_interesting_to(&self.stats.bitfield);
                let _ = self.peer_io_tx.send(PeerIoTask::MaybeUpdateInterest {
                    addr,
                    bitfield_interested: interested,
                });
            }
            Event::DiskStats {
                total_downloaded,
                files,
            } => {
                self.stats.downloaded = total_downloaded;
                self.stats.left = self
                    .torrent
                    .info
                    .total_size
                    .saturating_sub(total_downloaded);
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
            } => {
                self.stats.peers = connected_peers;
                self.stats.max_peers = max_peers;
                self.stats.blocks_inflight = blocks_inflight;
                self.stats.available_peers = available_peers;
            }
        }
        Ok(())
    }

    pub fn get_stats(&self) -> TorrentStats {
        self.stats.clone()
    }

    pub fn start(
        &self,
        io_rx: Receiver<IoTask>,
        peer_io_rx: Receiver<PeerIoTask>,
        announce_io_rx: Receiver<AnnounceIoTask>,
    ) {
        self.start_disk_thread(io_rx, self.event_tx.clone(), self.peer_io_tx.clone());
        self.start_tick_thread();
        self.start_peer_io_thread(peer_io_rx);
        self.start_announce_io_thread(announce_io_rx);
    }

    pub fn stop(&self) {
        let _ = self.event_tx.send(Event::Stop);
        let _ = self.io_tx.send(IoTask::Stop);
        let _ = self.peer_io_tx.send(PeerIoTask::Stop);
        let _ = self.announce_io_tx.send(AnnounceIoTask::Stop);
        if !self.stop_signal.is_set() {
            self.stop_signal.set();
        }
    }

    fn start_disk_thread(
        &self,
        io_rx: Receiver<IoTask>,
        event_tx: Sender<Event>,
        peer_io_tx: Sender<PeerIoTask>,
    ) {
        let total_pieces = self.torrent.info.piece_hashes.len();
        let total_size = self.torrent.info.total_size;
        let piece_length = self.torrent.info.piece_length as usize;
        let files = self.torrent.info.files.clone();
        self.tpool.execute(move || {
            let mut disk_mgr = DiskManager::new(total_pieces, total_size, piece_length, files);
            while let Ok(ev) = io_rx.recv() {
                match ev {
                    IoTask::WriteToDisk {
                        piece,
                        offset,
                        data,
                    } => {
                        if let Err(e) = disk_mgr.write_to_disk(piece, offset, &data) {
                            eprintln!("failed to write piece data to disk, index: {}, offset: {}, bytes: {} :: {}", piece, offset, data.len(), e);
                            let _ = event_tx.send(Event::Stop);
                            return;
                        }
                    },
                    IoTask::DiskVerifyPiece {
                        piece,
                        expected_hash,
                    } => {
                        if picker::verify_piece(piece, expected_hash, &disk_mgr) {
                            let _ = event_tx.send(Event::PieceVerified { piece });
                            let _ = peer_io_tx.send(PeerIoTask::PieceVerified { piece });
                        } else {
                            let _ = event_tx.send(Event::PieceVerificationFailed { piece });
                            let _ = peer_io_tx.send(PeerIoTask::PieceVerificationFailed { piece });
                        }
                    },
                    IoTask::Stop => return,
                    IoTask::CalculateFileStats { bitfield } => {
                        let mut total_downloaded = 0;
                        let files: Vec<(PathBuf, u64, u64)> = disk_mgr
                            .files()
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                let downloaded = disk_mgr.verified_bytes_for_file(i, &bitfield);
                                total_downloaded += downloaded;
                                (f.path.clone(), downloaded, f.length)
                            })
                            .collect();
                        let _ = event_tx.send(Event::DiskStats {
                            total_downloaded,
                            files,
                        });
                    }
                }
            }
        });
    }

    fn start_tick_thread(&self) {
        let stop_signal = self.stop_signal.clone();
        let event_tx = self.event_tx.clone();
        self.tpool.execute(move || {
            loop {
                let _ = event_tx.send(Event::Tick);
                if stop_signal.wait_timeout(Duration::from_millis(500)) {
                    return;
                }
            }
        });
    }

    fn start_peer_io_thread(&self, peer_io_rx: Receiver<PeerIoTask>) {
        let peer_io_tx = self.peer_io_tx.clone();
        let tpool_clone = self.tpool.clone();
        let total_pieces = self.torrent.info.piece_hashes.len();
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let piece_length = self.torrent.info.piece_length;
        let event_tx = self.event_tx.clone();
        let total_size = self.torrent.info.total_size;
        let piece_hashes = self.torrent.info.piece_hashes.clone();
        let io_tx = self.io_tx.clone();
        let stop_signal = self.stop_signal.clone();
        self.tpool.execute(move || {
            let mut picker = match PiecePicker::new(piece_length as usize, total_size, piece_hashes)
            {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("failed to create piece picker: {}", e);
                    let _ = event_tx.send(Event::Stop);
                    return;
                }
            };
            let mut connection_mgr =
                match ConnectionManager::new(peer_io_tx, tpool_clone, event_tx.clone(), io_tx) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("failed to create connection manager: {}", e);
                        let _ = event_tx.send(Event::Stop);
                        return;
                    }
                };
            const MAX_EVENTS_PER_TICK: usize = 100;
            loop {
                // Control messages
                let mut processed = 0;
                while processed < MAX_EVENTS_PER_TICK
                    && let Ok(ev) = peer_io_rx.try_recv()
                {
                    match ev {
                        PeerIoTask::Stop => return,
                        PeerIoTask::NewPeers { peers } => {
                            connection_mgr.on_new_peers(peers);
                        }
                        PeerIoTask::MaybeConnectPeers => {
                            connection_mgr.maybe_adjust_peers(total_size, &mut picker);
                        }
                        PeerIoTask::ConnectPeer { addr } => {
                            if stop_signal.is_set() {
                                continue;
                            }
                            connection_mgr.connect_peer(
                                addr,
                                info_hash,
                                peer_id,
                                stop_signal.clone(),
                            );
                        }
                        PeerIoTask::PeerConnected { addr, stream } => {
                            if let Err(e) =
                                connection_mgr.on_peer_connected(addr, stream, total_pieces)
                            {
                                eprintln!("failed to add connected peer {} :: {}", addr, e);
                            }
                        }
                        PeerIoTask::PeerConnectFailed { addr, reason } => {
                            if let Err(e) =
                                connection_mgr.on_peer_connect_failed(addr, reason, &mut picker)
                            {
                                eprintln!("failed to handle peer {} connect failure: {}", addr, e);
                            }
                        }
                        PeerIoTask::PeerDisconnected { addr, reason } => {
                            if let Err(e) =
                                connection_mgr.on_peer_disconnected(addr, reason, &mut picker)
                            {
                                eprintln!("failed to handle peer {} disconnect: {}", addr, e);
                            }
                        }
                        PeerIoTask::PeerMessage { addr, msg } => {
                            connection_mgr.handle_peer_message(addr, msg, &mut picker);
                        }
                        PeerIoTask::MaybeUpdateInterest {
                            addr,
                            bitfield_interested,
                        } => {
                            connection_mgr.maybe_update_interest(addr, bitfield_interested);
                        }
                        PeerIoTask::SendMessage { addr, msg } => {
                            connection_mgr.enqueue_peer_message(&addr, msg);
                        }
                        PeerIoTask::RequestBlocksForPeer { addr } => {
                            connection_mgr.request_blocks_for_peer(addr, &mut picker);
                        }
                        PeerIoTask::MaybeRequestBlocks => {
                            connection_mgr.maybe_request_blocks();
                        }
                        PeerIoTask::PeriodicReap {
                            should_enter_endgame,
                        } => {
                            connection_mgr.reap_block_timeouts(&mut picker);
                            if should_enter_endgame {
                                picker.enter_endgame();
                            }
                        }
                        PeerIoTask::PieceVerified { piece } => {
                            picker.on_piece_verified(piece);
                        }
                        PeerIoTask::PieceVerificationFailed { piece } => {
                            picker.on_piece_verification_failure(piece);
                        }
                        PeerIoTask::StatsUpdate => {
                            let _ = event_tx.send(Event::PeerIoStats {
                                connected_peers: connection_mgr.connected_peers(),
                                max_peers: connection_mgr.max_peers(),
                                blocks_inflight: picker.in_flight(),
                                available_peers: connection_mgr.available_peers(),
                            });
                        }
                    }
                    processed += 1;
                }

                match connection_mgr.poll_peers() {
                    Ok(addrs) => {
                        connection_mgr.drain_peer_messages(addrs, total_pieces);
                    }
                    Err(e) => {
                        eprintln!("failed to poll peers: {}", e);
                    }
                }
            }
        });
    }

    fn start_announce_io_thread(&self, announce_io_rx: Receiver<AnnounceIoTask>) {
        let peer_io_tx = self.peer_io_tx.clone();
        self.tpool.execute(move || {
            while let Ok(ev) = announce_io_rx.recv() {
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
                                eprintln!("fetched tracker: {} :: {:?}", url, response);
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
        });
    }

    pub fn on_tick(&mut self) {
        if self.completed {
            return;
        }
        self.check_announce();
        self.handle_disk_io_tick();
        self.handle_peer_io_tick();
    }

    fn handle_disk_io_tick(&self) {
        let _ = self.io_tx.send(IoTask::CalculateFileStats {
            bitfield: self.stats.bitfield.clone(),
        });
    }

    fn handle_peer_io_tick(&mut self) {
        let _ = self.peer_io_tx.send(PeerIoTask::StatsUpdate);
        if !self.endgame {
            let percent_left = self.stats.left as f64 / self.torrent.info.total_size as f64;
            let should_enter_endgame = percent_left < self.endgame_threshold;
            self.endgame = should_enter_endgame;
        }
        let _ = self.peer_io_tx.send(PeerIoTask::PeriodicReap {
            should_enter_endgame: self.endgame,
        });
        let _ = self.peer_io_tx.send(PeerIoTask::MaybeRequestBlocks);
        if self.endgame {
            return;
        }
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

fn endgame_threshold(total_size: u64) -> f64 {
    let total_mb = total_size as f64 / (1024.0 * 1024.0);

    let threshold = MAX_ENDGAME_THRESHOLD / (1.0 + (total_mb / 100.0).ln());

    threshold.clamp(MIN_ENDGAME_THRESHOLD, MAX_ENDGAME_THRESHOLD)
}
