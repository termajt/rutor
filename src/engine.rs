use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    time::Duration,
};

use crate::{
    announce::{self, AnnounceManager, TrackerResponse},
    bitfield::Bitfield,
    disk::DiskManager,
    peer::{self, Peer, PeerMessage},
    picker::{self, BLOCK_SIZE, PiecePicker},
    torrent::Torrent,
};

use rand::{Rng, distr::Alphanumeric};
use threadpool::ThreadPool;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const MAX_PEER_IO: usize = 32;
pub const MAX_PEER_PIPELINE: usize = 25;

#[derive(Debug)]
pub enum Event {
    Tick,
    TrackerAnnounceDue {
        tracker_url: String,
    },
    TrackerResponse {
        tracker_url: String,
        response: TrackerResponse,
    },
    TrackerError {
        tracker_url: String,
        error: String,
    },
    PeerConnected {
        addr: SocketAddr,
        stream: TcpStream,
    },
    PeerConnectFailed {
        addr: SocketAddr,
        error: String,
    },
    PeerHandshakeComplete {
        addr: SocketAddr,
    },
    PeerDisconnected {
        addr: SocketAddr,
        reason: String,
    },
    PeerMessage {
        addr: SocketAddr,
        message: PeerMessage,
    },
    SendPeerMessage {
        addr: SocketAddr,
        message: PeerMessage,
    },
    VerifyPiece {
        piece: usize,
        expected_hash: [u8; 20],
    },
    PieceVerified {
        piece: usize,
    },
    PieceVerificationFailure {
        piece: usize,
    },
    Complete,
}

#[derive(Debug)]
pub enum IoTask {
    Announce {
        tracker_url: String,
        event: Option<String>,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        stats: TorrentStats,
    },
    ConnectPeer {
        addr: SocketAddr,
    },
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
}

#[derive(Debug, Clone)]
pub struct TorrentStats {
    pub downloaded: u64,
    pub uploaded: u64,
    pub left: u64,
    pub files: Vec<(PathBuf, u64, u64)>,
    pub pieces_verified: usize,
    pub peers: usize,
    pub bitfield: Bitfield,
    pub available_peers: usize,
}

impl TorrentStats {
    fn new(
        total_size: u64,
        files: Vec<(PathBuf, u64, u64)>,
        downloaded: u64,
        uploaded: u64,
        pieces_verified: usize,
        peers: usize,
        bitfield: Bitfield,
        available_peers: usize,
    ) -> Self {
        let left = total_size.saturating_sub(downloaded);
        Self {
            downloaded,
            uploaded,
            left,
            files,
            pieces_verified,
            peers,
            bitfield,
            available_peers,
        }
    }
}

#[derive(Debug)]
pub struct Engine {
    announce_mgr: AnnounceManager,
    torrent: Torrent,
    event_tx: Sender<Event>,
    event_rx: Receiver<Event>,
    io_tx: Sender<IoTask>,
    stats: TorrentStats,
    peer_id: [u8; 20],
    peers: HashMap<SocketAddr, Peer>,
    tpool: Arc<ThreadPool>,
    pending_peers: HashSet<SocketAddr>,
    available_peers: HashSet<SocketAddr>,
    picker: PiecePicker,
    bitfield: Bitfield,
    disk_mgr: Arc<Mutex<DiskManager>>,
}

impl Engine {
    pub fn new(
        torrent: Torrent,
        event_tx: Sender<Event>,
        event_rx: Receiver<Event>,
        io_tx: Sender<IoTask>,
        tpool: Arc<ThreadPool>,
    ) -> Self {
        let picker = PiecePicker::new(&torrent, event_tx.clone());
        let bitfield = Bitfield::new(torrent.info.piece_hashes.len());
        let disk_mgr = Arc::new(Mutex::new(DiskManager::new(&torrent)));
        let stats = calculate_stats(&torrent, 0, 0, &bitfield, disk_mgr.clone());
        Self {
            announce_mgr: AnnounceManager::new(&torrent),
            torrent: torrent,
            event_tx,
            event_rx,
            io_tx,
            stats: stats,
            peer_id: generate_peer_id(),
            peers: HashMap::new(),
            tpool,
            pending_peers: HashSet::new(),
            available_peers: HashSet::new(),
            picker,
            bitfield,
            disk_mgr,
        }
    }

    pub fn poll_event(&self) -> Result<Event> {
        Ok(self.event_rx.recv()?)
    }

    pub fn handle_event(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Tick => {
                self.on_tick()?;
            }
            Event::TrackerResponse {
                tracker_url,
                response,
            } => {
                self.on_tracker_response(&tracker_url, response)?;
                self.attempt_connect_peers()?;
            }
            Event::TrackerError { tracker_url, error } => {
                self.on_tracker_error(&tracker_url, &error)?;
            }
            Event::TrackerAnnounceDue { tracker_url } => {
                self.on_tracker_announce_due(tracker_url)?;
            }
            Event::PeerConnected { addr, stream } => {
                self.on_peer_connected(addr, stream)?;
                self.attempt_connect_peers()?;
            }
            Event::PeerConnectFailed { addr, error } => {
                self.on_peer_connect_failed(addr, error)?;
                self.attempt_connect_peers()?;
            }
            Event::PeerDisconnected { addr, reason } => {
                self.on_peer_disconnected(addr, reason)?;
                self.attempt_connect_peers()?;
            }
            Event::PeerMessage { addr, message } => {
                self.on_peer_message(addr, message)?;
            }
            Event::SendPeerMessage { addr, message } => {
                self.on_send_peer_message(addr, message)?;
            }
            Event::PeerHandshakeComplete { addr } => {
                self.on_peer_handshake_complete(addr)?;
            }
            Event::PieceVerified { piece } => {
                self.on_piece_verified(piece)?;
            }
            Event::Complete => {}
            Event::VerifyPiece {
                piece,
                expected_hash,
            } => {
                let _ = self.io_tx.send(IoTask::DiskVerifyPiece {
                    piece,
                    expected_hash,
                });
            }
            Event::PieceVerificationFailure { piece } => {
                self.on_piece_verification_failure(piece)?;
            }
        }
        Ok(())
    }

    pub fn emit(&self, ev: Event) -> Result<()> {
        Ok(self.event_tx.send(ev)?)
    }

    fn on_piece_verified(&mut self, piece: usize) -> Result<()> {
        self.bitfield.set(&piece, true);
        self.picker.on_piece_verified(piece);
        Ok(())
    }

    fn on_piece_verification_failure(&mut self, piece: usize) -> Result<()> {
        self.picker.on_piece_verification_failure(piece);
        Ok(())
    }

    fn on_tracker_announce_due(&self, tracker_url: String) -> Result<()> {
        Ok(self.io_tx.send(IoTask::Announce {
            tracker_url,
            event: None,
            info_hash: self.torrent.info_hash,
            peer_id: self.peer_id,
            stats: self.stats.clone(),
        })?)
    }

    fn on_send_peer_message(&mut self, addr: SocketAddr, message: PeerMessage) -> Result<()> {
        if let Some(peer) = self.peers.get(&addr) {
            let _ = peer.outbound_queue.send(message);
        }
        Ok(())
    }

    fn on_peer_connected(&mut self, addr: SocketAddr, stream: TcpStream) -> Result<()> {
        self.pending_peers.remove(&addr);
        if self.peers.contains_key(&addr) {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel();
        self.peers.insert(
            addr,
            Peer::new(
                addr,
                self.torrent.info.piece_hashes.len(),
                tx.clone(),
                MAX_PEER_PIPELINE,
            ),
        );
        let event_tx = self.event_tx.clone();
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let total_pieces = self.torrent.info.piece_hashes.len();
        self.tpool.execute(move || {
            peer::peer_io_loop(
                addr,
                stream,
                event_tx,
                &info_hash,
                &peer_id,
                total_pieces,
                rx,
            );
        });
        Ok(())
    }

    fn on_peer_handshake_complete(&self, addr: SocketAddr) -> Result<()> {
        self.emit(Event::SendPeerMessage {
            addr,
            message: PeerMessage::Bitfield(self.stats.bitfield.clone()),
        })
    }

    fn on_peer_connect_failed(&mut self, addr: SocketAddr, error: String) -> Result<()> {
        eprintln!("peer connect to {} failed: {}", addr, error);
        self.peers.remove(&addr);
        self.pending_peers.remove(&addr);
        self.available_peers.remove(&addr);
        Ok(())
    }

    fn on_peer_disconnected(&mut self, addr: SocketAddr, reason: String) -> Result<()> {
        eprintln!("peer {} disconnected: {}", addr, reason);
        self.peers.remove(&addr);
        self.pending_peers.remove(&addr);
        self.available_peers.remove(&addr);
        Ok(())
    }

    fn on_peer_message(&mut self, addr: SocketAddr, message: PeerMessage) -> Result<()> {
        match message {
            PeerMessage::Choke => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                peer.am_choked = true;
            }
            PeerMessage::Unchoke => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                peer.am_choked = false;
                peer.try_request_from_peer(&mut self.picker, &self.bitfield, &self.event_tx);
            }
            PeerMessage::Interested => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                peer.interested = true;
            }
            PeerMessage::NotInterested => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                peer.interested = false;
            }
            PeerMessage::Bitfield(bf) => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                if on_bitfield_from_peer(peer, bf, &self.bitfield, &self.event_tx) {
                    peer.try_request_from_peer(&mut self.picker, &self.bitfield, &self.event_tx);
                }
            }
            PeerMessage::Have(piece_index) => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                if on_have_from_peer(peer, piece_index as usize, &self.bitfield, &self.event_tx) {
                    peer.try_request_from_peer(&mut self.picker, &self.bitfield, &self.event_tx);
                }
            }
            PeerMessage::Piece((piece, begin, data)) => {
                let peer = match self.peers.get_mut(&addr) {
                    Some(p) => p,
                    None => return Ok(()),
                };
                on_block_received_from_peer(
                    peer,
                    piece as usize,
                    begin as usize,
                    data,
                    &mut self.picker,
                    &self.io_tx,
                );
                peer.try_request_from_peer(&mut self.picker, &self.bitfield, &self.event_tx);
            }
            _ => {}
        }
        Ok(())
    }

    fn on_tracker_error(&mut self, tracker_url: &String, error: &String) -> Result<()> {
        eprintln!("error from tracker {}: {}", tracker_url, error);
        Ok(())
    }

    fn on_tracker_response(
        &mut self,
        tracker_url: &String,
        response: TrackerResponse,
    ) -> Result<()> {
        eprintln!(
            "tracker response: {}, {} peers",
            tracker_url,
            response.peers.len()
        );
        for peer in response.peers {
            if self.available_peers.contains(&peer)
                || self.peers.contains_key(&peer)
                || self.pending_peers.contains(&peer)
            {
                continue;
            }
            self.available_peers.insert(peer);
        }
        Ok(())
    }

    fn on_tick(&mut self) -> Result<()> {
        if let Some(tracker) = self.announce_mgr.next_due_tracker() {
            let url = tracker.url.clone();
            tracker.announced();

            self.emit(Event::TrackerAnnounceDue { tracker_url: url })?;
        }
        self.stats = calculate_stats(
            &self.torrent,
            self.peers.len(),
            self.available_peers.len(),
            &self.bitfield,
            self.disk_mgr.clone(),
        );

        for (addr, peer) in self.peers.iter_mut() {
            let timeout = block_timeout(peer.stats.avg_speed, BLOCK_SIZE);
            self.picker.reap_timeouts_for_peer(&addr, timeout);
            peer.try_request_from_peer(&mut self.picker, &self.bitfield, &self.event_tx);
        }
        if self.stats.left < 5 * self.picker.piece_length as u64 {
            self.picker.enter_endgame();
        }
        if self.bitfield.count_ones() == self.torrent.info.piece_hashes.len() {
            let _ = self.emit(Event::Complete);
        }
        Ok(())
    }

    fn attempt_connect_peers(&mut self) -> Result<()> {
        if self.available_peers.is_empty() {
            eprintln!("no available peers!");
        }
        if self.peers.len() >= MAX_PEER_IO {
            return Ok(());
        }
        let left_to_connect = MAX_PEER_IO
            .saturating_sub(self.peers.len())
            .saturating_sub(self.pending_peers.len());
        if left_to_connect == 0 {
            return Ok(());
        }
        let mut left = left_to_connect;
        eprintln!("trying to connect {} peers...", left);
        for peer in &self.available_peers {
            if left == 0 {
                break;
            }
            self.pending_peers.insert(*peer);
            let _ = self.io_tx.send(IoTask::ConnectPeer { addr: *peer });
            left -= 1;
        }
        Ok(())
    }

    pub fn get_stats(&self) -> TorrentStats {
        self.stats.clone()
    }

    pub fn start(&self, io_rx: Receiver<IoTask>) {
        self.spawn_io_handle(
            self.tpool.clone(),
            io_rx,
            self.event_tx.clone(),
            self.disk_mgr.clone(),
        );
    }

    pub fn stop(&self) {
        let _ = self.io_tx.send(IoTask::Stop);
    }

    fn spawn_io_handle(
        &self,
        tpool: Arc<ThreadPool>,
        io_rx: Receiver<IoTask>,
        event_tx: Sender<Event>,
        disk_mgr: Arc<Mutex<DiskManager>>,
    ) {
        let tpool_clone = tpool.clone();
        tpool.execute(move || {
            loop {
                if let Ok(ev) = io_rx.recv() {
                    match ev {
                        IoTask::Announce {
                            tracker_url,
                            event,
                            info_hash,
                            peer_id,
                            stats,
                        } => {
                            eprintln!("announcing to: {}", tracker_url);
                            let event_tx = event_tx.clone();
                            tpool_clone.execute(move || {
                                let result = announce::announce(
                                    &tracker_url,
                                    &info_hash,
                                    &peer_id,
                                    6081,
                                    stats.uploaded,
                                    stats.downloaded,
                                    stats.left,
                                    event,
                                    None,
                                );
                                match result {
                                    Ok(response) => {
                                        let _ = event_tx.send(Event::TrackerResponse {
                                            tracker_url,
                                            response,
                                        });
                                    }
                                    Err(e) => {
                                        let err_msg = format!("{}", e);
                                        let _ = event_tx.send(Event::TrackerError {
                                            tracker_url,
                                            error: err_msg,
                                        });
                                    }
                                }
                            });
                        }
                        IoTask::ConnectPeer { addr } => {
                            let event_tx = event_tx.clone();
                            tpool_clone.execute(move || {
                                match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
                                    Ok(stream) => {
                                        let _ = stream.set_nodelay(true);
                                        let _ =
                                            stream.set_read_timeout(Some(Duration::from_secs(10)));
                                        let _ =
                                            stream.set_write_timeout(Some(Duration::from_secs(10)));

                                        let _ =
                                            event_tx.send(Event::PeerConnected { addr, stream });
                                    }
                                    Err(e) => {
                                        let _ = event_tx.send(Event::PeerConnectFailed {
                                            addr,
                                            error: e.to_string(),
                                        });
                                    }
                                }
                            });
                        }
                        IoTask::WriteToDisk {
                            piece,
                            offset,
                            data,
                        } => {
                            let mut disk_mgr = disk_mgr.lock().unwrap();
                            if let Err(e) = disk_mgr.write_to_disk(piece, offset, &data) {
                                eprintln!(
                                    "failed to write data to disk (piece: {}, offset: {}): {}",
                                    piece,
                                    offset,
                                    e.to_string()
                                );
                            }
                        }
                        IoTask::Stop => break,
                        IoTask::DiskVerifyPiece {
                            piece,
                            expected_hash,
                        } => {
                            let disk_mgr = disk_mgr.lock().unwrap();
                            let verified = picker::verify_piece(piece, expected_hash, &disk_mgr);
                            if verified {
                                let _ = event_tx.send(Event::PieceVerified { piece });
                            } else {
                                let _ = event_tx.send(Event::PieceVerificationFailure { piece });
                            }
                        }
                    }
                }
            }
        });
    }
}

fn on_bitfield_from_peer(
    peer: &mut Peer,
    new_bitfield: Bitfield,
    my_bitfield: &Bitfield,
    event_tx: &Sender<Event>,
) -> bool {
    peer.bitfield = new_bitfield;
    check_interested(peer, my_bitfield, event_tx)
}

fn on_have_from_peer(
    peer: &mut Peer,
    piece_idx: usize,
    my_bitfield: &Bitfield,
    event_tx: &Sender<Event>,
) -> bool {
    peer.bitfield.set(&piece_idx, true);
    check_interested(peer, my_bitfield, event_tx)
}

fn check_interested(peer: &mut Peer, bitfield: &Bitfield, event_tx: &Sender<Event>) -> bool {
    let interested = peer.bitfield.is_interesting_to(bitfield);
    let changed = interested != peer.am_interested;
    peer.am_interested = interested;

    if changed {
        let _ = event_tx.send(Event::SendPeerMessage {
            addr: peer.addr,
            message: if interested {
                PeerMessage::Interested
            } else {
                PeerMessage::NotInterested
            },
        });
    }

    interested
}

fn on_block_received_from_peer(
    peer: &mut Peer,
    piece_idx: usize,
    begin: usize,
    data: Vec<u8>,
    picker: &mut PiecePicker,
    io_tx: &Sender<IoTask>,
) {
    peer.stats.update(data.len());
    peer.max_pipeline = peer.desired_pipeline(BLOCK_SIZE);
    peer.inflight_requests = peer.inflight_requests.saturating_sub(1);
    let received = picker.on_block_received(piece_idx, begin);
    if received {
        let _ = io_tx.send(IoTask::WriteToDisk {
            piece: piece_idx,
            offset: begin,
            data,
        });
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

fn calculate_stats(
    torrent: &Torrent,
    peers: usize,
    available_peers: usize,
    bitfield: &Bitfield,
    disk_mgr: Arc<Mutex<DiskManager>>,
) -> TorrentStats {
    let mut total_downloaded = 0;
    let bitfield = bitfield.clone();
    let pieces_verified = bitfield.count_ones();
    let files: Vec<(PathBuf, u64, u64)> = {
        let disk_mgr = disk_mgr.lock().unwrap();
        disk_mgr
            .files()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let downloaded = disk_mgr.verified_bytes_for_file(i, &bitfield);
                total_downloaded += downloaded;
                (f.path.clone(), downloaded, f.length)
            })
            .collect()
    };
    TorrentStats::new(
        torrent.info.total_size,
        files,
        total_downloaded,
        0,
        pieces_verified,
        peers,
        bitfield,
        available_peers,
    )
}

fn block_timeout(peer_speed: f64, block_size: usize) -> Duration {
    let estimated_time = block_size as f64 / peer_speed;
    let timeout_secs = estimated_time * 2.0;
    Duration::from_secs_f64(timeout_secs.clamp(1.0, 30.0))
}
