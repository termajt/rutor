use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    time::Duration,
};

use rand::{Rng, distr::Alphanumeric};
use threadpool::ThreadPool;

use crate::{
    announce::{self, AnnounceManager},
    bytespeed::ByteSpeed,
    consts::{self, ClientEvent, PeerEvent, PieceEvent},
    event::ManualResetEvent,
    peer::PeerManager,
    piece::PieceManager,
    pubsub::PubSub,
    socketmanager::{Command, SocketManager},
    torrent::Torrent,
};

/// Represents the current state of a torrent download.
///
/// Tracks total and current transfer statistics, as well as
/// timing and progress information. Used for reporting and coordination
/// between threads in the [`TorrentClient`].
#[derive(Debug, Clone)]
pub struct TorrentState {
    /// Total number of bytes downloaded so far.
    pub downloaded: u64,

    /// Total number of bytes left so far.
    pub left: u64,

    /// Total number of bytes uploaded so far.
    pub uploaded: u64,

    /// Total number of peers.
    pub peers: usize,

    /// Total number of connected peers.
    pub connected_peers: usize,

    /// The download byte speed.
    pub download_speed: ByteSpeed,
}

impl TorrentState {
    /// Creates a new [`TorrentState`] with the given total size and listening port.
    ///
    /// All counters are initialized to zero, and `left` is set equal to `total_size`.
    pub fn new() -> Self {
        TorrentState {
            downloaded: 0,
            left: 0,
            uploaded: 0,
            peers: 0,
            connected_peers: 0,
            download_speed: ByteSpeed::new(Duration::from_secs(20), Duration::from_secs(1)),
        }
    }
}

/// Represents a running torrent client instance.
///
/// The [`TorrentClient`] coordinates downloading and uploading pieces of
/// a torrent, manages tracker announcements, and maintains the overall
/// state through shared synchronization primitives.
pub struct TorrentClient {
    /// Shared reference to the [`Torrent`] metadata and info.
    torrent: Arc<RwLock<Torrent>>,
    state: Arc<RwLock<TorrentState>>,
    tpool: Arc<ThreadPool>,
    peer_id: [u8; 20],
    peer_manager: Arc<PeerManager>,
    piece_manager: Arc<RwLock<PieceManager>>,
    announce_manager: Arc<Mutex<AnnounceManager>>,
    socket_manager: Arc<Mutex<SocketManager>>,
    client_event: Arc<PubSub<ClientEvent>>,
    peer_event: Arc<PubSub<PeerEvent>>,
    piece_event: Arc<PubSub<PieceEvent>>,
    shutdown_ev: Arc<ManualResetEvent>,
    started: Mutex<bool>,
}

impl TorrentClient {
    /// Creates a new [`TorrentClient`] for the given torrent and listening port.
    ///
    /// Initializes shared state, thread pool, and generates a unique peer ID.
    pub fn new(torrent: Torrent) -> Result<Self, Box<dyn std::error::Error>> {
        let torrent = Arc::new(RwLock::new(torrent));
        let peer_id = generate_peer_id();
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let workers = std::env::var("RUTOR_MAX_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(cores);
        let thread_workers = (workers * 2).min(64);
        let tpool = Arc::new(ThreadPool::new(thread_workers));
        let client_event = Arc::new(PubSub::new());
        let peer_event = Arc::new(PubSub::new());
        let piece_event = Arc::new(PubSub::new());
        let peer_manager = Arc::new(PeerManager::new(
            50,
            tpool.clone(),
            torrent.clone(),
            peer_event.clone(),
            piece_event.clone(),
            client_event.clone(),
        )?);
        let piece_manager = Arc::new(RwLock::new(PieceManager::new(
            torrent.clone(),
            peer_manager.clone(),
            peer_event.clone(),
            client_event.clone(),
        )));
        let announce_manager = Arc::new(Mutex::new(AnnounceManager::new(torrent.clone())));
        let socket_manager = Arc::new(Mutex::new(SocketManager::new()?));
        Ok(Self {
            torrent: torrent,
            state: Arc::new(RwLock::new(TorrentState::new())),
            tpool: tpool,
            peer_id: peer_id,
            peer_manager: peer_manager,
            piece_manager: piece_manager,
            announce_manager: announce_manager,
            socket_manager: socket_manager,
            client_event: client_event,
            peer_event: peer_event,
            piece_event: piece_event,
            shutdown_ev: Arc::new(ManualResetEvent::new(false)),
            started: Mutex::new(false),
        })
    }

    pub fn file_status(&self) -> Vec<(PathBuf, u64, u64)> {
        let torrent = self.torrent.read().unwrap();
        let mut results = Vec::new();
        for f in &torrent.info.files {
            results.push((f.path(), f.written(), f.length()))
        }
        results
    }

    /// Starts the torrent client's background processes.
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut started = self.started.lock().unwrap();
        if *started {
            return Err("already started".into());
        }
        *started = true;
        drop(started);
        self.start_threads()?;
        let state = self.state.clone();
        let peer_manager = self.peer_manager.clone();
        let client_event_rx = self.client_event.subscribe(consts::TOPIC_CLIENT_EVENT)?;
        let torrent = self.torrent.clone();
        let shutdown_ev = self.shutdown_ev.clone();
        self.tpool.execute(move || {
            let mut delay = Duration::from_millis(5);
            let max_delay = Duration::from_secs(1);
            while !shutdown_ev.is_set() {
                match client_event_rx.recv_timeout(delay) {
                    Ok(ev) => {
                        handle_client_event(&ev, torrent.clone(), &state, &peer_manager);
                        delay = Duration::from_millis(5);
                    }
                    Err(_) => delay = (delay * 2).min(max_delay),
                }
            }
            // Drain events
            eprintln!("draining client events...");
            while let Ok(ev) = client_event_rx.try_recv() {
                handle_client_event(&ev, torrent.clone(), &state, &peer_manager);
            }
            eprintln!("client events drained!");
            eprintln!("client events thread returning!");
        });
        let state = self.state.clone();
        let shutdown_ev = self.shutdown_ev.clone();
        self.tpool.execute(move || {
            let mut prev_downloaded = 0;
            while !shutdown_ev.wait_timeout(Duration::from_millis(500)) {
                let mut state = state.write().unwrap();
                let downloaded_now = state.downloaded.saturating_sub(prev_downloaded);
                prev_downloaded = state.downloaded;
                state.download_speed.update(downloaded_now as usize);
                drop(state);
            }
            eprintln!("download speed thread returning!");
        });
        Ok(())
    }

    fn start_threads(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_tx = self.start_socket_manager()?;
        let max_handlers = std::cmp::max(2, self.tpool.max_count());
        self.start_peer_manager(max_handlers, socket_tx)?;
        self.start_piece_manager(max_handlers)?;
        self.start_announce_thread();
        Ok(())
    }

    fn start_announce_thread(&self) {
        let torrent = self.torrent.read().unwrap();
        let info_hash = torrent.info_hash;
        drop(torrent);
        let peer_id = self.peer_id;
        let state = self.state.clone();
        let announce_manager = self.announce_manager.clone();
        let peer_event_tx = self.peer_event.clone();
        let shutdown_ev = self.shutdown_ev.clone();
        self.tpool.execute(move || {
            loop {
                let mut manager = announce_manager.lock().unwrap();
                let tracker = {
                    match manager.next_due_tracker() {
                        Some(tracker) => tracker,
                        None => {
                            let next_time = manager
                                .tiers
                                .iter()
                                .flat_map(|tier| tier.iter().map(|t| t.time_until_next()))
                                .min()
                                .unwrap_or(Duration::from_secs(30));
                            drop(manager);
                            eprintln!("announcing again in {:?}", next_time);
                            if shutdown_ev.wait_timeout(next_time) {
                                break;
                            }
                            continue;
                        }
                    }
                };
                tracker.announced();
                let (uploaded, downloaded, left) = {
                    let state = state.read().unwrap();
                    (state.uploaded, state.downloaded, state.left)
                };
                if let Ok(response) = announce::announce(
                    &tracker.url,
                    &info_hash,
                    &peer_id,
                    6881,
                    uploaded,
                    downloaded,
                    left,
                    None,
                    None,
                ) {
                    tracker.update_from_response(&response);
                    drop(manager);
                    let _ = peer_event_tx.publish(
                        consts::TOPIC_PEER_EVENT,
                        PeerEvent::NewPeers {
                            peers: response.peers,
                        },
                    );
                }
            }
            eprintln!("announce thread returning!");
        });
    }

    fn start_socket_manager(&self) -> Result<Arc<Sender<Command>>, Box<dyn std::error::Error>> {
        let socket_manager = self.socket_manager.clone();
        let peer_event_tx = self.peer_event.clone();
        let shutdown_ev = self.shutdown_ev.clone();
        let (socket_tx, socket_rx) = mpsc::channel();
        let (data_tx, data_rx) = mpsc::channel();
        self.tpool.execute(move || {
            while !shutdown_ev.is_set() {
                let mut socket_manager = socket_manager.lock().unwrap();
                let mut events = Vec::new();
                match socket_manager.run_once(&socket_rx, &data_tx) {
                    Ok(disconnects) => {
                        for addr in disconnects {
                            events.push(PeerEvent::SocketDisconnect { addr: addr });
                        }
                    }
                    Err(e) => {
                        eprintln!("socket manager run_once failure: {e}");
                    }
                }
                drop(socket_manager);
                for event in events {
                    let _ = peer_event_tx.publish(consts::TOPIC_PEER_EVENT, event);
                }
            }
            eprintln!("socket manager thread returning!");
        });
        // let peer_event_tx = self.peer_event.clone();
        let shutdown_ev = self.shutdown_ev.clone();
        let peer_manager = self.peer_manager.clone();
        self.tpool.execute(move || {
            let mut delay = Duration::from_millis(5);
            let max_delay = Duration::from_millis(500);
            while !shutdown_ev.is_set() {
                match data_rx.recv_timeout(delay) {
                    Ok((data, addr)) => {
                        peer_manager.data_received(&addr, &data);
                        delay = Duration::from_millis(5);
                    }
                    Err(_) => delay = (delay * 2).min(max_delay),
                }
            }
            eprintln!("socket data thread returning!");
        });
        Ok(Arc::new(socket_tx))
    }

    fn start_peer_manager(
        &self,
        max_handlers: usize,
        socket_tx: Arc<Sender<Command>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let peer_manager = self.peer_manager.clone();
        let peer_id = self.peer_id;
        let peer_event_rx = self.peer_event.subscribe(consts::TOPIC_PEER_EVENT)?;
        let shutdown_ev = self.shutdown_ev.clone();
        let inflight = Arc::new(AtomicUsize::new(0));
        let tpool = self.tpool.clone();
        self.tpool.execute(move || {
            let mut delay = Duration::from_millis(5);
            let max_delay = Duration::from_secs(1);
            while !shutdown_ev.is_set() {
                match peer_event_rx.recv_timeout(delay) {
                    Ok(ev) => {
                        handle_peer_event(
                            ev,
                            inflight.clone(),
                            max_handlers,
                            peer_manager.clone(),
                            shutdown_ev.clone(),
                            tpool.clone(),
                            peer_id,
                            socket_tx.clone(),
                            false,
                        );
                    }
                    Err(_) => delay = (delay * 2).min(max_delay),
                }
            }

            // Drain events
            eprintln!("draining peer events...");
            while let Ok(ev) = peer_event_rx.try_recv() {
                handle_peer_event(
                    ev,
                    inflight.clone(),
                    max_handlers,
                    peer_manager.clone(),
                    shutdown_ev.clone(),
                    tpool.clone(),
                    peer_id,
                    socket_tx.clone(),
                    true,
                );
            }
            eprintln!("peer events drained!");
            eprintln!("peer events thread returning!");
        });
        Ok(())
    }

    fn start_piece_manager(&self, max_handlers: usize) -> Result<(), Box<dyn std::error::Error>> {
        {
            let inflight = Arc::new(AtomicUsize::new(0));
            let rx = self.piece_event.subscribe(consts::TOPIC_PIECE_EVENT)?;
            let piece_manager = self.piece_manager.clone();
            let shutdown_ev = self.shutdown_ev.clone();
            let tpool = self.tpool.clone();
            self.tpool.execute(move || {
                let mut delay = Duration::from_millis(5);
                let max_delay = Duration::from_secs(1);
                while !shutdown_ev.is_set() {
                    match rx.recv_timeout(delay) {
                        Ok(ev) => {
                            handle_piece_event(
                                ev,
                                inflight.clone(),
                                max_handlers,
                                piece_manager.clone(),
                                shutdown_ev.clone(),
                                tpool.clone(),
                                false,
                            );
                        }
                        Err(_) => delay = (delay * 2).min(max_delay),
                    }
                }

                // Drain events
                eprintln!("draining piece events...");
                while let Ok(ev) = rx.try_recv() {
                    handle_piece_event(
                        ev,
                        inflight.clone(),
                        max_handlers,
                        piece_manager.clone(),
                        shutdown_ev.clone(),
                        tpool.clone(),
                        true,
                    );
                }
                eprintln!("piece events drained!");
                eprintln!("piece events thread returning!");
            });
        }
        {
            let piece_manager = self.piece_manager.clone();
            let shutdown_ev = self.shutdown_ev.clone();
            self.tpool.execute(move || {
                while !shutdown_ev.wait_timeout(Duration::from_millis(250)) {
                    let piece_manager = piece_manager.read().unwrap();
                    piece_manager.run_piece_selection_once();
                    drop(piece_manager);
                }
                eprintln!("piece selection thread returning!");
            });
        }
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        if self.shutdown_ev.is_set() {
            return true;
        }
        let torrent = self.torrent.read().unwrap();
        torrent.info.is_complete()
    }

    /// Stops the torrent client and notifies all waiting threads.
    ///
    /// Sets the shutdown flag and signals any condition variables
    /// waiting for completion.
    pub fn stop(&self) {
        if self.shutdown_ev.is_set() {
            return;
        }
        self.shutdown_ev.set();
        let mut socket_manger = self.socket_manager.lock().unwrap();
        socket_manger.close();
        drop(socket_manger);
        eprintln!("socket manager closed!");
        eprintln!("closing threadpool...");
        self.tpool.join();
        eprintln!("threadpool closed and joined!");
        eprintln!("closing torrent...");
        let torrent = self.torrent.read().unwrap();
        torrent.close();
        eprintln!("torrent closed and flushed!");
    }

    pub fn get_thread_worker_count(&self) -> usize {
        self.tpool.active_count()
    }

    pub fn get_state(&self) -> TorrentState {
        self.state.read().unwrap().clone()
    }

    pub fn pieces_left(&self) -> usize {
        let torrent = self.torrent.read().unwrap();
        torrent.info.pieces_left()
    }

    pub fn total_pieces(&self) -> usize {
        let torrent = self.torrent.read().unwrap();
        torrent.info.piece_hashes.len()
    }

    pub fn pieces_verified(&self) -> usize {
        let torrent = self.torrent.read().unwrap();
        torrent.info.pieces_verified()
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

fn handle_client_event(
    ev: &ClientEvent,
    torrent: Arc<RwLock<Torrent>>,
    state: &RwLock<TorrentState>,
    peer_manager: &PeerManager,
) {
    match ev {
        ClientEvent::PieceVerificationFailure {
            piece_index: _,
            data_size,
        } => {
            let torrent = torrent.read().unwrap();
            let total_size = torrent.info.total_size;
            drop(torrent);
            let mut state = state.write().unwrap();
            state.downloaded = state.downloaded.saturating_sub(*data_size as u64);
            state.left = total_size.saturating_sub(state.downloaded);
        }
        ClientEvent::PeersChanged => {
            let mut state = state.write().unwrap();
            state.connected_peers = peer_manager.connected_peers();
            state.peers = peer_manager.peer_count();
        }
        ClientEvent::PieceVerified { piece_index } => {
            let mut torrent = torrent.write().unwrap();
            torrent.info.set_bitfield_index(*piece_index);
        }
        ClientEvent::WriteToDisk {
            piece_index,
            begin,
            data,
        } => {
            let mut guard = torrent.write().unwrap();
            if let Err(e) = guard.write_to_disk(*piece_index, *begin, data) {
                eprintln!("failed to write block to disk: {e}");
            }
            drop(guard);
            let torrent = torrent.read().unwrap();
            let total_size = torrent.info.total_size;
            let mut state = state.write().unwrap();
            state.downloaded += data.len() as u64;
            state.left = total_size.saturating_sub(state.downloaded);
        }
    }
}

fn handle_peer_event(
    ev: Arc<PeerEvent>,
    inflight: Arc<AtomicUsize>,
    max_handlers: usize,
    peer_manager: Arc<PeerManager>,
    shutdown_ev: Arc<ManualResetEvent>,
    tpool: Arc<ThreadPool>,
    peer_id: [u8; 20],
    socket_tx: Arc<Sender<Command>>,
    sync: bool,
) {
    if sync {
        eprintln!("handling peer event: {ev:?}");
        peer_manager.handle_event(ev, peer_id, socket_tx);
        return;
    }
    let is_connect_related = matches!(
        *ev,
        PeerEvent::ConnectFailure { .. }
            | PeerEvent::SocketDisconnect { .. }
            | PeerEvent::PeerConnected { .. }
            | PeerEvent::NewPeers { .. }
    );

    if is_connect_related {
        let cur = inflight.load(Ordering::Relaxed);
        if cur < max_handlers {
            inflight.fetch_add(1, Ordering::SeqCst);

            let peer_manager = peer_manager.clone();
            let inflight = inflight.clone();
            let shutdown_ev = shutdown_ev.clone();
            let ev = ev.clone();
            let socket_tx = socket_tx.clone();

            tpool.execute(move || {
                if shutdown_ev.is_set() {
                    inflight.fetch_sub(1, Ordering::SeqCst);
                    return;
                }

                peer_manager.handle_event(ev, peer_id, socket_tx);
            });
        } else {
            peer_manager.handle_event(ev, peer_id, socket_tx);
        }
    } else {
        peer_manager.handle_event(ev, peer_id, socket_tx);
    }
}

fn handle_piece_event(
    ev: Arc<PieceEvent>,
    inflight: Arc<AtomicUsize>,
    max_handlers: usize,
    piece_manager: Arc<RwLock<PieceManager>>,
    shutdown_ev: Arc<ManualResetEvent>,
    tpool: Arc<ThreadPool>,
    sync: bool,
) {
    if sync {
        eprintln!("handling piece event: {ev:?}");
        let mut piece_manager = piece_manager.write().unwrap();
        piece_manager.handle_event(ev);
        return;
    }
    let cur = inflight.load(Ordering::Relaxed);
    if cur < max_handlers {
        inflight.fetch_add(1, Ordering::SeqCst);

        let inflight = inflight.clone();
        let piece_manager = piece_manager.clone();
        let ev = ev.clone();
        let shutdown_ev = shutdown_ev.clone();
        tpool.execute(move || {
            if shutdown_ev.is_set() {
                inflight.fetch_sub(1, Ordering::SeqCst);
                return;
            }

            let mut piece_manager = piece_manager.write().unwrap();
            piece_manager.handle_event(ev);
            drop(piece_manager);
            inflight.fetch_sub(1, Ordering::SeqCst);
        });
    } else {
        let mut piece_manager = piece_manager.write().unwrap();
        piece_manager.handle_event(ev);
    }
}
