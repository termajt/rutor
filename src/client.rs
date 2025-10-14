use std::{
    sync::{
        Arc, Condvar, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use rand::{Rng, distr::Alphanumeric};

use crate::{
    announce::{self, AnnounceManager},
    consts::{self, ClientEvent, PeerEvent, PieceEvent},
    peer::PeerManager,
    piece::PieceManager,
    pool::ThreadPool,
    pubsub::PubSub,
    queue::{Queue, Receiver},
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
    /// The local port used by the torrent client.
    pub port: u16,

    /// Total number of bytes downloaded so far.
    pub downloaded: u64,

    /// Total number of bytes uploaded so far.
    pub uploaded: u64,

    /// Total size of the torrent in bytes.
    pub total_size: u64,

    /// Number of bytes left to download.
    pub left: u64,

    /// Timestamp when the torrent was started.
    pub started_at: Option<Instant>,

    pub peers: usize,
    pub connected_peers: usize,
}

impl TorrentState {
    /// Creates a new [`TorrentState`] with the given total size and listening port.
    ///
    /// All counters are initialized to zero, and `left` is set equal to `total_size`.
    pub fn new(total_size: u64, port: u16) -> Self {
        TorrentState {
            port: port,
            downloaded: 0,
            uploaded: 0,
            total_size: total_size,
            left: total_size,
            started_at: None,
            peers: 0,
            connected_peers: 0,
        }
    }

    /// Atomically updates transfer statistics, adding downloaded and uploaded byte counts.
    ///
    /// This method saturates to prevent numeric overflow.
    pub fn inc_update_stats(&mut self, downloaded: u64, uploaded: u64) {
        self.downloaded = self.downloaded.saturating_add(downloaded);
        self.uploaded = self.uploaded.saturating_add(uploaded);
        self.left = self.total_size.saturating_sub(self.downloaded);
    }
}

/// Represents a running torrent client instance.
///
/// The [`TorrentClient`] coordinates downloading and uploading pieces of
/// a torrent, manages tracker announcements, and maintains the overall
/// state through shared synchronization primitives.
pub struct TorrentClient {
    /// Shared reference to the [`Torrent`] metadata and info.
    pub torrent: Arc<Torrent>,
    state: Arc<Mutex<TorrentState>>,
    tpool: Arc<ThreadPool>,
    peer_id: [u8; 20],
    state_condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    shutdown_condvar: Arc<Condvar>,
    peer_manager: Arc<PeerManager>,
    piece_manager: Arc<RwLock<PieceManager>>,
    announce_manager: Arc<Mutex<AnnounceManager>>,
    socket_manager: Arc<Mutex<SocketManager>>,
    client_event: Arc<PubSub<ClientEvent>>,
    peer_event: Arc<PubSub<PeerEvent>>,
    socket_rx: Arc<Receiver<Command>>,
    piece_event: Arc<PubSub<PieceEvent>>,
}

impl TorrentClient {
    /// Creates a new [`TorrentClient`] for the given torrent and listening port.
    ///
    /// Initializes shared state, thread pool, and generates a unique peer ID.
    pub fn new(torrent: Torrent, port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let torrent = Arc::new(torrent);
        let total_size = torrent.info.total_size;
        let peer_id = generate_peer_id();
        let tpool = Arc::new(ThreadPool::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_condvar = Arc::new(Condvar::new());
        let (socket_tx, socket_rx) = Queue::new(None);
        let (socket_tx, socket_rx) = (Arc::new(socket_tx), Arc::new(socket_rx));
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
            socket_tx.clone(),
        )?);
        let piece_manager = Arc::new(RwLock::new(PieceManager::new(
            torrent.clone(),
            peer_manager.clone(),
            peer_event.clone(),
            client_event.clone(),
        )));
        let announce_manager = Arc::new(Mutex::new(AnnounceManager::new(&torrent)));
        let socket_manager = Arc::new(Mutex::new(SocketManager::new()?));
        Ok(Self {
            torrent: torrent,
            state: Arc::new(Mutex::new(TorrentState::new(total_size, port))),
            tpool: tpool,
            peer_id: peer_id,
            state_condvar: Arc::new(Condvar::new()),
            shutdown: shutdown,
            shutdown_condvar: shutdown_condvar,
            peer_manager: peer_manager,
            piece_manager: piece_manager,
            announce_manager: announce_manager,
            socket_manager: socket_manager,
            client_event: client_event,
            peer_event: peer_event,
            socket_rx: socket_rx,
            piece_event: piece_event,
        })
    }

    /// Starts the torrent client's background processes.
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut st = self.state.lock().unwrap();
            if st.started_at.is_some() {
                return Err("already started".into());
            }
            st.started_at = Some(Instant::now());
        }
        self.start_announce_thread();
        self.start_poll_thread();
        let state = self.state.clone();
        let peer_manager = self.peer_manager.clone();
        let client_event_rx = self.client_event.subscribe(consts::TOPIC_CLIENT_EVENT);
        self.tpool.execute(move || {
            while let Ok(ev) = client_event_rx.recv() {
                match &*ev {
                    ClientEvent::BytesDownloaded { data_size } => {
                        let mut state = state.lock().unwrap();
                        state.downloaded += *data_size as u64;
                        state.left = state.total_size.saturating_sub(state.downloaded);
                    }
                    ClientEvent::PieceVerificationFailure {
                        piece_index: _,
                        data_size,
                    } => {
                        let mut state = state.lock().unwrap();
                        state.downloaded = state.downloaded.saturating_sub(*data_size as u64);
                        state.left = state.total_size.saturating_sub(state.downloaded);
                    }
                    ClientEvent::PeersChanged => {
                        let mut state = state.lock().unwrap();
                        state.connected_peers = peer_manager.connected_peers();
                        state.peers = peer_manager.peer_count();
                    }
                }
            }
        });
        Ok(())
    }

    fn start_poll_thread(&self) {
        let socket_manager = self.socket_manager.clone();
        let shutdown = self.shutdown.clone();
        let peer_event_tx = self.peer_event.clone();
        let (data_tx, data_rx) = Queue::new(None);
        let socket_rx = self.socket_rx.clone();
        self.tpool.execute(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let mut socket_manager = socket_manager.lock().unwrap();
                match socket_manager.run_once(&socket_rx, &data_tx) {
                    Ok(disconnects) => {
                        for addr in disconnects {
                            peer_event_tx.publish(
                                consts::TOPIC_PEER_EVENT,
                                PeerEvent::SocketDisconnect { addr: addr },
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("socket manager run_once failure: {e}");
                    }
                }
            }
        });

        let peer_manager = self.peer_manager.clone();
        let peer_id = self.peer_id;
        self.tpool.execute(move || {
            peer_manager.run(peer_id);
        });

        let peer_event_tx = self.peer_event.clone();
        self.tpool.execute(move || {
            while let Ok((data, addr)) = data_rx.recv() {
                peer_event_tx.publish(
                    consts::TOPIC_PEER_EVENT,
                    PeerEvent::SocketData {
                        addr: addr,
                        data: data,
                    },
                );
            }
        });

        let piece_manager = self.piece_manager.clone();
        let rx = self.piece_event.subscribe(consts::TOPIC_PIECE_EVENT);
        self.tpool.execute(move || {
            while let Ok(ev) = rx.recv() {
                let mut piece_manager = piece_manager.write().unwrap();
                piece_manager.handle_event(ev);
            }
        });
    }

    fn start_announce_thread(&self) {
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let state = self.state.clone();
        let shutdown = self.shutdown.clone();
        let announce_manager = self.announce_manager.clone();
        let condvar = self.shutdown_condvar.clone();
        let peer_event_tx = self.peer_event.clone();
        self.tpool.execute(move || {
            while !shutdown.load(Ordering::Relaxed) {
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
                            let guard = announce_manager.lock().unwrap();
                            let _ = condvar.wait_timeout(guard, next_time).unwrap();
                            continue;
                        }
                    }
                };
                tracker.announced();
                let (uploaded, downloaded, left) = {
                    let state = state.lock().unwrap();
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
                    peer_event_tx.publish(
                        consts::TOPIC_PEER_EVENT,
                        PeerEvent::NewPeers {
                            peers: response.peers,
                        },
                    );
                }
            }
        });
    }

    /// Blocks the current thread until the torrent download completes
    /// or the client is shut down.
    ///
    /// Uses a condition variable to efficiently wait for progress updates.
    pub fn wait_until_complete(&self) {
        let mut state = self.state.lock().unwrap();
        while !self.shutdown.load(Ordering::Relaxed) && state.left > 0 {
            state = self.state_condvar.wait(state).unwrap();
        }
    }

    pub fn is_complete(&self) -> bool {
        let piece_manager = self.piece_manager.read().unwrap();
        piece_manager.is_complete()
    }

    /// Stops the torrent client and notifies all waiting threads.
    ///
    /// Sets the shutdown flag and signals any condition variables
    /// waiting for completion.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.shutdown_condvar.notify_all();
    }

    pub fn get_state(&self) -> TorrentState {
        self.state.lock().unwrap().clone()
    }

    pub fn pieces_left(&self) -> usize {
        let piece_manager = self.piece_manager.read().unwrap();
        piece_manager.pieces_left()
    }

    pub fn total_pieces(&self) -> usize {
        self.torrent.info.piece_hashes.len()
    }

    pub fn pieces_verified(&self) -> usize {
        let piece_manager = self.piece_manager.read().unwrap();
        piece_manager.pieces_verified()
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
