use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use rand::{Rng, distr::Alphanumeric};

use crate::{
    announce::{self, AnnounceManager, TrackerResponse},
    peer::{PeerManager, PeerStatus},
    pool::ThreadPool,
    queue::{Queue, Receiver, Sender},
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
        let peer_manager = Arc::new(PeerManager::new(
            50,
            tpool.clone(),
            shutdown.clone(),
            torrent.clone(),
        )?);
        Ok(Self {
            torrent: torrent,
            state: Arc::new(Mutex::new(TorrentState::new(total_size, port))),
            tpool: tpool,
            peer_id: peer_id,
            state_condvar: Arc::new(Condvar::new()),
            shutdown: shutdown,
            shutdown_condvar: shutdown_condvar,
            peer_manager: peer_manager,
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
        let peer_rx = self.start_announce_thread();
        self.start_peer_manager_thread(peer_rx);
        Ok(())
    }

    fn start_announce_thread(&self) -> Receiver<TrackerResponse> {
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let state = self.state.clone();
        let torrent = self.torrent.clone();
        let shutdown_condvar = self.shutdown_condvar.clone();
        let shutdown = self.shutdown.clone();
        let (tx, rx) = Queue::new(None);
        self.tpool.execute(move || {
            announce_thread(
                torrent,
                &info_hash,
                &peer_id,
                state,
                shutdown_condvar,
                shutdown,
                tx,
            );
        });
        rx
    }

    fn start_peer_manager_thread(&self, peer_rx: Receiver<TrackerResponse>) {
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let shutdown = self.shutdown.clone();
        let state = self.state.clone();
        let peer_manager = self.peer_manager.clone();
        self.tpool.execute(move || {
            while !shutdown.load(Ordering::Relaxed) {
                match peer_rx.recv() {
                    Ok(response) => {
                        peer_manager.add_peers(&response.peers);
                        let mut state = state.lock().unwrap();
                        state.peers = peer_manager.peer_count();
                        state.connected_peers = peer_manager.connected_peers();
                    }
                    Err(_) => break,
                }
            }
        });
        let shutdown = self.shutdown.clone();
        let shutdown_condvar = self.shutdown_condvar.clone();
        let state = self.state.clone();
        let peer_manager = self.peer_manager.clone();
        self.tpool.execute(move || {
            while !shutdown.load(Ordering::Relaxed) {
                if let Err(e) = peer_manager.handle_connections() {
                    eprintln!("socket manager error: {e}");
                }
                peer_manager.connect_peers(info_hash, peer_id);
                while let Ok((addr, status)) = peer_manager
                    .connect_rx
                    .recv_timeout(Duration::from_millis(500))
                {
                    match status {
                        PeerStatus::Connected => {
                            peer_manager.mark_connected(addr);
                        }
                        PeerStatus::Failed => {
                            peer_manager.mark_failed(addr);
                        }
                        _ => {}
                    }
                    let mut state = state.lock().unwrap();
                    state.peers = peer_manager.peer_count();
                    state.connected_peers = peer_manager.connected_peers();
                }
                let mut state = state.lock().unwrap();
                let result = shutdown_condvar
                    .wait_timeout(state, Duration::from_millis(100))
                    .unwrap();
                state = result.0;
                state.peers = peer_manager.peer_count();
                state.connected_peers = peer_manager.connected_peers();
            }
        });
        let shutdown = self.shutdown.clone();
        let peer_manager = self.peer_manager.clone();
        self.tpool.execute(move || {
            while !shutdown.load(Ordering::Relaxed) {
                peer_manager.run_handle_data();
                peer_manager.run_parse_messages();
            }
        });
        let shutdown = self.shutdown.clone();
        let peer_manager = self.peer_manager.clone();
        self.tpool.execute(move || {
            while !shutdown.load(Ordering::Relaxed) {
                if let Ok((addr, message)) = peer_manager
                    .peer_msg_rx
                    .recv_timeout(Duration::from_millis(500))
                {
                    peer_manager.handle_message(addr, message);
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
        let state = self.state.lock().unwrap();
        state.left == 0
    }

    /// Stops the torrent client and notifies all waiting threads.
    ///
    /// Sets the shutdown flag and signals any condition variables
    /// waiting for completion.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.peer_manager.stop();
        self.shutdown_condvar.notify_all();
    }

    pub fn get_state(&self) -> TorrentState {
        self.state.lock().unwrap().clone()
    }
}

fn announce_thread(
    torrent: Arc<Torrent>,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    state: Arc<Mutex<TorrentState>>,
    condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    response_sender: Sender<TrackerResponse>,
) {
    let mut manager = AnnounceManager::new(&torrent);
    while !shutdown.load(Ordering::Relaxed) {
        let (uploaded, downloaded, left) = {
            let state = state.lock().unwrap();
            (state.uploaded, state.downloaded, state.left)
        };
        if let Some(tracker) = manager.next_due_tracker() {
            println!("Announcing to {}", tracker.url);
            tracker.announced();
            match announce::announce(
                &tracker.url,
                info_hash,
                peer_id,
                6881,
                uploaded,
                downloaded,
                left,
                None,
                None,
            ) {
                Ok(resp) => {
                    println!("Got {} peers from {}", resp.peers.len(), tracker.url);
                    tracker.update_from_response(&resp);
                    if let Err(e) = response_sender.send(resp) {
                        eprintln!("failed to send tracker response: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("Tracker {} failed: {e}", tracker.url);
                }
            }
        } else {
            let next_time = manager
                .tiers
                .iter()
                .flat_map(|tier| tier.iter().map(|t| t.time_until_next()))
                .min()
                .unwrap_or(Duration::from_secs(30));
            println!("Sleeping {:?} until next announce...", next_time);

            let guard = state.lock().unwrap();
            let (_unused, _unused1) = condvar.wait_timeout(guard, next_time).unwrap();
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
