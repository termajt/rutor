use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use rand::{Rng, distr::Alphanumeric};

use crate::{
    announce::{self, AnnounceManager},
    pool::ThreadPool,
    torrent::Torrent,
};

/// Represents the current state of a torrent download.
///
/// Tracks total and current transfer statistics, as well as
/// timing and progress information. Used for reporting and coordination
/// between threads in the [`TorrentClient`].
#[derive(Debug)]
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
    tpool: ThreadPool,
    peer_id: [u8; 20],
    state_condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    shutdown_condvar: Arc<Condvar>,
}

impl TorrentClient {
    /// Creates a new [`TorrentClient`] for the given torrent and listening port.
    ///
    /// Initializes shared state, thread pool, and generates a unique peer ID.
    pub fn new(torrent: Torrent, port: u16) -> Self {
        let total_size = torrent.info.total_size;
        let peer_id = generate_peer_id();
        Self {
            torrent: Arc::new(torrent),
            state: Arc::new(Mutex::new(TorrentState::new(total_size, port))),
            tpool: ThreadPool::new(),
            peer_id: peer_id,
            state_condvar: Arc::new(Condvar::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_condvar: Arc::new(Condvar::new()),
        }
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
        let info_hash = self.torrent.info_hash;
        let peer_id = self.peer_id;
        let state = self.state.clone();
        let torrent = self.torrent.clone();
        let shutdown_condvar = self.shutdown_condvar.clone();
        let shutdown = self.shutdown.clone();
        self.tpool.execute(move || {
            announce_thread(
                torrent,
                &info_hash,
                &peer_id,
                state,
                shutdown_condvar,
                shutdown,
            );
        });
        Ok(())
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

    /// Stops the torrent client and notifies all waiting threads.
    ///
    /// Sets the shutdown flag and signals any condition variables
    /// waiting for completion.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.shutdown_condvar.notify_all();
    }
}

fn announce_thread(
    torrent: Arc<Torrent>,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
    state: Arc<Mutex<TorrentState>>,
    condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
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
