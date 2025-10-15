use rutor::client::{TorrentClient, TorrentState};
use rutor::torrent;
use std::collections::VecDeque;
use std::env;
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

struct ProgressTracker {
    name: String,
    total_size: u64,
    prev_downloaded: u64,
    prev_instant: Instant,
    bar_width: usize,
    speeds: VecDeque<f64>,
    max_samples: usize,
    eta: f64,
    avg_speed: f64,
    connected_peers: usize,
    all_peers: usize,
}

impl ProgressTracker {
    fn new(name: &str, total_size: u64, bar_width: usize, max_samples: usize) -> Self {
        Self {
            name: name.to_string(),
            total_size: total_size,
            prev_downloaded: 0,
            prev_instant: Instant::now(),
            bar_width: bar_width,
            speeds: VecDeque::with_capacity(max_samples),
            max_samples: max_samples,
            eta: f64::INFINITY,
            avg_speed: 0.0,
            connected_peers: 0,
            all_peers: 0,
        }
    }

    fn human_bytes(&self, bytes: u64) -> String {
        const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        format!("{:.2} {}", size, UNITS[unit_index])
    }

    fn format_bar(&self, downloaded: u64) -> String {
        let progress = downloaded as f64 / self.total_size as f64;
        let filled_blocks = (progress * self.bar_width as f64).floor() as usize;
        let remainder = progress * self.bar_width as f64 - filled_blocks as f64;

        let partial_block = if remainder >= 0.75 {
            "▓"
        } else if remainder >= 0.5 {
            "▒"
        } else if remainder >= 0.25 {
            "░"
        } else {
            ""
        };

        let empty_blocks =
            self.bar_width - filled_blocks - if partial_block.is_empty() { 0 } else { 1 };

        let reset = "\x1b[0m";
        let green_bg = "\x1b[42m";
        let white_gray_bg = "\x1b[100m";

        format!(
            "[{}{}{}{}{}{}] {:>3}%",
            green_bg,
            " ".repeat(filled_blocks),
            white_gray_bg,
            partial_block,
            "░".repeat(empty_blocks),
            reset,
            (progress * 100.0).round() as usize
        )
    }

    fn format_eta(&self, seconds: f64) -> String {
        if !seconds.is_finite() || seconds < 0.0 {
            return "--:--:--".to_string();
        }
        let hours = (seconds / 3600.0).floor() as u64;
        let minutes = ((seconds % 3600.0) / 60.0).floor() as u64;
        let secs = (seconds % 60.0).floor() as u64;
        format!("{:02}:{:02}:{:02}", hours, minutes, secs)
    }

    fn update(&mut self, state: &TorrentState) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.prev_instant).as_secs_f64();
        let instant_speeed = if elapsed > 0.0 {
            (state.downloaded - self.prev_downloaded) as f64 / elapsed
        } else {
            0.0
        };

        if self.speeds.len() + 1 > self.max_samples {
            self.speeds.pop_front();
        }
        self.speeds.push_back(instant_speeed);
        self.avg_speed = self.speeds.iter().sum::<f64>() / self.speeds.len() as f64;

        let remaining = self.total_size.saturating_sub(state.downloaded) as f64;
        self.eta = if self.avg_speed > 0.0 {
            remaining / self.avg_speed
        } else {
            f64::INFINITY
        };
        self.prev_downloaded = state.downloaded;
        self.prev_instant = now;
        self.connected_peers = state.connected_peers;
        self.all_peers = state.peers;
    }

    fn display(&self, first_draw: bool) {
        if !first_draw {
            for _ in 0..4 {
                print!("\r\x1B[1A\x1b[2K");
            }
        }

        let cyan = "\x1b[36m";
        let green = "\x1b[32m";
        let yellow = "\x1b[33m";
        let magenta = "\x1b[35m";
        let blue = "\x1b[34m";
        let reset = "\x1b[0m";

        println!("{}Name:{} {}", cyan, reset, self.name);
        println!(
            "{}Downloaded:{} {} / {} at {}{}/s{}, ETA: {}{}{}",
            green,
            reset,
            self.human_bytes(self.prev_downloaded),
            self.human_bytes(self.total_size),
            yellow,
            self.human_bytes(self.avg_speed as u64),
            reset,
            magenta,
            self.format_eta(self.eta),
            reset
        );
        println!(
            "{}Peers:{} {} (C) / {} (A)",
            blue, reset, self.connected_peers, self.all_peers
        );
        println!("{}", self.format_bar(self.prev_downloaded));
    }

    fn update_and_display(&mut self, state: &TorrentState, first_draw: bool) {
        self.update(state);
        self.display(first_draw);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().collect::<Vec<String>>();
    let program = Path::new(&args[0])
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    if args.len() < 2 {
        eprintln!("Usage: {} <filename>", program);
        std::process::exit(1);
    }

    let torrent: torrent::Torrent = {
        let file = File::open(&args[1])?;
        torrent::Torrent::from_file(&file)?
    };
    let name = torrent.info.name.clone();
    let total_size = torrent.info.total_size;
    let client = TorrentClient::new(torrent)?;
    client.start()?;
    let mut first_draw = true;
    let mut progress_tracker = ProgressTracker::new(&name, total_size, 40, 10);
    while !client.is_complete() {
        let state = client.get_state();
        progress_tracker.update_and_display(&state, first_draw);
        first_draw = false;
        std::thread::sleep(Duration::from_secs(1));
    }
    client.stop();
    let state = client.get_state();
    progress_tracker.update_and_display(&state, first_draw);
    println!("\nDownload complete!");
    Ok(())
}
