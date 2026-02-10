use rutor::bytespeed::ByteSpeed;
use rutor::engine::{Engine, Event, TorrentStats};
use rutor::{picker, torrent};
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};

struct ProgressTracker {
    name: String,
    total_size: u64,
    eta: f64,
    total_pieces: usize,
    cpu_usage: f32,
    mem_usage_kb: u64,
    pid: Pid,
    show_consumption: bool,
    stats: TorrentStats,
    speed: ByteSpeed,
    last_downloaded: u64,
}

impl ProgressTracker {
    fn new(
        name: &str,
        total_size: u64,
        total_pieces: usize,
        pid: Pid,
        show_consumption: bool,
        stats: TorrentStats,
    ) -> Self {
        Self {
            name: name.to_string(),
            total_size,
            eta: f64::INFINITY,
            total_pieces,
            cpu_usage: 0.0,
            mem_usage_kb: 0,
            pid,
            show_consumption,
            stats,
            speed: ByteSpeed::new(Duration::from_secs(1), false, 0.1),
            last_downloaded: 0,
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
        let percent = (progress * 100.0).floor() as usize;
        let bar_width = get_bar_width();
        let filled_blocks = (progress * bar_width as f64).floor() as usize;
        let remainder = progress * bar_width as f64 - filled_blocks as f64;

        let partial_block = if remainder >= 0.875 {
            "●" // full
        } else if remainder >= 0.625 {
            "◕" // mostly filled
        } else if remainder >= 0.375 {
            "◑" // half filled
        } else if remainder >= 0.125 {
            "◔" // slightly filled
        } else {
            "" // empty
        };

        let empty_blocks = bar_width
            .saturating_sub(filled_blocks)
            .saturating_sub(if partial_block.is_empty() { 0 } else { 1 });

        let green = "\x1b[32m"; // filled
        let yellow = "\x1b[33m"; // partial
        let gray = "\x1b[37m"; // empty
        let reset = "\x1b[0m";

        let filled_str = format!("{}{}{}", green, "●".repeat(filled_blocks), reset);
        let partial_str = format!("{}{}{}", yellow, partial_block, reset);
        let empty_str = format!("{}{}{}", gray, "○".repeat(empty_blocks), reset);

        // Rounded/circular brackets
        format!(
            "[{}{}{}] {:>3}%",
            filled_str, partial_str, empty_str, percent
        )
    }

    fn format_eta(&self, seconds: f64) -> String {
        if seconds.is_infinite() || seconds <= 0.0 {
            return "--:--:--".to_string();
        }
        let hours = (seconds / 3600.0).floor() as u64;
        let minutes = ((seconds % 3600.0) / 60.0).floor() as u64;
        let secs = (seconds % 60.0).floor() as u64;
        format!("{:02}:{:02}:{:02}", hours, minutes, secs)
    }

    fn update(&mut self, stats: TorrentStats, system: &System) {
        let delta_downloaded = (stats.downloaded - self.last_downloaded) as usize;
        self.last_downloaded = stats.downloaded;
        self.speed.update(delta_downloaded);
        let remaining = self.total_size.saturating_sub(stats.downloaded) as f64;
        self.eta = if self.speed.avg_speed > 0.0 {
            remaining / self.speed.avg_speed
        } else {
            f64::INFINITY
        };

        if let Some(process) = system.process(self.pid) {
            self.cpu_usage = process.cpu_usage();
            self.mem_usage_kb = process.memory();
        }
        self.stats = stats;
    }

    fn display(&self, first_draw: bool) {
        if !first_draw {
            let mut max = if self.show_consumption { 6 } else { 5 };
            if self.stats.files.len() > 1 {
                max += self.stats.files.len();
            }
            for _ in 0..max {
                print!("\r\x1B[1A\x1b[2K");
            }
        }

        let cyan = "\x1b[36m";
        let green = "\x1b[32m";
        let yellow = "\x1b[33m";
        let magenta = "\x1b[35m";
        let blue = "\x1b[34m";
        let gray = "\x1b[90m";
        let white = "\x1b[97m";
        let reset = "\x1b[0m";

        if self.stats.files.len() == 1 {
            println!("📦 {}{}{}", cyan, self.name, reset);
        } else {
            println!("📦 {}{}{}", cyan, self.name, reset);
            for (path, written, total_size) in &self.stats.files {
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("[unknown]");
                println!(
                    "  {}• {}{} {} / {}",
                    gray,
                    file_name,
                    reset,
                    self.human_bytes(*written),
                    self.human_bytes(*total_size)
                );
            }
        }
        println!(
            "{0}{1:<10}:{2} {3} / {4} @ {5}{6}/s{7} ETA: {8}{9}{10}",
            green,
            "Downloaded",
            reset,
            self.human_bytes(self.stats.downloaded),
            self.human_bytes(self.total_size),
            yellow,
            self.human_bytes(self.speed.avg_speed as u64),
            reset,
            self.format_eta(self.eta),
            reset,
            magenta,
        );
        println!(
            "{0}{1:<10}:{2} {3} / {4} (C) ({5} available)",
            blue,
            "Peers",
            reset,
            self.stats.peers,
            self.stats.max_peers,
            self.stats.available_peers
        );
        println!(
            "{0}{1:<10}:{2} {3} / {4} ({5} blocks in flight ~{6})",
            white,
            "Pieces",
            reset,
            self.stats.bitfield.count_ones(),
            self.total_pieces,
            self.stats.blocks_inflight,
            self.human_bytes((self.stats.blocks_inflight * picker::BLOCK_SIZE) as u64)
        );
        println!(
            "{}",
            self.format_bar(self.stats.downloaded.min(self.total_size))
        );
        if self.show_consumption {
            println!(
                "CPU: {:.1}% | Memory: {}",
                self.cpu_usage,
                self.human_bytes(self.mem_usage_kb),
            );
        }
    }

    fn update_and_display(&mut self, stats: TorrentStats, system: &System, first_draw: bool) {
        self.update(stats, system);
        self.display(first_draw);
    }
}

fn get_terminal_width() -> usize {
    use libc::{STDOUT_FILENO, TIOCGWINSZ, ioctl, winsize};
    use std::mem::zeroed;

    unsafe {
        let mut ws: winsize = zeroed();
        if ioctl(STDOUT_FILENO, TIOCGWINSZ, &mut ws) == 0 {
            ws.ws_col as usize
        } else {
            40
        }
    }
}

fn get_bar_width() -> usize {
    get_terminal_width() / 2
}

fn print_usage_header<W: Write>(writer: &mut W, prog: &str) {
    let _ = writeln!(writer, "Usage: {} [OPTIONS...] <torrent-file>", prog);
}

fn print_usage<W: Write>(writer: &mut W, prog: &str) {
    print_usage_header(writer, prog);

    let mut usage = String::from("\n");
    usage.push_str("OPTIONS:\n");
    usage.push_str(
        "  -d/--destination    destination folder of where the torrent should be downloaded to\n",
    );
    usage.push_str("  -c/--consumption    shows cpu and memory consumption used by the client\n");
    usage.push_str("  -h/--help           shows this help message and exits");

    let _ = writeln!(writer, "{}", usage);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().collect::<Vec<String>>();
    let program = Path::new(&args[0])
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    let mut filename = String::new();
    let mut destination: Option<PathBuf> = None;
    let mut show_consumption = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-d" | "--destination" => {
                i += 1;
                if i >= args.len() {
                    return Err("destination path not provided".into());
                }
                destination = Some(PathBuf::from(&args[i]));
            }
            "-h" | "--help" => {
                let mut stdout = std::io::stdout();
                print_usage(&mut stdout, &program);
                std::process::exit(0);
            }
            "-c" | "--consumption" => {
                show_consumption = true;
            }
            arg if filename.is_empty() => {
                filename = arg.to_string();
            }
            _ => {
                let mut stderr = std::io::stderr();
                print_usage_header(&mut stderr, &program);
                return Err(format!("unknown argument '{}'", args[i]).into());
            }
        }
        i += 1;
    }

    if filename.is_empty() {
        let mut stderr = std::io::stderr();
        print_usage_header(&mut stderr, &program);
        return Err("missing torrent file".into());
    }

    let pid = sysinfo::get_current_pid()?;
    let mut system = System::new_all();
    let torrent: torrent::Torrent = {
        let file = File::open(filename)?;
        torrent::Torrent::from_file(&file, destination)?
    };
    let name = torrent.info.name.clone();
    let total_size = torrent.info.total_size;
    let total_pieces = torrent.info.piece_hashes.len();
    let (event_tx, event_rx) = mpsc::channel();
    let (io_tx, io_rx) = mpsc::channel();
    let (peer_io_tx, peer_io_rx) = mpsc::channel();
    let (announce_io_tx, announce_io_rx) = mpsc::channel();
    let (hash_tx, hash_rx) = mpsc::channel();
    let mut engine = Engine::new(
        torrent,
        event_tx.clone(),
        event_rx,
        io_tx,
        peer_io_tx,
        announce_io_tx,
        0,
        0,
        hash_tx,
    );
    let mut progress_tracker = ProgressTracker::new(
        &name,
        total_size,
        total_pieces,
        pid,
        show_consumption,
        engine.get_stats(),
    );
    let mut first_draw = true;
    let mut last = Instant::now();
    engine.start(io_rx, peer_io_rx, announce_io_rx, hash_rx);
    loop {
        if last.elapsed() >= Duration::from_secs(1) {
            system.refresh_pids(&vec![pid]);
            progress_tracker.update_and_display(engine.get_stats(), &system, first_draw);
            first_draw = false;
            last = Instant::now();
        }
        let ev = engine.poll()?;
        match ev {
            Event::Terminate => {
                engine.handle_event(ev)?;
                break;
            }
            _ => engine.handle_event(ev)?,
        }
    }
    engine.join();

    Ok(())
}
