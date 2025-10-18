use rutor::bytespeed::ByteSpeed;
use rutor::client::{TorrentClient, TorrentState};
use rutor::torrent;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use sysinfo::{Pid, System};

struct ProgressTracker {
    name: String,
    total_size: u64,
    prev_downloaded: u64,
    eta: f64,
    connected_peers: usize,
    all_peers: usize,
    pieces_verified: usize,
    total_pieces: usize,
    speed: ByteSpeed,
    cpu_usage: f32,
    mem_usage_kb: u64,
    pid: Pid,
    show_consumption: bool,
}

impl ProgressTracker {
    fn new(
        name: &str,
        total_size: u64,
        total_pieces: usize,
        pid: Pid,
        show_consumption: bool,
    ) -> Self {
        Self {
            name: name.to_string(),
            total_size: total_size,
            prev_downloaded: 0,
            eta: f64::INFINITY,
            connected_peers: 0,
            all_peers: 0,
            pieces_verified: 0,
            total_pieces: total_pieces,
            speed: ByteSpeed::new(Duration::from_secs(7), Duration::from_secs(1)),
            cpu_usage: 0.0,
            mem_usage_kb: 0,
            pid: pid,
            show_consumption: show_consumption,
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

        let empty_blocks = bar_width - filled_blocks - if partial_block.is_empty() { 0 } else { 1 };

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
            filled_str,
            partial_str,
            empty_str,
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

    fn update(&mut self, state: &TorrentState, pieces_verified: usize, system: &System) {
        self.speed
            .update(state.downloaded.abs_diff(self.prev_downloaded) as usize);

        let remaining = self.total_size.saturating_sub(state.downloaded) as f64;
        self.eta = if self.speed.avg > 0.0 {
            remaining / self.speed.avg
        } else {
            f64::INFINITY
        };
        self.prev_downloaded = state.downloaded;
        self.connected_peers = state.connected_peers;
        self.all_peers = state.peers;
        self.pieces_verified = pieces_verified;
        if let Some(process) = system.process(self.pid) {
            self.cpu_usage = process.cpu_usage();
            self.mem_usage_kb = process.memory();
        }
    }

    fn display(&self, first_draw: bool) {
        if !first_draw {
            let max = if self.show_consumption { 6 } else { 5 };
            for _ in 0..max {
                print!("\r\x1B[1A\x1b[2K");
            }
        }

        let cyan = "\x1b[36m";
        let green = "\x1b[32m";
        let yellow = "\x1b[33m";
        let magenta = "\x1b[35m";
        let blue = "\x1b[34m";
        let white = "\x1b[97m";
        let reset = "\x1b[0m";

        println!("{}Name:{} {}", cyan, reset, self.name);
        println!(
            "{}Downloaded:{} {} / {} at {}{}/s{}, ETA: {}{}{}",
            green,
            reset,
            self.human_bytes(self.prev_downloaded),
            self.human_bytes(self.total_size),
            yellow,
            self.human_bytes(self.speed.avg as u64),
            reset,
            magenta,
            self.format_eta(self.eta),
            reset
        );
        println!(
            "{}Peers:{} {} (C) / {} (A)",
            blue, reset, self.connected_peers, self.all_peers
        );
        println!(
            "{}Pieces:{} {} / {}",
            white, reset, self.pieces_verified, self.total_pieces
        );
        println!("{}", self.format_bar(self.prev_downloaded));
        if self.show_consumption {
            println!(
                "CPU: {:.1}% | Memory: {}",
                self.cpu_usage,
                self.human_bytes(self.mem_usage_kb)
            );
        }
    }

    fn update_and_display(
        &mut self,
        state: &TorrentState,
        pieces_verified: usize,
        system: &System,
        first_draw: bool,
    ) {
        self.update(state, pieces_verified, system);
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
                    eprintln!("ERROR: destination path not provided");
                    std::process::exit(1);
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
                eprintln!("ERROR: unknown argument '{}'", args[i]);
                let mut stderr = std::io::stderr();
                print_usage_header(&mut stderr, &program);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    if filename.is_empty() {
        eprintln!("ERROR: missing torrent file");
        let mut stderr = std::io::stderr();
        print_usage_header(&mut stderr, &program);
        std::process::exit(1);
    }

    let pid = sysinfo::get_current_pid()?;
    let mut system = System::new_all();

    let torrent: torrent::Torrent = {
        let file = File::open(filename)?;
        torrent::Torrent::from_file(&file, destination)?
    };
    let name = torrent.info.name.clone();
    let total_size = torrent.info.total_size;
    let client = TorrentClient::new(torrent)?;
    client.start()?;
    let mut first_draw = true;
    let mut progress_tracker = ProgressTracker::new(
        &name,
        total_size,
        client.total_pieces(),
        pid,
        show_consumption,
    );
    while !client.is_complete() {
        system.refresh_all();
        let state = client.get_state();
        progress_tracker.update_and_display(&state, client.pieces_verified(), &system, first_draw);
        first_draw = false;
        std::thread::sleep(Duration::from_secs(1));
    }
    client.stop();
    system.refresh_all();
    let state = client.get_state();
    progress_tracker.update_and_display(&state, client.pieces_verified(), &system, first_draw);
    println!("\nDownload complete!");
    Ok(())
}
