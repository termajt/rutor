use rutor::bytespeed::ByteSpeed;
use rutor::client::{DownloadInfo, TorrentClient};
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
    eta: f64,
    total_pieces: usize,
    download_speed: f64,
    cpu_usage: f32,
    mem_usage_kb: u64,
    pid: Pid,
    show_consumption: bool,
    thread_workers: usize,
    download_info: DownloadInfo,
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
        thread_workers: usize,
        download_info: DownloadInfo,
    ) -> Self {
        let last_downloaded = download_info.downloaded;
        Self {
            name: name.to_string(),
            total_size: total_size,
            eta: f64::INFINITY,
            total_pieces: total_pieces,
            download_speed: 0.0,
            cpu_usage: 0.0,
            mem_usage_kb: 0,
            pid: pid,
            show_consumption: show_consumption,
            thread_workers: thread_workers,
            download_info: download_info,
            speed: ByteSpeed::new(Duration::from_secs(20), Duration::from_secs(1)),
            last_downloaded: last_downloaded,
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

    fn update(&mut self, download_info: DownloadInfo, system: &System, thread_workers: usize) {
        let downloaded_now = download_info.downloaded;
        let delta = downloaded_now.saturating_sub(self.last_downloaded);

        self.speed.update(delta as usize);
        self.last_downloaded = downloaded_now;
        self.download_speed = self.speed.avg;

        let remaining = self.total_size.saturating_sub(downloaded_now) as f64;
        self.eta = if self.download_speed > 0.0 {
            remaining / self.download_speed
        } else {
            f64::INFINITY
        };

        self.thread_workers = thread_workers;
        if let Some(process) = system.process(self.pid) {
            self.cpu_usage = process.cpu_usage();
            self.mem_usage_kb = process.memory();
        }
        self.download_info = download_info;
    }

    fn display(&self, first_draw: bool) {
        if !first_draw {
            let mut max = if self.show_consumption { 6 } else { 5 };
            if self.download_info.files.len() > 1 {
                max += self.download_info.files.len();
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

        if self.download_info.files.len() == 1 {
            println!("📦 {}{}{}", cyan, self.name, reset);
        } else {
            println!("📦 {}{}{}", cyan, self.name, reset);
            for (path, written, total_size) in &self.download_info.files {
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
            self.human_bytes(self.download_info.downloaded),
            self.human_bytes(self.total_size),
            yellow,
            self.human_bytes(self.download_speed as u64),
            reset,
            self.format_eta(self.eta),
            reset,
            magenta,
        );
        println!(
            "{0}{1:<10}:{2} {3} (C) / {4} (A)",
            blue,
            "Peers",
            reset,
            self.download_info.connected_peers,
            self.download_info.available_peers
        );
        println!(
            "{0}{1:<10}:{2} {3} / {4}",
            white, "Pieces", reset, self.download_info.pieces_verified, self.total_pieces
        );
        println!("{}", self.format_bar(self.download_info.downloaded));
        if self.show_consumption {
            println!(
                "CPU: {:.1}% | Memory: {} | Threads: {}",
                self.cpu_usage,
                self.human_bytes(self.mem_usage_kb),
                self.thread_workers
            );
        }
    }

    fn update_and_display(
        &mut self,
        download_info: DownloadInfo,
        system: &System,
        thread_workers: usize,
        first_draw: bool,
    ) {
        self.update(download_info, system, thread_workers);
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
    let total_pieces = torrent.info.piece_hashes.len();
    let name = torrent.info.name.clone();
    let total_size = torrent.info.total_size;
    let client = TorrentClient::new(torrent)?;
    client.start()?;
    let mut first_draw = true;
    let mut progress_tracker = ProgressTracker::new(
        &name,
        total_size,
        total_pieces,
        pid,
        show_consumption,
        client.get_thread_worker_count(),
        client.file_status(),
    );
    while !client.is_complete() {
        system.refresh_all();
        progress_tracker.update_and_display(
            client.file_status(),
            &system,
            client.get_thread_worker_count(),
            first_draw,
        );
        first_draw = false;
        std::thread::sleep(Duration::from_secs(1));
    }
    system.refresh_all();
    progress_tracker.update_and_display(
        client.file_status(),
        &system,
        client.get_thread_worker_count(),
        first_draw,
    );
    client.stop();
    eprintln!("✅ Download complete!");
    println!("✅ Download complete!");
    client.join();
    eprintln!("main returning!");
    Ok(())
}
