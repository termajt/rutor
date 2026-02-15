use flexi_logger::{Age, Cleanup, Criterion, DeferredNow, FileSpec, Logger, Naming};
use log::{LevelFilter, Record};
use rutor::bytespeed::ByteSpeed;
use rutor::engine::{Engine, EngineStats, Tick, duration_to_ticks};
use rutor::magnet::MagnetLink;
use rutor::net::MAX_CONNECTIONS;
use rutor::torrent::Torrent;
use rutor::{BLOCK_SIZE, torrent};
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use sysinfo::{Pid, System};

#[derive(Debug)]
enum InputSource {
    TorrentFile(PathBuf),
    Magnet(MagnetLink),
}

struct ProgressTracker {
    name: String,
    total_size: u64,
    eta: f64,
    total_pieces: usize,
    cpu_usage: f32,
    mem_usage_kb: u64,
    pid: Pid,
    show_consumption: bool,
    stats: EngineStats,
    speed: ByteSpeed,
    last_downloaded: u64,
    peak_cpu: f32,
    peak_mem_usage_kb: u64,
    min_cpu: Option<f32>,
    min_mem_usage_kb: Option<u64>,
}

impl ProgressTracker {
    fn new(
        name: &str,
        total_size: u64,
        total_pieces: usize,
        pid: Pid,
        show_consumption: bool,
        stats: EngineStats,
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
            peak_cpu: 0.0,
            peak_mem_usage_kb: 0,
            min_cpu: None,
            min_mem_usage_kb: None,
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

    fn update(&mut self, stats: EngineStats, system: &System) {
        let delta_downloaded = stats.downloaded_bytes.saturating_sub(self.last_downloaded) as usize;
        self.last_downloaded = stats.downloaded_bytes;
        self.speed.update(delta_downloaded);
        let remaining = self.total_size.saturating_sub(stats.downloaded_bytes) as f64;
        self.eta = if self.speed.avg_speed > 0.0 {
            remaining / self.speed.avg_speed
        } else {
            f64::INFINITY
        };

        if self.show_consumption {
            if let Some(process) = system.process(self.pid) {
                self.cpu_usage = process.cpu_usage();
                self.mem_usage_kb = process.memory();
                self.peak_cpu = self.peak_cpu.max(self.cpu_usage);
                self.peak_mem_usage_kb = self.peak_mem_usage_kb.max(self.mem_usage_kb);
                if let Some(v) = &mut self.min_cpu {
                    *v = v.min(self.cpu_usage);
                } else {
                    self.min_cpu = Some(self.cpu_usage);
                }
                if let Some(v) = &mut self.min_mem_usage_kb {
                    *v = std::cmp::min(*v, self.mem_usage_kb);
                } else {
                    self.min_mem_usage_kb = Some(self.mem_usage_kb);
                }
            }
        }
        self.stats = stats;
    }

    fn display(&self, first_draw: bool) {
        if !first_draw {
            let mut max = if self.show_consumption { 7 } else { 6 };
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
            for (name, _, verified, total_size) in &self.stats.files {
                println!(
                    "  {}• {}{} {} / {}",
                    gray,
                    name,
                    reset,
                    self.human_bytes(*verified),
                    self.human_bytes(*total_size)
                );
            }
        }
        println!(
            "{0}{1:<10}:{2} {3} / {4} ({5} remaining)",
            green,
            "Downloaded",
            reset,
            self.human_bytes(self.stats.downloaded_bytes),
            self.human_bytes(self.total_size),
            self.human_bytes(self.stats.left),
        );
        println!(
            "{0:<10}: {1}{2}{3}, ETA: {4}{5}{6}",
            "Speed",
            yellow,
            self.human_bytes(self.speed.avg_speed as u64),
            reset,
            magenta,
            self.format_eta(self.eta),
            reset
        );
        println!(
            "{0}{1:<10}:{2} {3} / {4} (C)",
            blue, "Peers", reset, self.stats.peers, MAX_CONNECTIONS
        );
        println!(
            "{0}{1:<10}:{2} {3} / {4}, ({5} inflight blocks ~{6})",
            white,
            "Pieces",
            reset,
            self.stats.verified_pieces,
            self.total_pieces,
            self.stats.inflight_blocks,
            self.human_bytes((self.stats.inflight_blocks * BLOCK_SIZE) as u64)
        );
        println!(
            "{}",
            self.format_bar(self.stats.downloaded_bytes.min(self.total_size))
        );
        if self.show_consumption {
            println!(
                "CPU: {:.1}% | Memory: {}",
                self.cpu_usage,
                self.human_bytes(self.mem_usage_kb),
            );
        }
    }

    fn display_peak(&self) {
        let peak_cpu_str = format!("{:.1}%", self.peak_cpu);
        let peak_mem_str = self.human_bytes(self.peak_mem_usage_kb);

        let min_cpu_str = format!("{:.1}%", self.min_cpu.unwrap_or(0.0));
        let min_mem_str = self.human_bytes(self.min_mem_usage_kb.unwrap_or(0));

        let cpu_width = peak_cpu_str.len().max(min_cpu_str.len()).max("CPU".len());
        let mem_width = peak_mem_str
            .len()
            .max(min_mem_str.len())
            .max("Memory".len());

        let total_width = cpu_width + mem_width + 7;
        // 7 = 3 for dividers | | and 4 for spaces/padding

        let border = format!("+{}+", "-".repeat(total_width - 2));

        println!("{}", border);
        println!(
            "| {:^cpu_width$} | {:^mem_width$} |",
            "CPU",
            "Memory",
            cpu_width = cpu_width,
            mem_width = mem_width
        );
        println!("{}", border);
        println!(
            "| {:>cpu_width$} | {:>mem_width$} |",
            peak_cpu_str,
            peak_mem_str,
            cpu_width = cpu_width,
            mem_width = mem_width
        );
        println!(
            "| {:>cpu_width$} | {:>mem_width$} |",
            min_cpu_str,
            min_mem_str,
            cpu_width = cpu_width,
            mem_width = mem_width
        );
        println!("{}", border);
    }

    fn update_and_display(&mut self, stats: EngineStats, system: &System, first_draw: bool) {
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

    let options = [
        ("-d/--destination", "Destination folder for the torrent"),
        ("-c/--consumption", "Show CPU and memory usage"),
        (
            "-r/--max-read",
            "Maximum read bytes per second (e.g., 1024, 1MB, 2.5MB)",
        ),
        (
            "-w/--max-write",
            "Maximum write bytes per second (e.g., 1024, 1MB, 2.5MB)",
        ),
        (
            "-v",
            "Set verbosity level:\n\
               -v     Errors only (default)\n\
               -vv    Warnings and errors\n\
               -vvv   Info, warnings, and errors\n\
               -vvvv  Debug, info, warnings, and errors",
        ),
        ("-h/--help", "Show this help message and exit"),
    ];

    // Print header
    let _ = writeln!(writer, "\nOPTIONS:");

    // Compute max flag width for alignment
    let max_flag_width = options
        .iter()
        .map(|(flag, _)| flag.len())
        .max()
        .unwrap_or(0);

    for (flag, desc) in &options {
        let lines: Vec<&str> = desc.split('\n').collect();
        // First line
        let first_line = format!(
            "  {:flag_width$}   {}",
            flag,
            lines[0],
            flag_width = max_flag_width
        );
        let _ = writeln!(writer, "{}", first_line);

        // Remaining lines
        for line in &lines[1..] {
            let _ = writeln!(
                writer,
                "  {:flag_width$}   {}",
                "",
                line,
                flag_width = max_flag_width
            );
        }
    }
}

fn parse_bytes(s: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty string".into());
    }

    let mut chars = s.chars();
    let mut number_str = String::new();
    while let Some(c) = chars.next() {
        if c.is_digit(10) || c == '.' {
            number_str.push(c);
        } else {
            let unit = format!("{}{}", c, chars.as_str()).to_uppercase();
            let number: f64 = number_str.parse()?;
            return Ok(match unit.as_str() {
                "B" => number as usize,
                "KB" => (number * 1024.0) as usize,
                "MB" => (number * 1024.0 * 1024.0) as usize,
                "GB" => (number * 1024.0 * 1024.0 * 1024.0) as usize,
                "" => number as usize,
                _ => return Err(format!("unknown unit '{}'", unit).into()),
            });
        }
    }

    let number: f64 = number_str.parse()?;
    Ok(number as usize)
}

fn parse_verbosity(verbosity_level: usize) -> LevelFilter {
    match verbosity_level {
        0 | 1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        _ => LevelFilter::Debug,
    }
}

fn log_format(w: &mut dyn Write, now: &mut DeferredNow, record: &Record) -> std::io::Result<()> {
    write!(
        w,
        "{} [{}] [{}:{}] {}",
        now.format_rfc3339(),
        record.level(),
        //record.module_path().unwrap_or("unknown"),
        record.file().unwrap_or("unknown"),
        record.line().unwrap_or(0),
        &record.args()
    )
}

fn init_logger(level: LevelFilter) -> Result<(), Box<dyn std::error::Error>> {
    Logger::try_with_env_or_str(level.to_string())?
        .log_to_file(
            FileSpec::default()
                .directory("logs")
                .basename("rutor")
                .suffix("log"),
        )
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Numbers,
            Cleanup::KeepLogFiles(5),
        )
        .format(log_format)
        .start()?;

    Ok(())
}

fn detect_input(s: &str) -> Result<InputSource, Box<dyn std::error::Error>> {
    if s.starts_with("magnet:?") {
        return Ok(InputSource::Magnet(MagnetLink::parse(s)?));
    }

    let path = Path::new(s);

    if path.exists() && path.is_file() {
        Ok(InputSource::TorrentFile(path.to_path_buf()))
    } else {
        Err(format!("'{}' is neither a magnet link nor a torrent file", s).into())
    }
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
    let mut max_read_bytes_per_sec = 0;
    let mut max_write_bytes_per_sec = 0;
    let mut verbosity_level = 0;
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
            "-r" | "--max-read" => {
                i += 1;
                if i >= args.len() {
                    return Err("max-read value not provided".into());
                }
                match parse_bytes(&args[i]) {
                    Ok(n) => max_read_bytes_per_sec = n,
                    Err(e) => {
                        eprintln!("failed to parse '{}' as max read bytes: {}", args[i], e);
                        return Err(e);
                    }
                }
            }
            "-w" | "--max-write" => {
                i += 1;
                if i >= args.len() {
                    return Err("max-write value not provided".into());
                }
                match parse_bytes(&args[i]) {
                    Ok(n) => max_write_bytes_per_sec = n,
                    Err(e) => {
                        eprintln!("failed to parse '{}' as max write bytes: {}", args[i], e);
                        return Err(e);
                    }
                }
            }
            arg if arg.starts_with("-v") => {
                verbosity_level = arg.chars().skip(1).filter(|&c| c == 'v').count();
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
        print_usage_header(&mut std::io::stderr(), &program);
        return Err("missing torrent file or magnet link".into());
    }

    let log_level = parse_verbosity(verbosity_level);
    init_logger(log_level)?;

    let input_source = detect_input(&filename)?;
    let mut torrent: Torrent;
    match input_source {
        InputSource::TorrentFile(path) => {
            torrent = {
                let file = File::open(path)?;
                torrent::Torrent::from_file(&file, destination)?
            };
        }
        InputSource::Magnet(magnet) => {
            println!("Fetching torrent metadata...");
            let torrent_bencode = magnet.fetch()?;
            torrent = torrent::Torrent::from_bencode(torrent_bencode, destination)?;
            torrent.add_trackers(magnet.trackers());
            println!("OK!");
        }
    }

    start_engine(
        torrent,
        max_read_bytes_per_sec,
        max_write_bytes_per_sec,
        show_consumption,
    )?;

    Ok(())
}

fn start_engine(
    torrent: Torrent,
    max_read_bytes_per_sec: usize,
    max_write_bytes_per_sec: usize,
    show_consumption: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let pid = sysinfo::get_current_pid()?;
    let mut system = System::new_all();
    let total_pieces = torrent.info.piece_hashes.len();
    let total_size = torrent.info.total_size;
    let name = torrent.info.name.clone();
    let mut engine = Engine::new(torrent, max_read_bytes_per_sec, max_write_bytes_per_sec)?;
    let mut progress_tracker = ProgressTracker::new(
        &name,
        total_size,
        total_pieces,
        pid,
        show_consumption,
        engine.get_stats(),
    );
    let progress_interval = duration_to_ticks(Duration::from_secs(1));
    let mut first_draw = true;
    log::debug!("Starting engine");
    println!("Starting download...");
    engine.start(|now, stats| {
        if now % progress_interval == Tick(0) {
            if show_consumption {
                system.refresh_pids(&vec![pid]);
            }
            progress_tracker.update_and_display(stats, &system, first_draw);
            first_draw = false;
        }
    });
    log::debug!("Stopping engine");
    engine.stop();
    log::debug!("Joining engine");
    engine.join();
    progress_tracker.display_peak();
    println!("Download complete!");

    Ok(())
}
