# rutor

> A minimal BitTorrent client written in Rust — for learning and experimentation.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/Made%20with-Rust-orange)](https://www.rust-lang.org/)

This is a **learning project** in Rust: a simple torrent client.

It is meant for educational purposes, to understand how the BitTorrent protocol works.

## Usage

### Using Cargo

Run the client directly with Cargo:

```bash
cargo run -- <torrent-file>
```

Replace `<torrent-file>` with the path to the `.torrent` file you want to download.

### Using the built executable

After building the project, you can run the compiled binary:

```bash
cargo build --release
./target/release/rutor <torrent-file>
```

This will start downloading the torrent and display a progress bar with download statistics.

### Additional help

For additional help, run with `-h/--help` flag:

```bash
cargo run -- -h
Usage: rutor [OPTIONS...] <torrent-file>

OPTIONS:
  -d/--destination   Destination folder for the torrent
  -c/--consumption   Show CPU and memory usage
  -r/--max-read      Maximum read bytes per second (e.g., 1024, 1MB, 2.5MB)
  -w/--max-write     Maximum write bytes per second (e.g., 1024, 1MB, 2.5MB)
  -v                 Set verbosity level:
                     -v     Errors only (default)
                     -vv    Warnings and errors
                     -vvv   Info, warnings, and errors
                     -vvvv  Debug, info, warnings, and errors
  -h/--help          Show this help message and exit
```

## Dependencies

This project uses a few well-established Rust crates:

| Crate | Purpose | Link |
|-------|---------|------|
| [`libc`](https://crates.io/crates/libc) | Low-level system bindings for cross-platform support | [Docs](https://docs.rs/libc) |
| [`sha1`](https://crates.io/crates/sha1) | Torrent piece hash verification | [Docs](https://docs.rs/sha1) |
| [`reqwest`](https://crates.io/crates/reqwest) | HTTP client for tracker communication | [Docs](https://docs.rs/reqwest) |
| [`rand`](https://crates.io/crates/rand) | Random peer IDs and session identifiers | [Docs](https://docs.rs/rand) |
| [`sysinfo`](https://crates.io/crates/sysinfo) | System stats for CPU/memory/thread usage in the UI | [Docs](https://docs.rs/sysinfo) |
| [`mio`](https://crates.io/crates/mio) | Low-level async IO for efficient peer connections | [Docs](https://docs.rs/mio/latest/mio) |
| [`crossbeam`](https://crates.io/crates/crossbeam) | Lock-free concurrency primitives and channels | [Docs](https://docs.rs/crossbeam) |
| [`bytes`](https://crates.io/crates/bytes) | Efficient byte buffers for network IO | [Docs](https://docs.rs/bytes) |
| [`flexi_logger`](https://crates.io/crates/flexi_logger) | Logging with rotation, file output, and configurable verbosity | [Docs](https://docs.rs/flexi_logger) |
| [`log`](https://crates.io/crates/log) | Standard logging facade for Rust | [Docs](https://docs.rs/log) |

## Notes

- This project is **not production-ready**. It is designed to learn Rust and how torrents work.
- Feel free to experiment with it, add features or extend it.

## License

Licensed under the [MIT License](./LICENSE)
