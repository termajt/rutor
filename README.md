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
  -d/--destination    destination folder of where the torrent should be downloaded to
  -c/--consumption    shows cpu and memory consumption used by the client
  -h/--help           shows this help message and exits
```

## Dependencies

This project uses a few well-established Rust crates:

| Crate | Purpose | Link |
|-------|----------|------|
| [`reqwest`](https://crates.io/crates/reqwest) | HTTP client for tracker communication | [Docs](https://docs.rs/reqwest) |
| [`sha1`](https://crates.io/crates/sha1) | Torrent piece hash verification | [Docs](https://docs.rs/sha1) |
| [`rand`](https://crates.io/crates/rand) | Random peer IDs and session identifiers | [Docs](https://docs.rs/rand) |
| [`sysinfo`](https://crates.io/crates/sysinfo) | System stats for CPU/memory/thread usage in the UI | [Docs](https://docs.rs/sysinfo) |
| [`threadpool`](https://crates.io/crates/threadpool) | Worker thread pool for concurrent peer handling | [Docs](https://docs.rs/threadpool) |
| [`libc`](https://crates.io/crates/libc) | Low-level system bindings (for cross-platform support) | [Docs](https://docs.rs/libc) |

## Notes

- This project is **not production-ready**. It is designed to learn Rust and how torrents work.
- Feel free to experiment with it, add features or extend it.

## License

Licensed under the [MIT License](./LICENSE)
