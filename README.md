# rutor

This is a **learning project** in Rust: a simple torrent client.

It is meant for educational purposes, to understand how torrents work.

## Usage

### Using Cargo

You can run the client directly with Cargo:

```bash
cargo run -- <torrent-file>
```

Replace `<torrent-file>` with the path to the `.torrent` file you want to download.

### Using the built executable

After building the project, you can run the compiled binary:

```bash
./ruton <torrent-file>
```

This will start downloading the torrent and display a progress bar with download statistics.

## Notes

- This project is **not production-ready**. It is designed to learn Rust and how torrents work.
- Feel free to experiment with it, add features or extennd it.

## License

MIT [LICENSE](./LICENSE)
