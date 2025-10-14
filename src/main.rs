use rutor::client::TorrentClient;
use rutor::torrent;
use std::env;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
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
    let client = TorrentClient::new(torrent, 6881)?;
    client.start()?;
    while !client.is_complete() {
        let state = client.get_state();
        println!("{}", name);
        println!(
            "downloaded: {}, uploaded (bytes): {}, left: {}, peers (C/A): {}/{}, pieces: {}/{}",
            human_bytes(state.downloaded),
            human_bytes(state.uploaded),
            human_bytes(state.left),
            state.connected_peers,
            state.peers,
            client.pieces_verified(),
            client.total_pieces()
        );
        std::thread::sleep(Duration::from_secs(1));
    }
    let state = client.get_state();
    println!("{}", name);
    println!(
        "downloaded: {}, uploaded (bytes): {}, left: {}, peers (C/A): {}/{}, pieces: {}/{}",
        human_bytes(state.downloaded),
        human_bytes(state.uploaded),
        human_bytes(state.left),
        state.connected_peers,
        state.peers,
        client.pieces_verified(),
        client.total_pieces()
    );
    client.stop();
    Ok(())
}
