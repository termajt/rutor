use rutor::client::TorrentClient;
use rutor::torrent;
use std::env;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

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
    let client = TorrentClient::new(torrent, 6881)?;
    client.start()?;
    while !client.is_complete() {
        let state = client.get_state();
        println!(
            "downloaded (bytes): {}, uploaded (bytes): {}, left (bytes): {}, peers (C/A): {}/{}",
            state.downloaded, state.uploaded, state.left, state.connected_peers, state.peers
        );
        std::thread::sleep(Duration::from_secs(3600));
    }
    client.stop();
    Ok(())
}
