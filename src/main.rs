use rutor::announce::{self, AnnounceManager};
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
    let mut manager = AnnounceManager::new(&torrent);
    const PEER_ID: [u8; 20] = *b"-RS1000-abcdefghij12";

    loop {
        if let Some(tracker) = manager.next_due_tracker() {
            println!("Announcing to {}", tracker.url);
            tracker.announced();
            match announce::announce(
                &tracker.url,
                &torrent.info_hash,
                &PEER_ID,
                6881,
                0,
                0,
                torrent.info.total_size,
                None,
                None,
            ) {
                Ok(resp) => {
                    println!("Got {} peers", resp.peers.len());
                    tracker.update_from_response(&resp);
                }
                Err(e) => {
                    eprintln!("Tracker failed: {e}");
                }
            }
        } else {
            let next_time = manager
                .tiers
                .iter()
                .flat_map(|tier| tier.iter().map(|t| t.time_until_next()))
                .min()
                .unwrap_or(Duration::from_secs(30));
            println!("Sleeping {:?} until next announce...", next_time);
            std::thread::sleep(next_time);
        }
    }
}
