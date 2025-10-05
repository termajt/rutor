use rutor::torrent;
use std::env;
use std::fs::File;
use std::path::Path;

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
    println!("{torrent:?}");

    Ok(())
}
