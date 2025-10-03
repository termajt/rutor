use rutor::bencode;
use std::env;
use std::fs::File;
use std::io::BufReader;
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


    let input_path = &args[1];
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let b = bencode::decode_from_reader(reader)?;
    if let Some(map) = b.as_dict() {
        for (k, v) in map.iter() {
            let key = std::str::from_utf8(k)?;
            match v {
                bencode::Bencode::Int(i) => println!("{} = {}", key, i),
                bencode::Bencode::Bytes(items) => println!("{} = bytes(len={})", key, items.len()),
                bencode::Bencode::List(bencodes) => println!("{} = list(len={})", key, bencodes.len()),
                bencode::Bencode::Dict(btree_map) => println!("{} = dict(len={})", key, btree_map.len()),
            }
        }
    }
    Ok(())
}
