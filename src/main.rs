use rutor::bencode;
use rutor::pool::ThreadPool;
use rutor::queue::Queue;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

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

    let (se, re) = Queue::new(None);
    let input_path = Arc::new(args[1].clone());
    ThreadPool::new().execute(move || {
        let file = File::open(Path::new(input_path.as_str())).unwrap();
        let reader = BufReader::new(file);
        let b = bencode::decode_from_reader(reader).unwrap();
        let _ = se.send(b);
    });
    if let Ok(b) = re.recv() {
        if let Some(map) = b.as_dict() {
            for (k, v) in map.iter() {
                let key = std::str::from_utf8(k).unwrap();
                match v {
                    bencode::Bencode::Int(i) => println!("{} = {}", key, i),
                    bencode::Bencode::Bytes(items) => {
                        println!("{} = bytes(len={})", key, items.len())
                    }
                    bencode::Bencode::List(bencodes) => {
                        println!("{} = list(len={})", key, bencodes.len())
                    }
                    bencode::Bencode::Dict(btree_map) => {
                        println!("{} = dict(len={})", key, btree_map.len())
                    }
                }
            }
        }
    }
    Ok(())
}
