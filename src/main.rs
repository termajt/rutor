use rutor::pool::ThreadPool;
use rutor::queue::Queue;
use rutor::socketmanager::{Command, Socket, SocketManager};
use std::env;
use std::net::TcpListener;
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

    let (data_sender, data_receiver) = Queue::new(None);
    let (command_sender, command_receiver) = Queue::new(None);

    let pool = ThreadPool::new();
    pool.execute(move || {
        let listener = TcpListener::bind("127.0.0.1:4000").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut manager = SocketManager::new().unwrap();
        manager.add_socket(Socket::Listener(listener)).unwrap();

        println!("Listening on 127.0.0.1:4000");

        loop {
            manager.run_once(&command_receiver, &data_sender).unwrap();
        }
    });

    loop {
        match data_receiver.recv() {
            Ok((data, peer_addr, fd)) => {
                println!(
                    "data from '{peer_addr:?}' (echo back!): {}",
                    String::from_utf8_lossy(&data)
                );
                if let Err(e) = command_sender.send(Command::Send(fd, data)) {
                    eprintln!("failed to send command: {e}");
                }
            }
            Err(qerr) => match qerr {
                rutor::queue::QueueError::Closed(_) => break,
                rutor::queue::QueueError::Disconnected(_) => break,
                rutor::queue::QueueError::ClosedRecv => break,
                rutor::queue::QueueError::Timeout => break,
                rutor::queue::QueueError::Empty => break,
            },
        }
    }
    Ok(())
}
