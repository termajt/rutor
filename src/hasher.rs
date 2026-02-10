use std::sync::mpsc::Sender;

use sha1::{Digest, Sha1};

use crate::engine::{Event, PeerIoTask};

#[derive(Debug)]
pub struct Hasher {
    expected_hashes: Vec<[u8; 20]>,
    event_tx: Sender<Event>,
    peer_io_tx: Sender<PeerIoTask>,
}

impl Hasher {
    pub fn new(
        expected_hashes: Vec<[u8; 20]>,
        event_tx: Sender<Event>,
        peer_io_tx: Sender<PeerIoTask>,
    ) -> Self {
        Self {
            expected_hashes,
            event_tx,
            peer_io_tx,
        }
    }

    pub fn hash_verify_piece(&self, piece: usize, data: &[u8]) {
        let expected = match self.expected_hashes.get(piece) {
            Some(e) => e,
            None => return,
        };
        let mut hsh = Sha1::new();
        hsh.update(data);
        let result = hsh.finalize();
        let ok = result.as_slice() == expected;
        if ok {
            let _ = self.event_tx.send(Event::PieceVerified { piece });
            let _ = self.peer_io_tx.send(PeerIoTask::PieceVerified { piece });
        } else {
            eprintln!("piece {} failed hash verification", piece);
            let _ = self.event_tx.send(Event::PieceVerificationFailed { piece });
            let _ = self
                .peer_io_tx
                .send(PeerIoTask::PieceVerificationFailed { piece });
        }
    }
}
