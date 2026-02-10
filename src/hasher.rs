use std::{collections::HashMap, sync::mpsc::Sender};

use sha1::{Digest, Sha1};

use crate::engine::{Event, PeerIoTask};

#[derive(Debug)]
struct HashState {
    hsh: Sha1,
    expected_hash: [u8; 20],
}

impl HashState {
    fn new(expected_hash: [u8; 20]) -> Self {
        Self {
            hsh: Sha1::new(),
            expected_hash,
        }
    }

    fn update(&mut self, data: &[u8]) {
        self.hsh.update(data);
    }

    fn finalize_and_verify(self) -> bool {
        let result = self.hsh.finalize();
        result.as_slice() == self.expected_hash
    }
}

#[derive(Debug)]
pub struct Hasher {
    expected_hashes: Vec<[u8; 20]>,
    event_tx: Sender<Event>,
    peer_io_tx: Sender<PeerIoTask>,
    states: HashMap<usize, HashState>,
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
            states: HashMap::new(),
        }
    }

    pub fn update_hash(&mut self, piece: usize, data: &[u8]) {
        if piece >= self.expected_hashes.len() {
            return;
        }
        let state = self
            .states
            .entry(piece)
            .or_insert_with(|| HashState::new(self.expected_hashes[piece]));
        state.update(data);
    }

    pub fn finalize(&mut self, piece: usize) {
        let state = match self.states.remove(&piece) {
            Some(s) => s,
            None => return,
        };
        if state.finalize_and_verify() {
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
