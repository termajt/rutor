use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct BlockKey {
    pub piece: u32,
    pub offset: u32,
    pub length: u32,
}

#[derive(Debug)]
pub struct PendingBlock {
    pub requested_at: Instant,
}

#[derive(Debug)]
pub struct PendingRequests {
    blocks: HashMap<BlockKey, PendingBlock>,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: BlockKey) {
        self.blocks.insert(
            key,
            PendingBlock {
                requested_at: Instant::now(),
            },
        );
    }

    pub fn remove(&mut self, key: &BlockKey) -> Option<PendingBlock> {
        self.blocks.remove(key)
    }

    pub fn expire(&mut self, timeout: Duration) -> Vec<BlockKey> {
        let now = Instant::now();
        let mut expired = Vec::new();

        self.blocks.retain(|k, v| {
            if now.duration_since(v.requested_at) >= timeout {
                expired.push(*k);
                false
            } else {
                true
            }
        });

        expired
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }
}
