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
    pub deadline: Instant,
}

#[derive(Debug)]
pub struct PendingRequests {
    blocks: HashMap<BlockKey, PendingBlock>,
    timeout: Duration,
}

impl PendingRequests {
    pub fn new(timeout: Duration) -> Self {
        Self {
            blocks: HashMap::new(),
            timeout,
        }
    }

    pub fn insert(&mut self, key: BlockKey) {
        self.blocks.insert(
            key,
            PendingBlock {
                deadline: Instant::now() + self.timeout,
            },
        );
    }

    pub fn remove(&mut self, key: &BlockKey) -> bool {
        self.blocks.remove(key).is_some()
    }

    pub fn expire(&mut self) -> Vec<BlockKey> {
        let now = Instant::now();
        let mut expired = Vec::new();

        self.blocks.retain(|k, v| {
            if now >= v.deadline {
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

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}
