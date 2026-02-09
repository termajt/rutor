#[derive(Debug, Default)]
pub struct InFlight {
    bytes: usize,
}

impl InFlight {
    pub fn add(&mut self, bytes: usize) {
        self.bytes += bytes;
    }

    pub fn complete(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_sub(bytes);
    }

    pub fn get(&self) -> usize {
        self.bytes
    }
}
