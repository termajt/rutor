use std::time::Instant;

#[derive(Debug)]
pub struct RateLimiter {
    pub bytes_per_sec: usize,
    available: usize,
    last: Instant,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: usize) -> Self {
        Self {
            bytes_per_sec,
            available: bytes_per_sec,
            last: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last);

        let added = (elapsed.as_secs_f64() * self.bytes_per_sec as f64) as usize;
        if added > 0 {
            self.available = (self.available + added).min(self.bytes_per_sec);
            self.last = now;
        }
    }

    /// Allow up to `want` bytes.
    /// Returns how many bytes are allowed **right now**.
    pub fn allow(&mut self, want: usize) -> usize {
        self.refill();
        let granted = want.min(self.available);
        self.available = self.available.saturating_sub(granted);
        granted
    }
}
