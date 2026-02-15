use std::time::Instant;

#[derive(Debug)]
pub struct RateLimiter {
    bytes_per_sec: f64,
    available: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: usize) -> Self {
        let bytes_per_sec = bytes_per_sec as f64;
        Self {
            bytes_per_sec,
            available: bytes_per_sec,
            last: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();

        self.available += self.bytes_per_sec * elapsed;
        if self.available > self.bytes_per_sec {
            self.available = self.bytes_per_sec;
        }
        self.last = now;
    }

    pub fn allow(&mut self) -> usize {
        if self.bytes_per_sec == 0.0 {
            return usize::MAX;
        }
        self.refill();

        let granted = self.available.floor();
        self.available -= granted;

        granted as usize
    }
}
