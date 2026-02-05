use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ByteSpeed {
    bytes_received: usize,
    pub avg_speed: f64,
    last_update: Instant,
    interval: Duration,
    continous: bool,
    alpha: f64,
}

impl ByteSpeed {
    pub fn new(interval: Duration, continous: bool, alpha: f64) -> Self {
        Self {
            bytes_received: 0,
            avg_speed: 0.0,
            last_update: Instant::now(),
            interval,
            continous,
            alpha,
        }
    }

    pub fn update(&mut self, bytes: usize) {
        self.bytes_received += bytes;
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();

        if elapsed <= 0.0 {
            return;
        }

        let alpha = self.alpha;
        if self.continous || elapsed >= self.interval.as_secs_f64() {
            let instant_rate = self.bytes_received as f64 / elapsed;
            if self.avg_speed == 0.0 {
                self.avg_speed = instant_rate;
            } else {
                self.avg_speed = self.avg_speed * (1.0 - alpha) + instant_rate * alpha;
            }

            if elapsed >= self.interval.as_secs_f64() {
                self.bytes_received = 0;
                self.last_update = now;
            }
        }
    }
}
