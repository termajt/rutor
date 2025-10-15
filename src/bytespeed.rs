use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct ByteSpeed {
    last_update: Instant,
    pub avg: f64,
    smoothing: f64,
    bytes: usize,
    min_interval: Duration,
}

impl ByteSpeed {
    pub fn new(min_interval: Duration) -> Self {
        Self::with_smoothing(0.3, min_interval)
    }

    pub fn with_smoothing(smoothing_factor: f64, min_interval: Duration) -> Self {
        Self {
            last_update: Instant::now(),
            avg: 0.0,
            smoothing: smoothing_factor,
            bytes: 0,
            min_interval: min_interval,
        }
    }

    pub fn update(&mut self, bytes: usize) {
        self.bytes += bytes;
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);
        if elapsed >= self.min_interval {
            let instant_speed = (self.bytes as f64) / elapsed.as_secs_f64();

            if self.avg == 0.0 {
                self.avg = instant_speed;
            } else {
                self.avg = self.smoothing * instant_speed + (1.0 - self.smoothing) * self.avg;
            }

            self.bytes = 0;
            self.last_update = now;
        }
    }
}
