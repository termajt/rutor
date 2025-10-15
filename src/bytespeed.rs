use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct ByteSpeed {
    last_update: Instant,
    pub avg: f64,
    smoothing: f64,
    bytes: usize,
    update_interval: Duration,
}

impl ByteSpeed {
    pub fn new(lookback: Duration, update_interval: Duration) -> Self {
        let dt = update_interval.as_secs_f64();
        let t = lookback.as_secs_f64();
        let n = t / dt;
        let alpha = 2.0 / (n + 1.0);
        Self {
            last_update: Instant::now(),
            avg: 0.0,
            smoothing: alpha,
            bytes: 0,
            update_interval: update_interval,
        }
    }

    pub fn update(&mut self, bytes: usize) {
        self.bytes += bytes;
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);
        if elapsed >= self.update_interval {
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
