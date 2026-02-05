use std::{
    sync::{Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct ManualResetEvent {
    state: Mutex<bool>,
    cvar: Condvar,
}

impl ManualResetEvent {
    pub fn new(initial: bool) -> Self {
        Self {
            state: Mutex::new(initial),
            cvar: Condvar::new(),
        }
    }

    pub fn wait(&self) {
        let mut signaled = self.state.lock().unwrap();
        while !*signaled {
            signaled = self.cvar.wait(signaled).unwrap();
        }
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut signaled = self.state.lock().unwrap();
        let start = Instant::now();
        while !*signaled {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return false;
            }
            let remaining = timeout - elapsed;
            let (lock_result, wait_result) = self.cvar.wait_timeout(signaled, remaining).unwrap();
            signaled = lock_result;
            if wait_result.timed_out() {
                return false;
            }
        }
        true
    }

    pub fn is_set(&self) -> bool {
        let signaled = self.state.lock().unwrap();
        *signaled
    }

    pub fn set(&self) {
        let mut signaled = self.state.lock().unwrap();
        *signaled = true;
        self.cvar.notify_all();
    }

    pub fn reset(&self) {
        let mut signaled = self.state.lock().unwrap();
        *signaled = false;
    }
}
