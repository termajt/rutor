use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

/// Errors returned by queue operations.
#[derive(Debug)]
pub enum QueueError<T> {
    /// Attempted to send to a closed queue.
    Closed(T),
    /// All receivers have been dropped; no one will receive this value.
    Disconnected(T),
    /// Receiver tried to receive but the queue is closed and empty.
    ClosedRecv,
    /// A blocking operation timed out.
    Timeout,
    /// Non-blocking receive found the queue empty.
    Empty,
}

impl<T> fmt::Display for QueueError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::Closed(_) => write!(f, "queue closed"),
            QueueError::Disconnected(_) => write!(f, "no receivers"),
            QueueError::ClosedRecv => write!(f, "queue closed and empty"),
            QueueError::Timeout => write!(f, "operation timed out"),
            QueueError::Empty => write!(f, "empty"),
        }
    }
}

impl<T> std::error::Error for QueueError<T> where T: fmt::Debug {}

#[derive(Debug)]
struct InnerState<T> {
    buffer: VecDeque<T>,
    capacity: Option<usize>,
    closed: bool,
    producers: usize,
    consumers: usize,
}

#[derive(Debug)]
struct Inner<T> {
    state: Mutex<InnerState<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}

impl<T> Inner<T> {
    fn new(capacity: Option<usize>) -> Self {
        Inner {
            state: Mutex::new(InnerState {
                buffer: VecDeque::new(),
                capacity: capacity,
                closed: false,
                producers: 1,
                consumers: 1,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }
}

/// A sending handle for the queue.
///
/// Cloning a `Sender` increases the number of active producers.
/// When all senders are dropped, the queue automatically closes.
#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

/// A receiving handle for the queue.
///
/// Cloning a `Receiver` increases the number of active consumers.
/// When all receivers are dropped, senders will see `Disconnected` errors.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

pub struct Queue<T> {
    _inner: Arc<Inner<T>>,
}

/// The main queue type.
///
/// Created via [`Queue::new`], returning a `(Sender, Receiver)` pair.
/// This type itself is not used directly; it only provides constructors.
impl<T> Queue<T> {
    /// Create a new queue.
    ///
    /// - `capacity = Some(n)` creates a bounded queue with capacity `n` (must be > 0)
    /// - `capacity = None` creates an unbounded queue
    ///
    /// Returns a `(Sender, Receiver)` pair.
    pub fn new(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
        let inner = Arc::new(Inner::new(capacity));
        let s = Sender {
            inner: inner.clone(),
        };
        let r = Receiver { inner };
        (s, r)
    }
}

impl<T> Sender<T> {
    /// Sends a value, blocking if the queue is full.
    ///
    /// - For bounded queues: waits until space is available.
    /// - For unbounded queues: always succeeds immediately.
    ///
    /// Returns:
    /// - `Ok(())` if the value was sent,
    /// - `Err(QueueError::Closed(value))` if the queue is closed,
    /// - `Err(QueueError::Disconnected(value))` if all receivers are gone.
    pub fn send(&self, value: T) -> Result<(), QueueError<T>> {
        let mut state = self.inner.state.lock().unwrap();
        if state.consumers == 0 {
            return Err(QueueError::Disconnected(value));
        }
        if state.closed {
            return Err(QueueError::Closed(value));
        }
        match state.capacity {
            Some(cap) => {
                while state.buffer.len() >= cap {
                    if state.closed {
                        return Err(QueueError::Closed(value));
                    }
                    if state.consumers == 0 {
                        return Err(QueueError::Disconnected(value));
                    }
                    state = self.inner.not_full.wait(state).unwrap();
                }
                state.buffer.push_back(value);
                drop(state);
                self.inner.not_empty.notify_one();
                Ok(())
            }
            None => {
                state.buffer.push_back(value);
                drop(state);
                self.inner.not_empty.notify_one();
                Ok(())
            }
        }
    }

    /// Attempts to send a value without blocking.
    ///
    /// Returns immediately:
    /// - `Ok(())` if the value was enqueued,
    /// - `Err(QueueError::Closed(value))` if the queue is closed or full,
    /// - `Err(QueueError::Disconnected(value))` if all receivers are gone.
    pub fn try_send(&self, value: T) -> Result<(), QueueError<T>> {
        let mut state = self.inner.state.lock().unwrap();
        if state.consumers == 0 {
            return Err(QueueError::Disconnected(value));
        }
        if state.closed {
            return Err(QueueError::Closed(value));
        }
        if let Some(cap) = state.capacity {
            if state.buffer.len() >= cap {
                return Err(QueueError::Closed(value));
            }
        }
        state.buffer.push_back(value);
        drop(state);
        self.inner.not_empty.notify_one();
        Ok(())
    }

    /// Sends a value, blocking with a timeout.
    ///
    /// Waits up to `timeout` for space in the queue.
    ///
    /// Returns:
    /// - `Ok(())` on success,
    /// - `Err(QueueError::Timeout)` if the timeout elapsed,
    /// - `Err(QueueError::Closed(_))` or `Err(QueueError::Disconnected(_))` otherwise.
    pub fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), QueueError<T>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.inner.state.lock().unwrap();
        if state.consumers == 0 {
            return Err(QueueError::Disconnected(value));
        }
        if state.closed {
            return Err(QueueError::Closed(value));
        }
        match state.capacity {
            Some(cap) => {
                while state.buffer.len() >= cap {
                    if state.consumers == 0 {
                        return Err(QueueError::Disconnected(value));
                    }
                    if state.closed {
                        return Err(QueueError::Closed(value));
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(QueueError::Timeout);
                    }
                    let waited = deadline - now;
                    let res = self.inner.not_full.wait_timeout(state, waited).unwrap();
                    state = res.0;
                    if res.1.timed_out() {
                        return Err(QueueError::Timeout);
                    }
                }
                state.buffer.push_back(value);
                drop(state);
                self.inner.not_empty.notify_one();
                Ok(())
            }
            None => {
                state.buffer.push_back(value);
                drop(state);
                self.inner.not_empty.notify_one();
                Ok(())
            }
        }
    }

    /// Closes the queue, preventing further sends.
    ///
    /// Wakes up all blocked senders and receivers.
    pub fn close(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.closed = true;
        drop(state);
        self.inner.not_empty.notify_all();
        self.inner.not_full.notify_all();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.producers > 0 {
            state.producers -= 1;
        }
        if state.producers == 0 {
            state.closed = true;
            drop(state);
            self.inner.not_empty.notify_all();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let mut state = self.inner.state.lock().unwrap();
        state.producers += 1;
        drop(state);
        Sender {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Receiver<T> {
    /// Receives the next available value, blocking if necessary.
    ///
    /// Returns:
    /// - `Ok(value)` when a value is available,
    /// - `Err(QueueError::ClosedRecv)` if the queue is closed and empty.
    pub fn recv(&self) -> Result<T, QueueError<()>> {
        let mut state = self.inner.state.lock().unwrap();
        loop {
            if let Some(v) = state.buffer.pop_front() {
                drop(state);
                self.inner.not_full.notify_one();
                return Ok(v);
            }
            if state.closed && state.producers == 0 {
                return Err(QueueError::ClosedRecv);
            }
            state = self.inner.not_empty.wait(state).unwrap();
        }
    }

    /// Attempts to receive a value without blocking.
    ///
    /// Returns:
    /// - `Ok(value)` if a value is immediately available,
    /// - `Err(QueueError::Empty)` if the queue is empty,
    /// - `Err(QueueError::ClosedRecv)` if the queue is closed and empty.
    pub fn try_recv(&self) -> Result<T, QueueError<()>> {
        let mut state = self.inner.state.lock().unwrap();
        if let Some(v) = state.buffer.pop_front() {
            drop(state);
            self.inner.not_full.notify_one();
            return Ok(v);
        }
        if state.closed && state.producers == 0 {
            return Err(QueueError::ClosedRecv);
        }
        Err(QueueError::Empty)
    }

    /// Receives a value, blocking up to a timeout.
    ///
    /// Returns:
    /// - `Ok(value)` if a value is received in time,
    /// - `Err(QueueError::Timeout)` if no value was received before the timeout,
    /// - `Err(QueueError::ClosedRecv)` if the queue is closed and drained.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, QueueError<()>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.inner.state.lock().unwrap();
        loop {
            if let Some(v) = state.buffer.pop_front() {
                drop(state);
                self.inner.not_full.notify_one();
                return Ok(v);
            }
            if state.closed && state.producers == 0 {
                return Err(QueueError::ClosedRecv);
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(QueueError::Timeout);
            }
            let waited = deadline - now;
            let res = self.inner.not_empty.wait_timeout(state, waited).unwrap();
            state = res.0;
            if res.1.timed_out() {
                return Err(QueueError::Timeout);
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.consumers > 0 {
            state.consumers -= 1;
        }
        if state.consumers == 0 {
            drop(state);
            self.inner.not_full.notify_all();
            self.inner.not_empty.notify_all();
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let mut state = self.inner.state.lock().unwrap();
        state.consumers += 1;
        drop(state);
        Receiver {
            inner: Arc::clone(&self.inner),
        }
    }
}
