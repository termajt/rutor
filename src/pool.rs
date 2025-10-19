use std::{
    fmt,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct WorkerHandle {
    thread: Option<thread::JoinHandle<()>>,
    idle_flag: Arc<(Mutex<bool>, Condvar)>,
    alive: Arc<Mutex<bool>>,
}

struct PoolState {
    jobs: Mutex<Vec<Job>>,
    workers: Mutex<Vec<WorkerHandle>>,
    available: Condvar,
}

impl fmt::Debug for PoolState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolState")
            .field("jobs", &self.jobs.lock().unwrap().len())
            .field("workers", &self.workers.lock().unwrap().len())
            .field("available", &self.available)
            .finish()
    }
}

/// A dynamically-scaling thread pool with an idle timeout.
///
/// The pool will:
/// - Spawn new threads on demand up to `max_workers`.
/// - Reuse existing idle threads for new jobs.
/// - Terminate idle threads after `idle_timeout`.
///
/// When dropped, the pool gracefully stops all workers and waits for them to exit.
#[derive(Debug)]
pub struct ThreadPool {
    state: Arc<PoolState>,
    max_workers: usize,
    idle_timeout: Duration,
}

impl ThreadPool {
    /// Creates a new `ThreadPool` with a default number of workers
    /// equal to the number of logical CPU cores.
    ///
    /// If the system does not report any cores, defaults to 4 workers.
    pub fn new() -> Self {
        let cores = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self::with_capacity(cores)
    }

    /// Creates a new `ThreadPool` with the specified maximum number of workers
    /// and a default idle timeout of 30 seconds.
    ///
    /// # Arguments
    /// * `max_workers` — maximum number of threads to allow.
    ///
    /// # Panics
    /// Panics if `max_workers` is 0.
    pub fn with_capacity(max_workers: usize) -> Self {
        Self::with_capacity_and_timeout(max_workers, Duration::from_secs(30))
    }

    /// Creates a new `ThreadPool` with custom idle timeout.
    ///
    /// # Arguments
    /// * `idle_timeout` — duration before idle workers shut down.
    ///
    /// # Panics
    /// Panics if `max_workers` is 0.
    pub fn with_timeout(idle_timeout: Duration) -> Self {
        let cores = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self::with_capacity_and_timeout(cores, idle_timeout)
    }

    /// Creates a new `ThreadPool` with custom worker capacity and idle timeout.
    ///
    /// # Arguments
    /// * `max_workers` — maximum number of threads to allow.
    /// * `idle_timeout` — duration before idle workers shut down.
    ///
    /// # Panics
    /// Panics if `max_workers` is 0.
    pub fn with_capacity_and_timeout(max_workers: usize, idle_timeout: Duration) -> Self {
        assert!(max_workers > 0);
        ThreadPool {
            state: Arc::new(PoolState {
                jobs: Mutex::new(Vec::new()),
                workers: Mutex::new(Vec::new()),
                available: Condvar::new(),
            }),
            max_workers: max_workers,
            idle_timeout: idle_timeout,
        }
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    /// Submits a job (closure) to the thread pool for execution.
    ///
    /// If there are idle workers available, one is woken to handle the job.
    /// Otherwise, if the pool has not yet reached its maximum capacity,
    /// a new worker thread is spawned.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        {
            let mut jobs = self.state.jobs.lock().unwrap();
            jobs.push(Box::new(f));
        }

        let mut workers = self.state.workers.lock().unwrap();
        self.cleanup_dead_workers(&mut workers);

        let idle_worker = workers.iter().find(|w| *w.idle_flag.0.lock().unwrap());
        if let Some(w) = idle_worker {
            w.idle_flag.1.notify_one();
        } else if workers.len() < self.max_workers {
            let idle_flag = Arc::new((Mutex::new(false), Condvar::new()));
            let state = self.state.clone();
            let idle_clone = idle_flag.clone();
            let alive = Arc::new(Mutex::new(true));
            let alive_clone = alive.clone();
            let timeout = self.idle_timeout;
            let thread =
                thread::spawn(move || worker_loop(state, idle_clone, alive_clone, timeout));
            workers.push(WorkerHandle {
                thread: Some(thread),
                idle_flag: idle_flag,
                alive: alive,
            });
        } else {
            self.state.available.notify_one();
        }
    }

    /// Returns the number of currently active worker threads.
    ///
    /// This automatically removes terminated workers before counting.
    pub fn worker_count(&self) -> usize {
        let mut workers = self.state.workers.lock().unwrap();
        self.cleanup_dead_workers(&mut workers);
        workers.len()
    }

    fn cleanup_dead_workers(&self, workers: &mut Vec<WorkerHandle>) {
        workers.retain(|w| *w.alive.lock().unwrap());
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        let mut workers = self.state.workers.lock().unwrap();
        for mut w in workers.drain(..) {
            *w.alive.lock().unwrap() = false;
            w.idle_flag.1.notify_all();
            if let Some(thread) = w.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

fn worker_loop(
    state: Arc<PoolState>,
    idle_flag: Arc<(Mutex<bool>, Condvar)>,
    alive: Arc<Mutex<bool>>,
    timeout: Duration,
) {
    loop {
        let job = {
            let mut jobs = state.jobs.lock().unwrap();
            loop {
                if let Some(job) = jobs.pop() {
                    break Some(job);
                }

                {
                    let alive = alive.lock().unwrap();
                    if !*alive {
                        return;
                    }
                }

                {
                    let mut idle = idle_flag.0.lock().unwrap();
                    *idle = true;
                }

                let (lock, timeout_result) = idle_flag.1.wait_timeout(jobs, timeout).unwrap();
                jobs = lock;

                {
                    let mut idle = idle_flag.0.lock().unwrap();
                    *idle = false;
                }

                if timeout_result.timed_out() {
                    {
                        let mut alive = alive.lock().unwrap();
                        *alive = false;
                    }
                    return;
                }
            }
        };
        if let Some(job) = job {
            job();
            state.available.notify_one();
        }
    }
}
