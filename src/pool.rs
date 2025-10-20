use std::{
    fmt,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// A dynamically-scaling thread pool with an idle timeout.
///
/// The pool will:
/// - Spawn new threads on demand up to `max_workers`.
/// - Reuse existing idle threads for new jobs.
/// - Terminate idle threads after `idle_timeout`.
///
/// When dropped, the pool gracefully stops all workers and waits for them to exit.
pub struct ThreadPool {
    workers: Mutex<Vec<JoinHandle<()>>>,
    jobs: Arc<Mutex<Vec<Job>>>,
    available: Arc<Condvar>,
    max_workers: usize,
    idle_timeout: Duration,
    shutting_down: Arc<Mutex<bool>>,
}

impl fmt::Debug for ThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPool")
            .field("jobs_count", &self.jobs.lock().unwrap().len())
            .field("worker_count", &self.workers.lock().unwrap().len())
            .finish()
    }
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
        Self {
            workers: Mutex::new(Vec::new()),
            jobs: Arc::new(Mutex::new(Vec::new())),
            available: Arc::new(Condvar::new()),
            max_workers: max_workers,
            idle_timeout: idle_timeout,
            shutting_down: Arc::new(Mutex::new(false)),
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
        let mut jobs = self.jobs.lock().unwrap();
        jobs.push(Box::new(f));
        drop(jobs);
        self.available.notify_one();

        let mut workers = self.workers.lock().unwrap();
        if workers.len() < self.max_workers {
            let shutting_down = self.shutting_down.clone();
            let jobs = self.jobs.clone();
            let available = self.available.clone();
            let timeout = self.idle_timeout;

            let handle = thread::spawn(move || {
                worker_loop(jobs, available, timeout, shutting_down);
            });
            workers.push(handle);
        }
    }

    /// Returns the number of currently active worker threads.
    ///
    /// This automatically removes terminated workers before counting.
    pub fn worker_count(&self) -> usize {
        let workers = self.workers.lock().unwrap();
        workers.len()
    }

    pub fn close(&self) {
        {
            let mut shutting_down = self.shutting_down.lock().unwrap();
            if *shutting_down {
                return;
            }
            *shutting_down = true;
        }

        self.available.notify_all();

        let mut workers = self.workers.lock().unwrap();
        while let Some(handle) = workers.pop() {
            let _ = handle.join();
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.close();
    }
}

fn worker_loop(
    jobs: Arc<Mutex<Vec<Job>>>,
    available: Arc<Condvar>,
    idle_timeout: Duration,
    shutting_down: Arc<Mutex<bool>>,
) {
    loop {
        let job_opt = {
            let mut jobs_guard = jobs.lock().unwrap();

            while jobs_guard.is_empty() {
                let shutting_down_guard = shutting_down.lock().unwrap();
                if *shutting_down_guard {
                    return;
                }
                drop(shutting_down_guard);

                let (guard, timeout_res) =
                    available.wait_timeout(jobs_guard, idle_timeout).unwrap();
                jobs_guard = guard;

                if timeout_res.timed_out() && jobs_guard.is_empty() {
                    return;
                }
            }
            jobs_guard.pop()
        };
        if let Some(job) = job_opt {
            job();
        }
    }
}
