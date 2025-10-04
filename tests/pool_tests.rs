use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use rutor::pool::ThreadPool;

#[test]
fn execute_tasks() {
    let pool = ThreadPool::with_capacity(4);
    let result = Arc::new(Mutex::new(Vec::new()));

    for i in 0..5 {
        let res = result.clone();
        pool.execute(move || res.lock().unwrap().push(i));
    }

    thread::sleep(Duration::from_millis(500));

    let mut r = result.lock().unwrap();
    r.sort();
    assert_eq!(*r, vec![0, 1, 2, 3, 4]);
}

#[test]
fn reuses_workers() {
    let pool = ThreadPool::with_capacity(2);

    pool.execute(|| thread::sleep(Duration::from_millis(100)));
    thread::sleep(Duration::from_millis(500));
    let count_after_first = pool.worker_count();

    pool.execute(|| {});
    pool.execute(|| {});
    thread::sleep(Duration::from_millis(500));

    assert!(pool.worker_count() <= 2, "Workers should be reused");
    assert_eq!(count_after_first, pool.worker_count());
}

#[test]
fn spawns_workers_on_demand() {
    let pool = ThreadPool::new();

    pool.execute(|| thread::sleep(Duration::from_millis(300)));
    pool.execute(|| thread::sleep(Duration::from_millis(300)));
    thread::sleep(Duration::from_millis(50));

    assert_eq!(pool.worker_count(), 2)
}

#[test]
fn terminates_idle_workers() {
    let pool = ThreadPool::with_timeout(Duration::from_secs(1));

    pool.execute(|| {});
    thread::sleep(Duration::from_millis(200));
    assert!(pool.worker_count() > 0);

    thread::sleep(Duration::from_secs(2));

    assert_eq!(pool.worker_count(), 0, "Idle workers should terminate");
}

#[test]
fn respects_max_capacity() {
    let pool = ThreadPool::with_capacity(2);

    for _ in 0..5 {
        pool.execute(|| thread::sleep(Duration::from_millis(100)));
    }
    thread::sleep(Duration::from_millis(100));
    assert!(pool.worker_count() <= 2);
}

#[test]
fn handles_many_tasks_quickly() {
    let pool = ThreadPool::new();
    let counter = Arc::new(Mutex::new(0));

    for _ in 0..20 {
        let c = counter.clone();
        pool.execute(move || {
            *c.lock().unwrap() += 1;
        });
    }

    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(*counter.lock().unwrap(), 20);
}

#[test]
fn tasks_complete_before_idle_timeout() {
    let pool = ThreadPool::with_timeout(Duration::from_secs(2));
    let start = Instant::now();

    for _ in 0..4 {
        pool.execute(|| std::thread::sleep(Duration::from_millis(300)));
    }

    std::thread::sleep(Duration::from_secs(1));
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(pool.worker_count() > 0);
}
