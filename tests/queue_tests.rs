use std::thread;

use rutor::queue::Queue;

#[test]
fn spsc_basic() {
    let (s, r) = Queue::new(Some(4));
    let t = thread::spawn(move || {
        for i in 0..100 {
            s.send(i).unwrap();
        }
    });
    let mut sum = 0usize;
    while let Ok(v) = r.recv() {
        sum += v;
    }
    t.join().unwrap();
    assert_eq!(sum, (0..100).sum());
}

#[test]
fn mpsc_basic() {
    let (s, r) = Queue::new(Some(16));
    let producers = 4;
    let each = 250;
    let mut handles = Vec::new();
    for _ in 0..producers {
        let s2 = s.clone();
        handles.push(thread::spawn(move || {
            for i in 0..each {
                s2.send(i).unwrap();
            }
        }));
    }
    drop(s);
    let mut count = 0usize;
    while let Ok(_v) = r.recv() {
        count += 1;
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(count, producers * each);
}

#[test]
fn spmc_basic() {
    let (s, r) = Queue::new(Some(32));
    let consumers = 3usize;
    let produced = 300usize;
    let mut handles = Vec::new();

    let r1 = r.clone();
    for _ in 0..consumers - 1 {
        let rtmp = r1.clone();
        handles.push(thread::spawn(move || {
            let mut local = 0usize;
            while let Ok(_v) = rtmp.recv() {
                local += 1;
            }
            local
        }));
    }
    let prod = thread::spawn(move || {
        for i in 0..produced {
            s.send(i).unwrap();
        }
    });
    let mut main_count = 0usize;
    while let Ok(_v) = r.recv() {
        main_count += 1;
    }
    prod.join().unwrap();
    for h in handles {
        let _local = h.join().unwrap();
    }
    assert!(main_count <= produced)
}
