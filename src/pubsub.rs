use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
};

type Topic = String;

#[derive(Debug)]
pub struct PubSub<T: Send + Sync + 'static> {
    topics: Arc<Mutex<HashMap<Topic, Vec<Sender<Arc<T>>>>>>,
    closed: AtomicBool,
}

impl<T: Send + Sync + 'static> PubSub<T> {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            closed: AtomicBool::new(false),
        }
    }

    pub fn subscribe(&self, topic: &str) -> Result<Receiver<Arc<T>>, Box<dyn std::error::Error>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err("PubSub closed".into());
        }
        let (tx, rx) = mpsc::channel();
        let mut topics = self.topics.lock().unwrap();
        topics.entry(topic.to_string()).or_default().push(tx);
        Ok(rx)
    }

    pub fn publish(&self, topic: &str, message: T) -> Result<(), Box<dyn std::error::Error>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err("PubSub closed".into());
        }
        let message = Arc::new(message);
        let topics = self.topics.lock().unwrap();
        if let Some(subscribers) = topics.get(topic) {
            for tx in subscribers {
                // Ignore send errors (subscriber dropped)
                let _ = tx.send(message.clone());
            }
        }
        Ok(())
    }

    pub fn close(&self) {
        let mut topics = self.topics.lock().unwrap();
        topics.clear();
    }
}
