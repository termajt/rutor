use crate::queue::{Queue, Receiver, Sender};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type Topic = String;

#[derive(Debug)]
pub struct PubSub<T: Send + Sync + 'static> {
    topics: Arc<Mutex<HashMap<Topic, Vec<Sender<Arc<T>>>>>>,
}

impl<T: Send + Sync + 'static> PubSub<T> {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn subscribe(&self, topic: &str) -> Receiver<Arc<T>> {
        let (tx, rx) = Queue::new(None);
        let mut topics = self.topics.lock().unwrap();
        topics.entry(topic.to_string()).or_default().push(tx);
        rx
    }
    pub fn publish(&self, topic: &str, message: T) {
        let message = Arc::new(message);
        let topics = self.topics.lock().unwrap();
        if let Some(subscribers) = topics.get(topic) {
            for tx in subscribers {
                // Ignore send errors (subscriber dropped)
                let _ = tx.send(message.clone());
            }
        }
    }
}
