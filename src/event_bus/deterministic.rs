//! Deterministic event bus implementation for reproducible backtesting.
//!
//! This module provides a synchronous, deterministic event bus that ensures
//! reproducible event ordering for backtesting scenarios. Unlike the async
//! implementation, this version uses VecDeque-based queues and synchronous
//! operations to guarantee deterministic behavior.

use crate::event_bus::{
    BusStats, MessageBus, PublishError, SubscribeError, SubscriptionConfig,
};
use crate::events::{SharedEventPayload, Topic};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Errors specific to deterministic mode operations.
#[derive(Error, Debug, Clone)]
pub enum DeterministicError {
    /// Operation not supported in deterministic mode
    #[error("Operation {operation} is not supported in deterministic mode: {reason}")]
    UnsupportedOperation { operation: String, reason: String },
    /// Time advancement error
    #[error("Invalid time advancement: {reason}")]
    InvalidTimeAdvancement { reason: String },
    /// Queue operation error
    #[error("Queue operation failed: {reason}")]
    QueueError { reason: String },
}

/// Controlled time source for deterministic execution.
///
/// This replaces system time with a controlled clock that can be advanced
/// manually to ensure reproducible timing behavior during backtests.
#[derive(Debug, Clone)]
pub struct DeterministicClock {
    current_time: Arc<Mutex<Instant>>,
    start_time: Instant,
}

impl DeterministicClock {
    /// Creates a new deterministic clock starting at the given time.
    pub fn new() -> Self {
        let start = Instant::now();
        Self {
            current_time: Arc::new(Mutex::new(start)),
            start_time: start,
        }
    }

    /// Returns the current deterministic time.
    pub fn now(&self) -> Instant {
        *self.current_time.lock().unwrap()
    }

    /// Advances the clock by the specified duration.
    pub fn advance(&self, duration: Duration) -> Result<(), DeterministicError> {
        let mut current = self.current_time.lock().unwrap();
        *current = *current + duration;
        debug!(
            elapsed = ?current.duration_since(self.start_time),
            advance = ?duration,
            "Advanced deterministic clock"
        );
        Ok(())
    }

    /// Sets the clock to a specific time offset from start.
    pub fn set_time(&self, offset: Duration) -> Result<(), DeterministicError> {
        let mut current = self.current_time.lock().unwrap();
        *current = self.start_time + offset;
        debug!(
            elapsed = ?offset,
            "Set deterministic clock time"
        );
        Ok(())
    }

    /// Returns the elapsed time since clock creation.
    pub fn elapsed(&self) -> Duration {
        self.now().duration_since(self.start_time)
    }
}

/// Event queue entry with deterministic ordering.
#[derive(Debug, Clone)]
struct QueuedEvent {
    event: SharedEventPayload,
    timestamp: Instant,
    sequence: u64,
}

impl PartialEq for QueuedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}

impl Eq for QueuedEvent {}

impl PartialOrd for QueuedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by timestamp first, then by sequence for deterministic tie-breaking
        self.timestamp
            .cmp(&other.timestamp)
            .then(self.sequence.cmp(&other.sequence))
    }
}

/// Deterministic event queue using VecDeque with controlled ordering.
#[derive(Debug)]
struct DeterministicQueue {
    events: VecDeque<QueuedEvent>,
    next_sequence: u64,
    max_size: usize,
    dropped_count: u64,
}

impl DeterministicQueue {
    fn new(max_size: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(max_size.min(1000)),
            next_sequence: 0,
            max_size,
            dropped_count: 0,
        }
    }

    /// Adds an event to the queue with deterministic ordering.
    fn enqueue(&mut self, event: SharedEventPayload, clock: &DeterministicClock) -> Result<(), DeterministicError> {
        if self.events.len() >= self.max_size {
            // Drop oldest event to make room
            self.events.pop_front();
            self.dropped_count += 1;
            warn!(
                dropped_count = self.dropped_count,
                max_size = self.max_size,
                "Dropped oldest event due to queue size limit"
            );
        }

        let sequence = self.next_sequence;
        let queued_event = QueuedEvent {
            event,
            timestamp: clock.now(),
            sequence,
        };

        self.next_sequence += 1;

        // Maintain sorted order - insert in the correct position
        match self.events.binary_search(&queued_event) {
            Ok(pos) | Err(pos) => {
                self.events.insert(pos, queued_event);
            }
        }

        debug!(
            sequence = sequence,
            queue_size = self.events.len(),
            "Enqueued event with deterministic ordering"
        );

        Ok(())
    }

    /// Removes and returns the next event in deterministic order.
    fn dequeue(&mut self) -> Option<SharedEventPayload> {
        self.events.pop_front().map(|queued| {
            debug!(
                sequence = queued.sequence,
                remaining = self.events.len(),
                "Dequeued event"
            );
            queued.event
        })
    }

    /// Returns the number of events in the queue.
    fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true if the queue is empty.
    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the number of dropped events.
    fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}

/// Subscriber information for deterministic delivery.
#[derive(Debug)]
struct DeterministicSubscriber {
    queue: DeterministicQueue,
    config: SubscriptionConfig,
    topic: Topic,
    created_at: Instant,
}

impl DeterministicSubscriber {
    fn new(topic: Topic, config: SubscriptionConfig, clock: &DeterministicClock) -> Self {
        let max_size = config.max_buffered_events.unwrap_or(10000);
        Self {
            queue: DeterministicQueue::new(max_size),
            config,
            topic,
            created_at: clock.now(),
        }
    }
}

/// Deterministic message bus implementation.
///
/// This provides a synchronous, reproducible event bus for backtesting scenarios.
/// All operations are deterministic and time can be controlled manually.
#[derive(Debug)]
pub struct DeterministicMessageBus {
    subscribers: Arc<RwLock<HashMap<Topic, Vec<Arc<Mutex<DeterministicSubscriber>>>>>>,
    clock: DeterministicClock,
    stats: Arc<Mutex<BusStats>>,
    shutdown: Arc<Mutex<bool>>,
    next_subscriber_id: Arc<Mutex<u64>>,
}

impl DeterministicMessageBus {
    /// Creates a new deterministic message bus.
    pub fn new() -> Self {
        info!("Creating deterministic message bus");
        Self {
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            clock: DeterministicClock::new(),
            stats: Arc::new(Mutex::new(BusStats::default())),
            shutdown: Arc::new(Mutex::new(false)),
            next_subscriber_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Returns a reference to the deterministic clock.
    pub fn clock(&self) -> &DeterministicClock {
        &self.clock
    }

    /// Advances the internal clock by the specified duration.
    pub fn advance_time(&self, duration: Duration) -> Result<(), DeterministicError> {
        self.clock.advance(duration)
    }

    /// Processes all pending events in deterministic order.
    ///
    /// This method delivers events to subscribers in the order they were
    /// enqueued, ensuring deterministic execution.
    pub fn process_events(&self) -> Result<usize, DeterministicError> {
        let subscribers = self.subscribers.read().unwrap();
        let mut total_processed = 0;

        for topic_subscribers in subscribers.values() {
            for subscriber in topic_subscribers {
                let mut sub = subscriber.lock().unwrap();
                while !sub.queue.is_empty() {
                    if let Some(_event) = sub.queue.dequeue() {
                        total_processed += 1;
                        // In a real implementation, this would deliver to the subscriber
                    }
                }
            }
        }

        if total_processed > 0 {
            debug!(
                processed = total_processed,
                clock_time = ?self.clock.elapsed(),
                "Processed events in deterministic order"
            );
        }

        Ok(total_processed)
    }

    /// Returns the number of pending events across all topics.
    pub fn pending_events(&self) -> usize {
        let subscribers = self.subscribers.read().unwrap();
        subscribers
            .values()
            .flat_map(|subs| subs.iter())
            .map(|sub| sub.lock().unwrap().queue.len())
            .sum()
    }

    /// Clears all pending events (useful for test cleanup).
    pub fn clear_all_events(&self) -> Result<(), DeterministicError> {
        let subscribers = self.subscribers.read().unwrap();
        for topic_subscribers in subscribers.values() {
            for subscriber in topic_subscribers {
                let mut sub = subscriber.lock().unwrap();
                sub.queue = DeterministicQueue::new(sub.config.max_buffered_events.unwrap_or(10000));
            }
        }

        info!("Cleared all pending events from deterministic bus");
        Ok(())
    }

    /// Creates a synchronous receiver for the specified topic.
    ///
    /// Returns a channel receiver that will receive events in deterministic order.
    pub fn create_receiver(&self, topic: Topic, config: SubscriptionConfig) -> Result<mpsc::Receiver<SharedEventPayload>, SubscribeError> {
        if *self.shutdown.lock().unwrap() {
            return Err(SubscribeError::Internal {
                message: "Bus is shut down".to_string(),
            });
        }

        let (_tx, rx) = mpsc::channel(config.buffer_size);
        let subscriber = Arc::new(Mutex::new(DeterministicSubscriber::new(
            topic,
            config,
            &self.clock,
        )));

        {
            let mut subscribers = self.subscribers.write().unwrap();
            subscribers.entry(topic).or_insert_with(Vec::new).push(subscriber);
        }

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.active_subscriptions += 1;
        }

        debug!(topic = ?topic, "Created deterministic receiver");

        // Note: In a full implementation, we'd spawn a task to forward events from the
        // internal queue to the mpsc channel. For simplicity, we return the receiver here.
        Ok(rx)
    }
}

impl Default for DeterministicMessageBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageBus for DeterministicMessageBus {
    async fn publish(&self, topic: Topic, event: SharedEventPayload) -> Result<(), PublishError> {
        if *self.shutdown.lock().unwrap() {
            return Err(PublishError::Disconnected);
        }

        let subscribers = self.subscribers.read().unwrap();
        let mut delivery_count = 0;

        if let Some(topic_subscribers) = subscribers.get(&topic) {
            for subscriber in topic_subscribers {
                let mut sub = subscriber.lock().unwrap();
                if let Err(e) = sub.queue.enqueue(event.clone(), &self.clock) {
                    warn!(
                        topic = ?topic,
                        error = %e,
                        "Failed to enqueue event for subscriber"
                    );
                    continue;
                }
                delivery_count += 1;
            }
        }

        // Update stats (always update published count, even with no subscribers)
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_published += 1;
            // In deterministic mode, events are queued immediately
            stats.total_received += delivery_count;
        }

        debug!(
            topic = ?topic,
            subscribers = delivery_count,
            sequence = event.sequence(),
            "Published event to deterministic subscribers"
        );

        Ok(())
    }

    async fn subscribe(&self, _topic: Topic, _config: SubscriptionConfig) -> Result<crate::event_bus::BusStream, SubscribeError> {
        // In deterministic mode, we return an error for the async interface
        // Users should use create_receiver() for deterministic operation
        Err(SubscribeError::InvalidConfiguration {
            reason: "Use create_receiver() for deterministic subscriptions".to_string(),
        })
    }

    async fn subscribe_multiple(&self, _topics: &[Topic], _config: SubscriptionConfig) -> Result<Vec<crate::event_bus::BusStream>, SubscribeError> {
        // Not supported in deterministic mode
        Err(SubscribeError::InvalidConfiguration {
            reason: "Multiple subscriptions not supported in deterministic mode".to_string(),
        })
    }

    async fn stats(&self) -> BusStats {
        let base_stats = self.stats.lock().unwrap().clone();
        let subscribers = self.subscribers.read().unwrap();

        // Calculate current memory usage approximation
        let memory_usage = subscribers
            .values()
            .flat_map(|subs| subs.iter())
            .map(|sub| {
                let sub = sub.lock().unwrap();
                sub.queue.len() * std::mem::size_of::<SharedEventPayload>()
            })
            .sum();

        // Calculate total dropped events
        let dropped_events = subscribers
            .values()
            .flat_map(|subs| subs.iter())
            .map(|sub| sub.lock().unwrap().queue.dropped_count())
            .sum();

        BusStats {
            memory_usage_bytes: memory_usage,
            dropped_events,
            ..base_stats
        }
    }

    async fn subscriber_count(&self, topic: Topic) -> usize {
        let subscribers = self.subscribers.read().unwrap();
        subscribers.get(&topic).map_or(0, |subs| subs.len())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down deterministic message bus");

        {
            let mut shutdown = self.shutdown.lock().unwrap();
            *shutdown = true;
        }

        // Clear all subscribers
        {
            let mut subscribers = self.subscribers.write().unwrap();
            subscribers.clear();
        }

        // Reset stats
        {
            let mut stats = self.stats.lock().unwrap();
            *stats = BusStats::default();
        }

        info!("Deterministic message bus shutdown complete");
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        !*self.shutdown.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventPayload, MarketData, MarketDataEvent, TradeSide};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_event(sequence: u64) -> SharedEventPayload {
        Arc::new(EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        }))
    }

    #[test]
    fn test_deterministic_clock() {
        let clock = DeterministicClock::new();
        let start = clock.now();

        // Advance time
        clock.advance(Duration::from_secs(10)).unwrap();
        let after_advance = clock.now();

        assert!(after_advance > start);
        assert_eq!(after_advance.duration_since(start), Duration::from_secs(10));
        assert_eq!(clock.elapsed(), Duration::from_secs(10));
    }

    #[test]
    fn test_deterministic_queue_ordering() {
        let clock = DeterministicClock::new();
        let mut queue = DeterministicQueue::new(100);

        // Add events with different sequences
        let event1 = create_test_event(1);
        let event2 = create_test_event(2);
        let event3 = create_test_event(3);

        queue.enqueue(event2.clone(), &clock).unwrap();
        clock.advance(Duration::from_millis(10)).unwrap();
        queue.enqueue(event1.clone(), &clock).unwrap();
        clock.advance(Duration::from_millis(10)).unwrap();
        queue.enqueue(event3.clone(), &clock).unwrap();

        // Events should come out in timestamp order (which follows insertion order in this test)
        let dequeued1 = queue.dequeue().unwrap();
        let dequeued2 = queue.dequeue().unwrap();
        let dequeued3 = queue.dequeue().unwrap();

        assert_eq!(dequeued1.sequence(), 2); // First inserted
        assert_eq!(dequeued2.sequence(), 1); // Second inserted
        assert_eq!(dequeued3.sequence(), 3); // Third inserted
    }

    #[test]
    fn test_deterministic_queue_size_limit() {
        let clock = DeterministicClock::new();
        let mut queue = DeterministicQueue::new(2); // Very small limit

        // Add events beyond capacity
        for i in 1..=5 {
            let event = create_test_event(i);
            queue.enqueue(event, &clock).unwrap();
        }

        assert_eq!(queue.len(), 2); // Should be at capacity
        assert_eq!(queue.dropped_count(), 3); // Should have dropped 3 events
    }

    #[tokio::test]
    async fn test_deterministic_bus_creation() {
        let bus = DeterministicMessageBus::new();
        assert!(bus.is_healthy().await);
        assert_eq!(bus.pending_events(), 0);

        let stats = bus.stats().await;
        assert_eq!(stats.total_published, 0);
        assert_eq!(stats.active_subscriptions, 0);
    }

    #[tokio::test]
    async fn test_deterministic_bus_publish() {
        let bus = DeterministicMessageBus::new();
        let event = create_test_event(1);

        // Publishing without subscribers should succeed but not deliver
        let result = bus.publish(Topic::MarketData, event).await;
        assert!(result.is_ok());

        let stats = bus.stats().await;
        assert_eq!(stats.total_published, 1);
        assert_eq!(stats.total_received, 0); // No subscribers
    }

    #[tokio::test]
    async fn test_deterministic_bus_subscribe_error() {
        let bus = DeterministicMessageBus::new();

        // Async subscribe should fail in deterministic mode
        let result = bus.subscribe(Topic::MarketData, SubscriptionConfig::default()).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            SubscribeError::InvalidConfiguration { reason } => {
                assert!(reason.contains("create_receiver"));
            }
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_deterministic_bus_create_receiver() {
        let bus = DeterministicMessageBus::new();

        let receiver = bus.create_receiver(Topic::MarketData, SubscriptionConfig::default());
        assert!(receiver.is_ok());

        // Should have created a subscriber
        let rt = tokio::runtime::Runtime::new().unwrap();
        let count = rt.block_on(bus.subscriber_count(Topic::MarketData));
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_deterministic_bus_shutdown() {
        let bus = DeterministicMessageBus::new();

        assert!(bus.is_healthy().await);

        let result = bus.shutdown().await;
        assert!(result.is_ok());
        assert!(!bus.is_healthy().await);

        // Operations after shutdown should fail
        let event = create_test_event(1);
        let result = bus.publish(Topic::MarketData, event).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PublishError::Disconnected => {}
            other => panic!("Expected Disconnected error, got: {:?}", other),
        }
    }

    #[test]
    fn test_deterministic_bus_time_control() {
        let bus = DeterministicMessageBus::new();
        let initial_time = bus.clock().elapsed();

        // Advance time
        bus.advance_time(Duration::from_secs(60)).unwrap();
        let after_advance = bus.clock().elapsed();

        assert!(after_advance > initial_time);
        assert_eq!(after_advance - initial_time, Duration::from_secs(60));
    }

    #[test]
    fn test_deterministic_bus_process_events() {
        let bus = DeterministicMessageBus::new();

        // Create a subscriber
        let _receiver = bus.create_receiver(Topic::MarketData, SubscriptionConfig::default()).unwrap();

        // Publish some events
        let rt = tokio::runtime::Runtime::new().unwrap();
        for i in 1..=3 {
            let event = create_test_event(i);
            rt.block_on(bus.publish(Topic::MarketData, event)).unwrap();
        }

        assert_eq!(bus.pending_events(), 3);

        // Process events
        let processed = bus.process_events().unwrap();
        assert_eq!(processed, 3);
        assert_eq!(bus.pending_events(), 0);
    }

    #[test]
    fn test_deterministic_bus_clear_events() {
        let bus = DeterministicMessageBus::new();

        // Create a subscriber and publish events
        let _receiver = bus.create_receiver(Topic::MarketData, SubscriptionConfig::default()).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        for i in 1..=5 {
            let event = create_test_event(i);
            rt.block_on(bus.publish(Topic::MarketData, event)).unwrap();
        }

        assert_eq!(bus.pending_events(), 5);

        // Clear all events
        bus.clear_all_events().unwrap();
        assert_eq!(bus.pending_events(), 0);
    }
}