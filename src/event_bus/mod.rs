//! Message bus traits and types for the trading agent event system.
//!
//! This module defines the core message bus interface that all event bus implementations
//! must provide. It includes error types, subscription management, and async streaming
//! capabilities for efficient event processing.

pub mod stream;

#[cfg(feature = "deterministic")]
pub mod deterministic;

// Re-export key types from stream module for easier access
pub use stream::{BusStream as EnhancedBusStream, StreamConfig, StreamError, Receiver, TypedSubscriber};

#[cfg(feature = "deterministic")]
pub use deterministic::{DeterministicMessageBus, DeterministicClock, DeterministicError};

#[cfg(test)]
use crate::events::EventPayload;
use crate::events::{SharedEventPayload, Topic};
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::sync::mpsc;

/// Errors that can occur when publishing events to the message bus.
#[derive(Error, Debug, Clone)]
pub enum PublishError {
    /// Bus is disconnected or shut down
    #[error("Message bus is disconnected")]
    Disconnected,
    /// Channel is full and cannot accept more messages
    #[error("Message bus channel is full")]
    ChannelFull,
    /// Event payload is invalid or malformed
    #[error("Invalid event payload: {reason}")]
    InvalidPayload { reason: String },
    /// Publishing to topic is not allowed
    #[error("Publishing to topic {topic:?} is not allowed: {reason}")]
    TopicNotAllowed { topic: Topic, reason: String },
    /// Internal bus error
    #[error("Internal bus error: {message}")]
    Internal { message: String },
}

/// Errors that can occur when subscribing to topics on the message bus.
#[derive(Error, Debug, Clone)]
pub enum SubscribeError {
    /// Topic does not exist or is not available
    #[error("Topic {topic:?} is not available")]
    TopicNotAvailable { topic: Topic },
    /// Subscription limit exceeded
    #[error("Subscription limit exceeded for topic {topic:?}")]
    SubscriptionLimitExceeded { topic: Topic },
    /// Invalid subscription configuration
    #[error("Invalid subscription configuration: {reason}")]
    InvalidConfiguration { reason: String },
    /// Internal bus error
    #[error("Internal bus error: {message}")]
    Internal { message: String },
}

/// Errors that can occur when receiving events from a subscription.
#[derive(Error, Debug, Clone)]
pub enum RecvError {
    /// Subscription channel is closed
    #[error("Subscription channel is closed")]
    ChannelClosed,
    /// Event receive timeout
    #[error("Event receive timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    /// Event deserialization failed
    #[error("Failed to deserialize event: {reason}")]
    DeserializationFailed { reason: String },
    /// Internal receiver error
    #[error("Internal receiver error: {message}")]
    Internal { message: String },
}

/// Configuration for message bus subscriptions.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Buffer size for the subscription channel
    pub buffer_size: usize,
    /// Whether to enable message filtering on the server side
    pub enable_filtering: bool,
    /// Maximum number of events to buffer before dropping old ones
    pub max_buffered_events: Option<usize>,
    /// Timeout for receiving events (None for no timeout)
    pub recv_timeout_ms: Option<u64>,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            enable_filtering: true,
            max_buffered_events: Some(10000),
            recv_timeout_ms: None,
        }
    }
}

/// Statistics about message bus operation.
#[derive(Debug, Clone, Default)]
pub struct BusStats {
    /// Total events published across all topics
    pub total_published: u64,
    /// Total events received across all subscriptions
    pub total_received: u64,
    /// Number of active subscriptions
    pub active_subscriptions: usize,
    /// Number of dropped events due to full channels
    pub dropped_events: u64,
    /// Current memory usage in bytes (approximate)
    pub memory_usage_bytes: usize,
}

/// Async stream wrapper for receiving events from a message bus subscription.
///
/// This provides an ergonomic way to iterate over events using async/await syntax
/// and integrates well with Tokio's stream processing utilities.
pub struct BusStream {
    receiver: mpsc::Receiver<Result<SharedEventPayload, RecvError>>,
    topic: Topic,
    closed: bool,
}

impl BusStream {
    /// Creates a new BusStream from a receiver and topic.
    pub fn new(receiver: mpsc::Receiver<Result<SharedEventPayload, RecvError>>, topic: Topic) -> Self {
        Self {
            receiver,
            topic,
            closed: false,
        }
    }

    /// Returns the topic this stream is subscribed to.
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Returns true if the stream is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Attempts to receive the next event without blocking.
    pub fn try_recv(&mut self) -> Result<Option<SharedEventPayload>, RecvError> {
        match self.receiver.try_recv() {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(err)) => Err(err),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.closed = true;
                Err(RecvError::ChannelClosed)
            }
        }
    }

    /// Receives the next event, waiting if necessary.
    pub async fn recv(&mut self) -> Result<SharedEventPayload, RecvError> {
        match self.receiver.recv().await {
            Some(Ok(event)) => Ok(event),
            Some(Err(err)) => Err(err),
            None => {
                self.closed = true;
                Err(RecvError::ChannelClosed)
            }
        }
    }

    /// Closes the stream, preventing further event reception.
    pub fn close(&mut self) {
        self.receiver.close();
        self.closed = true;
    }
}

impl Stream for BusStream {
    type Item = Result<SharedEventPayload, RecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }

        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
            Poll::Ready(None) => {
                self.closed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl fmt::Debug for BusStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusStream")
            .field("topic", &self.topic)
            .field("closed", &self.closed)
            .finish()
    }
}

/// Core message bus trait for publishing and subscribing to events.
///
/// This trait defines the interface that all message bus implementations must provide.
/// It supports both single-event publishing and subscription-based event consumption
/// with configurable buffering and filtering.
///
/// # Examples
///
/// ```rust,no_run
/// use ai_trading_agent::event_bus::{MessageBus, SubscriptionConfig};
/// use ai_trading_agent::events::{Topic, EventPayload};
/// use std::sync::Arc;
///
/// async fn example(bus: Arc<dyn MessageBus>) -> Result<(), Box<dyn std::error::Error>> {
///     // Subscribe to market data events
///     let mut stream = bus.subscribe(Topic::MarketData, SubscriptionConfig::default()).await?;
///
///     // Process events
///     while let Ok(event) = stream.recv().await {
///         println!("Received event: {:?}", event.topic());
///     }
///
///     Ok(())
/// }
/// ```
#[async_trait::async_trait]
pub trait MessageBus: Send + Sync {
    /// Publishes an event to the specified topic.
    ///
    /// The event will be delivered to all active subscribers of the topic.
    /// This method is non-blocking and will return an error if the bus is
    /// disconnected or the channel is full.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish the event to
    /// * `event` - The event payload to publish
    ///
    /// # Errors
    ///
    /// Returns `PublishError` if the event cannot be published.
    async fn publish(&self, topic: Topic, event: SharedEventPayload) -> Result<(), PublishError>;

    /// Subscribes to events on the specified topic.
    ///
    /// Returns a `BusStream` that can be used to receive events asynchronously.
    /// The subscription will remain active until the stream is dropped or
    /// explicitly closed.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to subscribe to
    /// * `config` - Configuration for the subscription
    ///
    /// # Errors
    ///
    /// Returns `SubscribeError` if the subscription cannot be created.
    async fn subscribe(&self, topic: Topic, config: SubscriptionConfig) -> Result<BusStream, SubscribeError>;

    /// Subscribes to multiple topics with the same configuration.
    ///
    /// This is more efficient than creating multiple individual subscriptions
    /// when you need to listen to multiple topics.
    ///
    /// # Arguments
    ///
    /// * `topics` - The topics to subscribe to
    /// * `config` - Configuration for all subscriptions
    ///
    /// # Errors
    ///
    /// Returns `SubscribeError` if any subscription cannot be created.
    async fn subscribe_multiple(&self, topics: &[Topic], config: SubscriptionConfig) -> Result<Vec<BusStream>, SubscribeError>;

    /// Returns current statistics about the message bus.
    ///
    /// This can be used for monitoring, debugging, and performance analysis.
    async fn stats(&self) -> BusStats;

    /// Returns the number of active subscribers for a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to check
    async fn subscriber_count(&self, topic: Topic) -> usize;

    /// Shuts down the message bus and closes all subscriptions.
    ///
    /// After calling this method, all publish operations will fail and
    /// all active streams will be closed.
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Returns true if the message bus is healthy and operating normally.
    async fn is_healthy(&self) -> bool;
}

/// Type alias for a boxed MessageBus trait object.
pub type BoxedMessageBus = Box<dyn MessageBus>;

/// Type alias for an Arc-wrapped MessageBus trait object.
pub type SharedMessageBus = Arc<dyn MessageBus>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{MarketData, MarketDataEvent, TradeSide};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[test]
    fn test_subscription_config_default() {
        let config = SubscriptionConfig::default();
        assert_eq!(config.buffer_size, 1000);
        assert!(config.enable_filtering);
        assert_eq!(config.max_buffered_events, Some(10000));
        assert_eq!(config.recv_timeout_ms, None);
    }

    #[test]
    fn test_bus_stats_default() {
        let stats = BusStats::default();
        assert_eq!(stats.total_published, 0);
        assert_eq!(stats.total_received, 0);
        assert_eq!(stats.active_subscriptions, 0);
        assert_eq!(stats.dropped_events, 0);
        assert_eq!(stats.memory_usage_bytes, 0);
    }

    #[tokio::test]
    async fn test_bus_stream_creation() {
        let (tx, rx) = mpsc::channel(10);
        let stream = BusStream::new(rx, Topic::MarketData);

        assert_eq!(stream.topic(), Topic::MarketData);
        assert!(!stream.is_closed());

        drop(tx); // Close the sender to test closed detection
    }

    #[tokio::test]
    async fn test_bus_stream_try_recv() {
        let (tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::new(rx, Topic::MarketData);

        // Test empty channel
        let result = stream.try_recv();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Send an event
        let event = Arc::new(EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 1,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        }));

        tx.send(Ok(event.clone())).await.unwrap();

        // Receive the event
        let result = stream.try_recv();
        assert!(result.is_ok());
        let received = result.unwrap();
        assert!(received.is_some());
        assert_eq!(received.unwrap().topic(), Topic::MarketData);

        drop(tx);

        // Test closed channel
        tokio::task::yield_now().await; // Allow close to propagate
        let result = stream.try_recv();
        match result {
            Err(RecvError::ChannelClosed) => {
                assert!(stream.is_closed());
            }
            Ok(None) => {
                // This is also acceptable - channel might not be fully closed yet
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_bus_stream_recv() {
        let (tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::new(rx, Topic::MarketData);

        // Send an event
        let event = Arc::new(EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 1,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        }));

        tx.send(Ok(event.clone())).await.unwrap();

        // Receive the event
        let received = stream.recv().await.unwrap();
        assert_eq!(received.topic(), Topic::MarketData);

        drop(tx);

        // Test closed channel
        let result = stream.recv().await;
        assert!(result.is_err());
        match result {
            Err(RecvError::ChannelClosed) => assert!(stream.is_closed()),
            other => panic!("Expected ChannelClosed error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_bus_stream_close() {
        let (_tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::new(rx, Topic::MarketData);

        assert!(!stream.is_closed());
        stream.close();
        assert!(stream.is_closed());

        // Further operations should fail
        let result = stream.recv().await;
        assert!(result.is_err());
        match result {
            Err(RecvError::ChannelClosed) => {}
            other => panic!("Expected ChannelClosed error, got: {:?}", other),
        }
    }

    #[test]
    fn test_error_display() {
        let publish_err = PublishError::Disconnected;
        assert!(!publish_err.to_string().is_empty());

        let subscribe_err = SubscribeError::TopicNotAvailable { topic: Topic::MarketData };
        assert!(!subscribe_err.to_string().is_empty());

        let recv_err = RecvError::ChannelClosed;
        assert!(!recv_err.to_string().is_empty());
    }

    #[test]
    fn test_error_clone() {
        let publish_err = PublishError::Disconnected;
        let cloned = publish_err.clone();
        assert_eq!(format!("{}", publish_err), format!("{}", cloned));

        let subscribe_err = SubscribeError::TopicNotAvailable { topic: Topic::MarketData };
        let cloned = subscribe_err.clone();
        assert_eq!(format!("{}", subscribe_err), format!("{}", cloned));

        let recv_err = RecvError::ChannelClosed;
        let cloned = recv_err.clone();
        assert_eq!(format!("{}", recv_err), format!("{}", cloned));
    }
}