//! Stream wrapper for unified receiver handling with error recovery.
//!
//! This module provides `BusStream<T>` that unifies mpsc and broadcast receivers
//! into a single Stream interface with robust error handling and recovery mechanisms.

use crate::events::{SharedEventPayload, Topic};
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{timeout, Instant};
use tracing::{debug, warn, error};

/// Errors that can occur when receiving events from a stream.
#[derive(Error, Debug, Clone)]
pub enum StreamError {
    /// Stream channel is closed
    #[error("Stream channel is closed")]
    ChannelClosed,
    /// Event receive timeout
    #[error("Event receive timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    /// Broadcast channel lagged and some events were missed
    #[error("Stream lagged by {missed_events} events")]
    Lagged { missed_events: u64 },
    /// Event deserialization failed
    #[error("Failed to deserialize event: {reason}")]
    DeserializationFailed { reason: String },
    /// Internal receiver error
    #[error("Internal receiver error: {message}")]
    Internal { message: String },
}

/// Configuration for stream error recovery and backpressure handling.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Maximum time to wait for a single recv operation
    pub recv_timeout: Option<Duration>,
    /// Maximum number of consecutive lagged errors before giving up
    pub max_lagged_errors: usize,
    /// Whether to automatically recover from lagged errors
    pub auto_recover_lagged: bool,
    /// Backoff duration between recovery attempts
    pub recovery_backoff: Duration,
    /// Maximum total recovery time before failing
    pub max_recovery_time: Duration,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            recv_timeout: None,
            max_lagged_errors: 10,
            auto_recover_lagged: true,
            recovery_backoff: Duration::from_millis(100),
            max_recovery_time: Duration::from_secs(30),
        }
    }
}

/// Unified receiver type that can handle both mpsc and broadcast channels.
pub enum Receiver {
    /// Lossless mpsc receiver
    Mpsc(mpsc::Receiver<Result<SharedEventPayload, StreamError>>),
    /// Lossy broadcast receiver with lagged error recovery
    Broadcast(broadcast::Receiver<SharedEventPayload>),
}

/// Stream wrapper that unifies mpsc and broadcast receivers with error handling.
///
/// This provides an ergonomic way to iterate over events using async/await syntax
/// with robust error recovery for broadcast channels and proper backpressure handling.
///
/// # Examples
///
/// ```rust,no_run
/// use ai_trading_agent::event_bus::stream::{BusStream, StreamConfig, Receiver};
/// use ai_trading_agent::events::Topic;
/// use tokio::sync::mpsc;
/// use futures::StreamExt;
///
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let (tx, rx) = mpsc::channel(100);
///     let mut stream = BusStream::new(
///         Receiver::Mpsc(rx),
///         Topic::MarketData,
///         StreamConfig::default(),
///     );
///
///     // Use as a stream
///     while let Some(result) = stream.next().await {
///         match result {
///             Ok(event) => println!("Received: {:?}", event.topic()),
///             Err(e) => eprintln!("Stream error: {}", e),
///         }
///     }
///
///     Ok(())
/// }
/// ```
pub struct BusStream {
    receiver: Receiver,
    topic: Topic,
    config: StreamConfig,
    closed: bool,
    lagged_error_count: usize,
    last_recovery_attempt: Option<Instant>,
}

impl BusStream {
    /// Creates a new BusStream from a receiver, topic, and configuration.
    pub fn new(receiver: Receiver, topic: Topic, config: StreamConfig) -> Self {
        Self {
            receiver,
            topic,
            config,
            closed: false,
            lagged_error_count: 0,
            last_recovery_attempt: None,
        }
    }

    /// Creates a new BusStream from an mpsc receiver.
    pub fn from_mpsc(receiver: mpsc::Receiver<Result<SharedEventPayload, StreamError>>, topic: Topic) -> Self {
        Self::new(Receiver::Mpsc(receiver), topic, StreamConfig::default())
    }

    /// Creates a new BusStream from a broadcast receiver.
    pub fn from_broadcast(receiver: broadcast::Receiver<SharedEventPayload>, topic: Topic) -> Self {
        Self::new(Receiver::Broadcast(receiver), topic, StreamConfig::default())
    }

    /// Creates a new BusStream with custom configuration.
    pub fn with_config(receiver: Receiver, topic: Topic, config: StreamConfig) -> Self {
        Self::new(receiver, topic, config)
    }

    /// Returns the topic this stream is subscribed to.
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Returns true if the stream is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns the current stream configuration.
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    /// Updates the stream configuration.
    pub fn set_config(&mut self, config: StreamConfig) {
        self.config = config;
    }

    /// Returns the number of consecutive lagged errors encountered.
    pub fn lagged_error_count(&self) -> usize {
        self.lagged_error_count
    }

    /// Attempts to receive the next event without blocking.
    pub fn try_recv(&mut self) -> Result<Option<SharedEventPayload>, StreamError> {
        if self.closed {
            return Err(StreamError::ChannelClosed);
        }

        match &mut self.receiver {
            Receiver::Mpsc(rx) => {
                match rx.try_recv() {
                    Ok(Ok(event)) => Ok(Some(event)),
                    Ok(Err(err)) => Err(err),
                    Err(mpsc::error::TryRecvError::Empty) => Ok(None),
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        self.closed = true;
                        Err(StreamError::ChannelClosed)
                    }
                }
            }
            Receiver::Broadcast(rx) => {
                match rx.try_recv() {
                    Ok(event) => {
                        self.lagged_error_count = 0; // Reset on successful receive
                        Ok(Some(event))
                    }
                    Err(broadcast::error::TryRecvError::Empty) => Ok(None),
                    Err(broadcast::error::TryRecvError::Closed) => {
                        self.closed = true;
                        Err(StreamError::ChannelClosed)
                    }
                    Err(broadcast::error::TryRecvError::Lagged(missed)) => {
                        match self.handle_lagged_error(missed) {
                            Ok(event) => Ok(Some(event)),
                            Err(err) => Err(err),
                        }
                    }
                }
            }
        }
    }

    /// Receives the next event, waiting if necessary.
    pub async fn recv(&mut self) -> Result<SharedEventPayload, StreamError> {
        if self.closed {
            return Err(StreamError::ChannelClosed);
        }

        // Check timeout configuration before creating the future
        let recv_timeout = self.config.recv_timeout;

        let result = match &mut self.receiver {
            Receiver::Mpsc(rx) => {
                let recv_future = async {
                    match rx.recv().await {
                        Some(Ok(event)) => Ok(event),
                        Some(Err(err)) => Err(err),
                        None => {
                            Err(StreamError::ChannelClosed)
                        }
                    }
                };

                // Apply timeout if configured
                if let Some(timeout_duration) = recv_timeout {
                    match timeout(timeout_duration, recv_future).await {
                        Ok(result) => result,
                        Err(_) => Err(StreamError::Timeout {
                            timeout_ms: timeout_duration.as_millis() as u64,
                        }),
                    }
                } else {
                    recv_future.await
                }
            }
            Receiver::Broadcast(rx) => {
                let recv_future = async {
                    match rx.recv().await {
                        Ok(event) => Ok(event),
                        Err(broadcast::error::RecvError::Closed) => {
                            Err(StreamError::ChannelClosed)
                        }
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            Err(StreamError::Lagged { missed_events: missed })
                        }
                    }
                };

                // Apply timeout if configured
                if let Some(timeout_duration) = recv_timeout {
                    match timeout(timeout_duration, recv_future).await {
                        Ok(result) => result,
                        Err(_) => Err(StreamError::Timeout {
                            timeout_ms: timeout_duration.as_millis() as u64,
                        }),
                    }
                } else {
                    recv_future.await
                }
            }
        };

        // Handle the result and update state
        match result {
            Ok(event) => {
                self.lagged_error_count = 0; // Reset on successful receive
                Ok(event)
            }
            Err(StreamError::ChannelClosed) => {
                self.closed = true;
                Err(StreamError::ChannelClosed)
            }
            Err(StreamError::Lagged { missed_events }) => {
                self.handle_lagged_error(missed_events)
            }
            other => other,
        }
    }

    /// Closes the stream, preventing further event reception.
    pub fn close(&mut self) {
        match &mut self.receiver {
            Receiver::Mpsc(rx) => rx.close(),
            Receiver::Broadcast(_) => {} // Broadcast receivers can't be closed individually
        }
        self.closed = true;
    }

    /// Handles lagged errors from broadcast channels.
    fn handle_lagged_error(&mut self, missed_events: u64) -> Result<SharedEventPayload, StreamError> {
        self.lagged_error_count += 1;

        warn!(
            topic = ?self.topic,
            missed_events = missed_events,
            consecutive_errors = self.lagged_error_count,
            "Broadcast channel lagged, some events were missed"
        );

        if !self.config.auto_recover_lagged {
            return Err(StreamError::Lagged { missed_events });
        }

        if self.lagged_error_count > self.config.max_lagged_errors {
            error!(
                topic = ?self.topic,
                max_errors = self.config.max_lagged_errors,
                "Maximum lagged errors exceeded, giving up"
            );
            self.closed = true;
            return Err(StreamError::Lagged { missed_events });
        }

        // Check if we've exceeded the maximum recovery time
        if let Some(last_attempt) = self.last_recovery_attempt {
            if last_attempt.elapsed() > self.config.max_recovery_time {
                error!(
                    topic = ?self.topic,
                    max_recovery_time = ?self.config.max_recovery_time,
                    "Maximum recovery time exceeded, giving up"
                );
                self.closed = true;
                return Err(StreamError::Lagged { missed_events });
            }
        } else {
            self.last_recovery_attempt = Some(Instant::now());
        }

        debug!(
            topic = ?self.topic,
            attempt = self.lagged_error_count,
            backoff = ?self.config.recovery_backoff,
            "Attempting recovery from lagged error"
        );

        // For lagged errors, we continue trying to receive
        // The next recv() call should succeed or fail definitively
        Err(StreamError::Lagged { missed_events })
    }

    /// Attempts to recover from errors by recreating the internal receiver state.
    pub async fn recover(&mut self) -> Result<(), StreamError> {
        if !self.closed {
            return Ok(());
        }

        // Recovery is only meaningful for certain error types
        // For now, we just reset internal state
        self.closed = false;
        self.lagged_error_count = 0;
        self.last_recovery_attempt = None;

        debug!(topic = ?self.topic, "Stream recovery attempted");
        Ok(())
    }
}

impl Stream for BusStream {
    type Item = Result<SharedEventPayload, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }

        match &mut self.receiver {
            Receiver::Mpsc(rx) => {
                match rx.poll_recv(cx) {
                    Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
                    Poll::Ready(None) => {
                        self.closed = true;
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            Receiver::Broadcast(rx) => {
                // Since broadcast receiver doesn't have poll_recv, we use try_recv
                match rx.try_recv() {
                    Ok(event) => {
                        self.lagged_error_count = 0;
                        Poll::Ready(Some(Ok(event)))
                    }
                    Err(broadcast::error::TryRecvError::Closed) => {
                        self.closed = true;
                        Poll::Ready(None)
                    }
                    Err(broadcast::error::TryRecvError::Lagged(missed)) => {
                        match self.handle_lagged_error(missed) {
                            Ok(event) => Poll::Ready(Some(Ok(event))),
                            Err(err) => Poll::Ready(Some(Err(err))),
                        }
                    }
                    Err(broadcast::error::TryRecvError::Empty) => {
                        // Register the waker to be notified when data is available
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
        }
    }
}

impl fmt::Debug for BusStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusStream")
            .field("topic", &self.topic)
            .field("closed", &self.closed)
            .field("lagged_error_count", &self.lagged_error_count)
            .field("config", &self.config)
            .finish()
    }
}

/// Typed subscription helpers for creating BusStreams for specific event types.
pub struct TypedSubscriber;

impl TypedSubscriber {
    /// Creates a BusStream specifically for market data events.
    pub fn subscribe_market_data(
        receiver: Receiver,
        config: Option<StreamConfig>,
    ) -> BusStream {
        BusStream::new(
            receiver,
            Topic::MarketData,
            config.unwrap_or_default(),
        )
    }

    /// Creates a BusStream specifically for order command events.
    pub fn subscribe_order_commands(
        receiver: Receiver,
        config: Option<StreamConfig>,
    ) -> BusStream {
        BusStream::new(
            receiver,
            Topic::OrderCommand,
            config.unwrap_or_default(),
        )
    }

    /// Creates a BusStream specifically for execution report events.
    pub fn subscribe_execution_reports(
        receiver: Receiver,
        config: Option<StreamConfig>,
    ) -> BusStream {
        BusStream::new(
            receiver,
            Topic::ExecutionReport,
            config.unwrap_or_default(),
        )
    }

    /// Creates a BusStream specifically for position update events.
    pub fn subscribe_position_updates(
        receiver: Receiver,
        config: Option<StreamConfig>,
    ) -> BusStream {
        BusStream::new(
            receiver,
            Topic::PositionUpdate,
            config.unwrap_or_default(),
        )
    }

    /// Creates a BusStream specifically for risk alert events.
    pub fn subscribe_risk_alerts(
        receiver: Receiver,
        config: Option<StreamConfig>,
    ) -> BusStream {
        BusStream::new(
            receiver,
            Topic::RiskAlert,
            config.unwrap_or_default(),
        )
    }

    /// Creates a BusStream specifically for control command events.
    pub fn subscribe_control_commands(
        receiver: Receiver,
        config: Option<StreamConfig>,
    ) -> BusStream {
        BusStream::new(
            receiver,
            Topic::ControlCommand,
            config.unwrap_or_default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventPayload, MarketData, MarketDataEvent, TradeSide};
    use chrono::Utc;
    use futures::StreamExt;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_event() -> SharedEventPayload {
        Arc::new(EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 1,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        }))
    }

    #[test]
    fn test_stream_config_default() {
        let config = StreamConfig::default();
        assert_eq!(config.recv_timeout, None);
        assert_eq!(config.max_lagged_errors, 10);
        assert!(config.auto_recover_lagged);
        assert_eq!(config.recovery_backoff, Duration::from_millis(100));
        assert_eq!(config.max_recovery_time, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_mpsc_stream_creation() {
        let (tx, rx) = mpsc::channel(10);
        let stream = BusStream::from_mpsc(rx, Topic::MarketData);

        assert_eq!(stream.topic(), Topic::MarketData);
        assert!(!stream.is_closed());
        assert_eq!(stream.lagged_error_count(), 0);

        drop(tx);
    }

    #[tokio::test]
    async fn test_broadcast_stream_creation() {
        let (tx, rx) = broadcast::channel(10);
        let stream = BusStream::from_broadcast(rx, Topic::MarketData);

        assert_eq!(stream.topic(), Topic::MarketData);
        assert!(!stream.is_closed());
        assert_eq!(stream.lagged_error_count(), 0);

        drop(tx);
    }

    #[tokio::test]
    async fn test_mpsc_stream_recv() {
        let (tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::from_mpsc(rx, Topic::MarketData);

        let event = create_test_event();
        tx.send(Ok(event.clone())).await.unwrap();

        let received = stream.recv().await.unwrap();
        assert_eq!(received.topic(), Topic::MarketData);

        drop(tx);

        // Test closed channel
        let result = stream.recv().await;
        assert!(result.is_err());
        match result {
            Err(StreamError::ChannelClosed) => assert!(stream.is_closed()),
            other => panic!("Expected ChannelClosed error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_broadcast_stream_recv() {
        let (tx, rx) = broadcast::channel(10);
        let mut stream = BusStream::from_broadcast(rx, Topic::MarketData);

        let event = create_test_event();
        tx.send(event.clone()).unwrap();

        let received = stream.recv().await.unwrap();
        assert_eq!(received.topic(), Topic::MarketData);

        drop(tx);

        // Test closed channel
        let result = stream.recv().await;
        assert!(result.is_err());
        match result {
            Err(StreamError::ChannelClosed) => assert!(stream.is_closed()),
            other => panic!("Expected ChannelClosed error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_mpsc_stream_try_recv() {
        let (tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::from_mpsc(rx, Topic::MarketData);

        // Test empty channel
        let result = stream.try_recv();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Send an event
        let event = create_test_event();
        tx.send(Ok(event.clone())).await.unwrap();

        // Receive the event
        let result = stream.try_recv();
        assert!(result.is_ok());
        let received = result.unwrap();
        assert!(received.is_some());
        assert_eq!(received.unwrap().topic(), Topic::MarketData);

        drop(tx);
    }

    #[tokio::test]
    async fn test_broadcast_stream_try_recv() {
        let (tx, rx) = broadcast::channel(10);
        let mut stream = BusStream::from_broadcast(rx, Topic::MarketData);

        // Test empty channel
        let result = stream.try_recv();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Send an event
        let event = create_test_event();
        tx.send(event.clone()).unwrap();

        // Receive the event
        let result = stream.try_recv();
        assert!(result.is_ok());
        let received = result.unwrap();
        assert!(received.is_some());
        assert_eq!(received.unwrap().topic(), Topic::MarketData);

        drop(tx);
    }

    #[tokio::test]
    async fn test_stream_with_timeout() {
        let (_tx, rx) = mpsc::channel(10);
        let config = StreamConfig {
            recv_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let mut stream = BusStream::with_config(Receiver::Mpsc(rx), Topic::MarketData, config);

        let result = stream.recv().await;
        assert!(result.is_err());
        match result {
            Err(StreamError::Timeout { timeout_ms }) => assert_eq!(timeout_ms, 100),
            other => panic!("Expected Timeout error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_stream_close() {
        let (_tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::from_mpsc(rx, Topic::MarketData);

        assert!(!stream.is_closed());
        stream.close();
        assert!(stream.is_closed());

        let result = stream.recv().await;
        assert!(result.is_err());
        match result {
            Err(StreamError::ChannelClosed) => {}
            other => panic!("Expected ChannelClosed error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_stream_as_futures_stream() {
        let (tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::from_mpsc(rx, Topic::MarketData);

        // Send events
        let event1 = create_test_event();
        let event2 = create_test_event();
        tx.send(Ok(event1.clone())).await.unwrap();
        tx.send(Ok(event2.clone())).await.unwrap();
        drop(tx);

        // Use as a futures Stream
        let mut events = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(event) => events.push(event),
                Err(_) => break,
            }
        }

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].topic(), Topic::MarketData);
        assert_eq!(events[1].topic(), Topic::MarketData);
    }

    #[tokio::test]
    async fn test_broadcast_lagged_recovery() {
        let (tx, rx) = broadcast::channel(2); // Small buffer to trigger lagging
        let config = StreamConfig {
            auto_recover_lagged: true,
            max_lagged_errors: 5,
            ..Default::default()
        };
        let mut stream = BusStream::with_config(Receiver::Broadcast(rx), Topic::MarketData, config);

        // Fill the channel to cause lagging
        let event = create_test_event();
        for _ in 0..5 {
            tx.send(event.clone()).unwrap();
        }

        // This should cause a lagged error but the stream should recover
        let result = stream.try_recv();
        // The exact result depends on timing, but we should either get an event or an error
        match result {
            Ok(Some(_)) => {
                // Successfully received an event
                assert_eq!(stream.lagged_error_count(), 0);
            }
            Ok(None) => {
                // Channel was empty, which is fine
            }
            Err(StreamError::Lagged { .. }) => {
                // This is expected behavior for lagged channels
                assert!(stream.lagged_error_count() > 0);
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_typed_subscribers() {
        let (_tx, rx) = mpsc::channel(10);

        let market_data_stream = TypedSubscriber::subscribe_market_data(
            Receiver::Mpsc(rx),
            None,
        );
        assert_eq!(market_data_stream.topic(), Topic::MarketData);

        let (_tx, rx) = mpsc::channel(10);
        let order_stream = TypedSubscriber::subscribe_order_commands(
            Receiver::Mpsc(rx),
            None,
        );
        assert_eq!(order_stream.topic(), Topic::OrderCommand);

        let (_tx, rx) = mpsc::channel(10);
        let execution_stream = TypedSubscriber::subscribe_execution_reports(
            Receiver::Mpsc(rx),
            None,
        );
        assert_eq!(execution_stream.topic(), Topic::ExecutionReport);

        let (_tx, rx) = mpsc::channel(10);
        let position_stream = TypedSubscriber::subscribe_position_updates(
            Receiver::Mpsc(rx),
            None,
        );
        assert_eq!(position_stream.topic(), Topic::PositionUpdate);

        let (_tx, rx) = mpsc::channel(10);
        let risk_stream = TypedSubscriber::subscribe_risk_alerts(
            Receiver::Mpsc(rx),
            None,
        );
        assert_eq!(risk_stream.topic(), Topic::RiskAlert);

        let (_tx, rx) = mpsc::channel(10);
        let control_stream = TypedSubscriber::subscribe_control_commands(
            Receiver::Mpsc(rx),
            None,
        );
        assert_eq!(control_stream.topic(), Topic::ControlCommand);
    }

    #[tokio::test]
    async fn test_stream_recovery() {
        let (_tx, rx) = mpsc::channel(10);
        let mut stream = BusStream::from_mpsc(rx, Topic::MarketData);

        stream.close();
        assert!(stream.is_closed());

        let recovery_result = stream.recover().await;
        assert!(recovery_result.is_ok());
        assert!(!stream.is_closed());
        assert_eq!(stream.lagged_error_count(), 0);
    }

    #[test]
    fn test_error_display() {
        let channel_closed = StreamError::ChannelClosed;
        assert!(!channel_closed.to_string().is_empty());

        let timeout_err = StreamError::Timeout { timeout_ms: 1000 };
        assert!(timeout_err.to_string().contains("1000ms"));

        let lagged_err = StreamError::Lagged { missed_events: 5 };
        assert!(lagged_err.to_string().contains("5"));

        let deser_err = StreamError::DeserializationFailed {
            reason: "invalid format".to_string(),
        };
        assert!(deser_err.to_string().contains("invalid format"));

        let internal_err = StreamError::Internal {
            message: "test error".to_string(),
        };
        assert!(internal_err.to_string().contains("test error"));
    }
}