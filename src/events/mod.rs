//! Event types and payloads for the trading agent event bus.
//!
//! This module defines the core event types used throughout the trading system,
//! including market data events, order commands, execution reports, and more.
//! All events flow through the message bus using these standardized types.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Event topics that can be published on the message bus.
///
/// Each topic represents a different category of events in the trading system.
/// This enum ensures type safety and exhaustive matching when handling events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Topic {
    /// Market data events (prices, volumes, order book updates)
    MarketData,
    /// Order commands from strategies to execution engine
    OrderCommand,
    /// Execution reports from the execution engine
    ExecutionReport,
    /// Position and portfolio updates
    PositionUpdate,
    /// Risk management alerts and violations
    RiskAlert,
    /// System control commands (start, stop, pause)
    ControlCommand,
}

impl Topic {
    /// Returns all available topics as a slice.
    pub fn all() -> &'static [Topic] {
        &[
            Topic::MarketData,
            Topic::OrderCommand,
            Topic::ExecutionReport,
            Topic::PositionUpdate,
            Topic::RiskAlert,
            Topic::ControlCommand,
        ]
    }

    /// Returns a human-readable description of the topic.
    pub fn description(&self) -> &'static str {
        match self {
            Topic::MarketData => "Market data events including prices, volumes, and order book updates",
            Topic::OrderCommand => "Order commands issued by trading strategies",
            Topic::ExecutionReport => "Trade execution reports and order status updates",
            Topic::PositionUpdate => "Position and portfolio balance updates",
            Topic::RiskAlert => "Risk management alerts and rule violations",
            Topic::ControlCommand => "System control commands for lifecycle management",
        }
    }
}

/// Market data event containing price and volume information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataEvent {
    /// Trading symbol (e.g., "BTCUSDT")
    pub symbol: String,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Market data payload
    pub data: MarketData,
}

/// Market data types that can be received.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketData {
    /// OHLCV candlestick data
    Kline {
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        interval: String,
    },
    /// Individual trade tick
    Trade {
        price: Decimal,
        quantity: Decimal,
        side: TradeSide,
    },
    /// Order book snapshot or update
    OrderBook {
        bids: Vec<(Decimal, Decimal)>, // (price, quantity)
        asks: Vec<(Decimal, Decimal)>, // (price, quantity)
        is_snapshot: bool,
    },
}

/// Trading side for trades and orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Order command from strategy to execution engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderCommandEvent {
    /// Unique order ID
    pub order_id: Uuid,
    /// Strategy session ID
    pub session_id: Uuid,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Order command details
    pub command: OrderCommand,
}

/// Order command types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderCommand {
    /// Place a new order
    PlaceOrder {
        symbol: String,
        side: TradeSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Option<Decimal>, // None for market orders
        time_in_force: TimeInForce,
    },
    /// Cancel an existing order
    CancelOrder {
        order_id: Uuid,
        symbol: String,
    },
    /// Modify an existing order
    ModifyOrder {
        order_id: Uuid,
        symbol: String,
        new_quantity: Option<Decimal>,
        new_price: Option<Decimal>,
    },
}

/// Order types supported by the execution engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

/// Time in force options for orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    /// Good Till Cancelled
    GTC,
    /// Immediate Or Cancel
    IOC,
    /// Fill Or Kill
    FOK,
    /// Good Till Date
    GTD,
}

/// Execution report from the execution engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReportEvent {
    /// Order ID this report relates to
    pub order_id: Uuid,
    /// Strategy session ID
    pub session_id: Uuid,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Execution report details
    pub report: ExecutionReport,
}

/// Execution report types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionReport {
    /// Order was accepted and is pending
    OrderAccepted {
        symbol: String,
        side: TradeSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Option<Decimal>,
    },
    /// Order was rejected
    OrderRejected {
        symbol: String,
        reason: String,
    },
    /// Partial or full execution occurred
    Trade {
        symbol: String,
        side: TradeSide,
        executed_quantity: Decimal,
        executed_price: Decimal,
        remaining_quantity: Decimal,
        commission: Decimal,
        commission_asset: String,
    },
    /// Order was cancelled
    OrderCancelled {
        symbol: String,
        reason: String,
    },
}

/// Position update event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionUpdateEvent {
    /// Strategy session ID
    pub session_id: Uuid,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Position update details
    pub update: PositionUpdate,
}

/// Position update types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PositionUpdate {
    /// Asset balance changed
    BalanceUpdate {
        asset: String,
        available: Decimal,
        locked: Decimal,
    },
    /// Position in a symbol changed
    PositionUpdate {
        symbol: String,
        quantity: Decimal,
        average_price: Decimal,
        unrealized_pnl: Decimal,
    },
    /// Portfolio-level update
    PortfolioUpdate {
        total_value: Decimal,
        available_margin: Decimal,
        used_margin: Decimal,
        unrealized_pnl: Decimal,
    },
}

/// Risk alert event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskAlertEvent {
    /// Strategy session ID
    pub session_id: Uuid,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Risk alert details
    pub alert: RiskAlert,
}

/// Risk alert types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RiskAlert {
    /// Position size limit exceeded
    PositionSizeViolation {
        symbol: String,
        current_size: Decimal,
        limit: Decimal,
    },
    /// Maximum drawdown exceeded
    DrawdownViolation {
        current_drawdown: Decimal,
        limit: Decimal,
    },
    /// Daily loss limit exceeded
    DailyLossViolation {
        current_loss: Decimal,
        limit: Decimal,
    },
    /// Margin requirement violated
    MarginViolation {
        required_margin: Decimal,
        available_margin: Decimal,
    },
    /// Custom risk rule violation
    CustomRuleViolation {
        rule_name: String,
        description: String,
        severity: RiskSeverity,
    },
}

/// Risk alert severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskSeverity {
    Warning,
    Error,
    Critical,
}

/// Control command event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlCommandEvent {
    /// Command source (could be admin, strategy, or system)
    pub source: String,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Control command details
    pub command: ControlCommand,
}

/// System control commands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ControlCommand {
    /// Start trading operations
    StartTrading {
        session_id: Uuid,
        strategy_name: String,
    },
    /// Stop trading operations
    StopTrading {
        session_id: Uuid,
        reason: String,
    },
    /// Pause trading temporarily
    PauseTrading {
        session_id: Uuid,
        reason: String,
    },
    /// Resume paused trading
    ResumeTrading {
        session_id: Uuid,
    },
    /// Emergency stop all trading
    EmergencyStop {
        reason: String,
    },
    /// System health check
    HealthCheck,
}

/// Sum type containing all possible event payloads.
///
/// This enum allows the message bus to handle any event type in a type-safe manner
/// while maintaining the ability to pattern match on specific event types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventPayload {
    MarketData(MarketDataEvent),
    OrderCommand(OrderCommandEvent),
    ExecutionReport(ExecutionReportEvent),
    PositionUpdate(PositionUpdateEvent),
    RiskAlert(RiskAlertEvent),
    ControlCommand(ControlCommandEvent),
}

impl EventPayload {
    /// Returns the topic for this event payload.
    pub fn topic(&self) -> Topic {
        match self {
            EventPayload::MarketData(_) => Topic::MarketData,
            EventPayload::OrderCommand(_) => Topic::OrderCommand,
            EventPayload::ExecutionReport(_) => Topic::ExecutionReport,
            EventPayload::PositionUpdate(_) => Topic::PositionUpdate,
            EventPayload::RiskAlert(_) => Topic::RiskAlert,
            EventPayload::ControlCommand(_) => Topic::ControlCommand,
        }
    }

    /// Returns the timestamp of this event.
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            EventPayload::MarketData(e) => e.timestamp,
            EventPayload::OrderCommand(e) => e.timestamp,
            EventPayload::ExecutionReport(e) => e.timestamp,
            EventPayload::PositionUpdate(e) => e.timestamp,
            EventPayload::RiskAlert(e) => e.timestamp,
            EventPayload::ControlCommand(e) => e.timestamp,
        }
    }

    /// Returns the sequence number of this event.
    pub fn sequence(&self) -> u64 {
        match self {
            EventPayload::MarketData(e) => e.sequence,
            EventPayload::OrderCommand(e) => e.sequence,
            EventPayload::ExecutionReport(e) => e.sequence,
            EventPayload::PositionUpdate(e) => e.sequence,
            EventPayload::RiskAlert(e) => e.sequence,
            EventPayload::ControlCommand(e) => e.sequence,
        }
    }

    /// Returns the session ID if this event has one.
    pub fn session_id(&self) -> Option<Uuid> {
        match self {
            EventPayload::MarketData(_) => None,
            EventPayload::OrderCommand(e) => Some(e.session_id),
            EventPayload::ExecutionReport(e) => Some(e.session_id),
            EventPayload::PositionUpdate(e) => Some(e.session_id),
            EventPayload::RiskAlert(e) => Some(e.session_id),
            EventPayload::ControlCommand(_) => None,
        }
    }
}

/// Type alias for Arc-wrapped event payloads for efficient sharing.
pub type SharedEventPayload = Arc<EventPayload>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_all() {
        let topics = Topic::all();
        assert_eq!(topics.len(), 6);
        assert!(topics.contains(&Topic::MarketData));
        assert!(topics.contains(&Topic::OrderCommand));
        assert!(topics.contains(&Topic::ExecutionReport));
        assert!(topics.contains(&Topic::PositionUpdate));
        assert!(topics.contains(&Topic::RiskAlert));
        assert!(topics.contains(&Topic::ControlCommand));
    }

    #[test]
    fn test_topic_description() {
        assert!(!Topic::MarketData.description().is_empty());
        assert!(!Topic::OrderCommand.description().is_empty());
        assert!(!Topic::ExecutionReport.description().is_empty());
        assert!(!Topic::PositionUpdate.description().is_empty());
        assert!(!Topic::RiskAlert.description().is_empty());
        assert!(!Topic::ControlCommand.description().is_empty());
    }

    #[test]
    fn test_event_payload_topic() {
        let market_data = EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 1,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        });
        assert_eq!(market_data.topic(), Topic::MarketData);

        let order_command = EventPayload::OrderCommand(OrderCommandEvent {
            order_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            sequence: 2,
            command: OrderCommand::PlaceOrder {
                symbol: "BTCUSDT".to_string(),
                side: TradeSide::Buy,
                order_type: OrderType::Limit,
                quantity: Decimal::new(1, 1),
                price: Some(Decimal::new(49000, 0)),
                time_in_force: TimeInForce::GTC,
            },
        });
        assert_eq!(order_command.topic(), Topic::OrderCommand);
    }

    #[test]
    fn test_event_payload_timestamp() {
        let now = Utc::now();
        let market_data = EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: now,
            sequence: 1,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        });
        assert_eq!(market_data.timestamp(), now);
    }

    #[test]
    fn test_event_payload_sequence() {
        let market_data = EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 42,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        });
        assert_eq!(market_data.sequence(), 42);
    }

    #[test]
    fn test_event_payload_session_id() {
        let session_id = Uuid::new_v4();

        // Event with session ID
        let order_command = EventPayload::OrderCommand(OrderCommandEvent {
            order_id: Uuid::new_v4(),
            session_id,
            timestamp: Utc::now(),
            sequence: 1,
            command: OrderCommand::PlaceOrder {
                symbol: "BTCUSDT".to_string(),
                side: TradeSide::Buy,
                order_type: OrderType::Limit,
                quantity: Decimal::new(1, 1),
                price: Some(Decimal::new(49000, 0)),
                time_in_force: TimeInForce::GTC,
            },
        });
        assert_eq!(order_command.session_id(), Some(session_id));

        // Event without session ID
        let market_data = EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 1,
            data: MarketData::Trade {
                price: Decimal::new(50000, 0),
                quantity: Decimal::new(1, 1),
                side: TradeSide::Buy,
            },
        });
        assert_eq!(market_data.session_id(), None);
    }

    #[test]
    fn test_serialization() {
        let market_data = EventPayload::MarketData(MarketDataEvent {
            symbol: "BTCUSDT".to_string(),
            timestamp: Utc::now(),
            sequence: 1,
            data: MarketData::Kline {
                open: Decimal::new(49000, 0),
                high: Decimal::new(51000, 0),
                low: Decimal::new(48000, 0),
                close: Decimal::new(50000, 0),
                volume: Decimal::new(100, 0),
                interval: "1h".to_string(),
            },
        });

        // Test JSON serialization
        let json = serde_json::to_string(&market_data).expect("Failed to serialize");
        let deserialized: EventPayload = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(market_data, deserialized);
    }
}