//! Domain event structures for the AI Trading Agent.
//!
//! This module contains concrete event structures that represent business domain events
//! flowing through the event bus. These structs are designed for performance and correctness
//! in high-frequency trading scenarios.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Market data event containing OHLCV price and volume information.
///
/// This structure represents standardized market data that flows through the system,
/// normalized from various exchange formats or CSV files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MarketData {
    /// Trading symbol (e.g., "BTCUSDT", "ETHBTC")
    pub symbol: String,
    /// Event timestamp in UTC
    pub timestamp: DateTime<Utc>,
    /// Monotonic sequence number for event ordering
    pub sequence_number: u64,
    /// Trading session ID for correlation
    pub session_id: Uuid,
    /// Opening price for the period
    pub open: Decimal,
    /// Highest price during the period
    pub high: Decimal,
    /// Lowest price during the period
    pub low: Decimal,
    /// Closing price for the period
    pub close: Decimal,
    /// Total volume traded during the period
    pub volume: Decimal,
}

impl MarketData {
    /// Creates a new MarketData event with validation.
    ///
    /// # Arguments
    /// * `symbol` - Trading pair symbol
    /// * `timestamp` - Event timestamp
    /// * `sequence_number` - Monotonic sequence for ordering
    /// * `session_id` - Session identifier
    /// * `open` - Opening price
    /// * `high` - High price
    /// * `low` - Low price
    /// * `close` - Closing price
    /// * `volume` - Trading volume
    ///
    /// # Returns
    /// * `Ok(MarketData)` if all validations pass
    /// * `Err(String)` if validation fails
    pub fn new(
        symbol: String,
        timestamp: DateTime<Utc>,
        sequence_number: u64,
        session_id: Uuid,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
    ) -> Result<Self, String> {
        // Validate symbol is not empty
        if symbol.trim().is_empty() {
            return Err("Symbol cannot be empty".to_string());
        }

        // Validate prices are positive
        if open <= Decimal::ZERO {
            return Err("Open price must be positive".to_string());
        }
        if high <= Decimal::ZERO {
            return Err("High price must be positive".to_string());
        }
        if low <= Decimal::ZERO {
            return Err("Low price must be positive".to_string());
        }
        if close <= Decimal::ZERO {
            return Err("Close price must be positive".to_string());
        }

        // Validate volume is non-negative
        if volume < Decimal::ZERO {
            return Err("Volume cannot be negative".to_string());
        }

        // Validate OHLC relationships
        if high < low {
            return Err("High price cannot be less than low price".to_string());
        }
        if high < open || high < close {
            return Err("High price must be >= open and close prices".to_string());
        }
        if low > open || low > close {
            return Err("Low price must be <= open and close prices".to_string());
        }

        Ok(MarketData {
            symbol,
            timestamp,
            sequence_number,
            session_id,
            open,
            high,
            low,
            close,
            volume,
        })
    }

    /// Validates the OHLCV data integrity.
    pub fn is_valid(&self) -> bool {
        self.open > Decimal::ZERO
            && self.high > Decimal::ZERO
            && self.low > Decimal::ZERO
            && self.close > Decimal::ZERO
            && self.volume >= Decimal::ZERO
            && self.high >= self.low
            && self.high >= self.open
            && self.high >= self.close
            && self.low <= self.open
            && self.low <= self.close
            && !self.symbol.trim().is_empty()
    }
}

/// Order side enumeration for buy/sell directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Order type enumeration for different order types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

/// Order command issued by trading strategies to the execution engine.
///
/// This structure represents an instruction to place, modify, or cancel orders
/// in the market. It flows from strategies to the execution engine via the event bus.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OrderCommand {
    /// Trading symbol for the order
    pub symbol: String,
    /// Buy or sell side
    pub side: OrderSide,
    /// Quantity to trade (always positive)
    pub quantity: Decimal,
    /// Type of order (market, limit, etc.)
    pub order_type: OrderType,
    /// Price for limit orders (None for market orders)
    pub price: Option<Decimal>,
    /// Command timestamp in UTC
    pub timestamp: DateTime<Utc>,
    /// Monotonic sequence number for ordering
    pub sequence_number: u64,
    /// Trading session ID for correlation
    pub session_id: Uuid,
}

impl OrderCommand {
    /// Creates a new OrderCommand with validation.
    ///
    /// # Arguments
    /// * `symbol` - Trading pair symbol
    /// * `side` - Buy or sell
    /// * `quantity` - Amount to trade
    /// * `order_type` - Type of order
    /// * `price` - Price for limit orders
    /// * `timestamp` - Command timestamp
    /// * `sequence_number` - Sequence for ordering
    /// * `session_id` - Session identifier
    ///
    /// # Returns
    /// * `Ok(OrderCommand)` if all validations pass
    /// * `Err(String)` if validation fails
    pub fn new(
        symbol: String,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Option<Decimal>,
        timestamp: DateTime<Utc>,
        sequence_number: u64,
        session_id: Uuid,
    ) -> Result<Self, String> {
        // Validate symbol is not empty
        if symbol.trim().is_empty() {
            return Err("Symbol cannot be empty".to_string());
        }

        // Validate quantity is positive
        if quantity <= Decimal::ZERO {
            return Err("Quantity must be positive".to_string());
        }

        // Validate price for limit orders
        match order_type {
            OrderType::Limit | OrderType::StopLossLimit | OrderType::TakeProfitLimit => {
                if price.is_none() {
                    return Err("Price is required for limit orders".to_string());
                }
                if let Some(p) = price {
                    if p <= Decimal::ZERO {
                        return Err("Price must be positive".to_string());
                    }
                }
            }
            OrderType::Market | OrderType::StopLoss | OrderType::TakeProfit => {
                // These order types don't require a price
            }
        }

        Ok(OrderCommand {
            symbol,
            side,
            quantity,
            order_type,
            price,
            timestamp,
            sequence_number,
            session_id,
        })
    }

    /// Validates the order command.
    pub fn is_valid(&self) -> bool {
        !self.symbol.trim().is_empty()
            && self.quantity > Decimal::ZERO
            && match self.order_type {
                OrderType::Limit | OrderType::StopLossLimit | OrderType::TakeProfitLimit => {
                    self.price.map_or(false, |p| p > Decimal::ZERO)
                }
                _ => true,
            }
    }
}

/// Order execution status enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

/// Execution report from the execution engine about order status changes.
///
/// This structure represents the result of order processing, including fills,
/// cancellations, and rejections. It flows from the execution engine back to strategies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ExecutionReport {
    /// Unique order identifier
    pub order_id: Uuid,
    /// Trading symbol
    pub symbol: String,
    /// Buy or sell side
    pub side: OrderSide,
    /// Original order quantity
    pub quantity: Decimal,
    /// Execution price (if filled)
    pub price: Option<Decimal>,
    /// Current order status
    pub status: OrderStatus,
    /// Report timestamp in UTC
    pub timestamp: DateTime<Utc>,
    /// Monotonic sequence number for ordering
    pub sequence_number: u64,
    /// Trading session ID for correlation
    pub session_id: Uuid,
}

impl ExecutionReport {
    /// Creates a new ExecutionReport with validation.
    ///
    /// # Arguments
    /// * `order_id` - Unique order identifier
    /// * `symbol` - Trading symbol
    /// * `side` - Order side
    /// * `quantity` - Order quantity
    /// * `price` - Execution price
    /// * `status` - Order status
    /// * `timestamp` - Report timestamp
    /// * `sequence_number` - Sequence number
    /// * `session_id` - Session identifier
    ///
    /// # Returns
    /// * `Ok(ExecutionReport)` if all validations pass
    /// * `Err(String)` if validation fails
    pub fn new(
        order_id: Uuid,
        symbol: String,
        side: OrderSide,
        quantity: Decimal,
        price: Option<Decimal>,
        status: OrderStatus,
        timestamp: DateTime<Utc>,
        sequence_number: u64,
        session_id: Uuid,
    ) -> Result<Self, String> {
        // Validate symbol is not empty
        if symbol.trim().is_empty() {
            return Err("Symbol cannot be empty".to_string());
        }

        // Validate quantity is positive
        if quantity <= Decimal::ZERO {
            return Err("Quantity must be positive".to_string());
        }

        // Validate price if present
        if let Some(p) = price {
            if p <= Decimal::ZERO {
                return Err("Price must be positive when specified".to_string());
            }
        }

        // For filled orders, price should be specified
        if matches!(status, OrderStatus::Filled | OrderStatus::PartiallyFilled) && price.is_none() {
            return Err("Price is required for filled orders".to_string());
        }

        Ok(ExecutionReport {
            order_id,
            symbol,
            side,
            quantity,
            price,
            status,
            timestamp,
            sequence_number,
            session_id,
        })
    }

    /// Validates the execution report.
    pub fn is_valid(&self) -> bool {
        !self.symbol.trim().is_empty()
            && self.quantity > Decimal::ZERO
            && self.price.map_or(true, |p| p > Decimal::ZERO)
            && match self.status {
                OrderStatus::Filled | OrderStatus::PartiallyFilled => self.price.is_some(),
                _ => true,
            }
    }
}

/// Position update event containing current position and P&L information.
///
/// This structure represents changes in portfolio positions, including quantity
/// changes, average price updates, and unrealized profit/loss calculations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PositionUpdate {
    /// Trading symbol for the position
    pub symbol: String,
    /// Current position quantity (positive for long, negative for short)
    pub quantity: Decimal,
    /// Average entry price for the position
    pub avg_price: Decimal,
    /// Unrealized profit and loss
    pub unrealized_pnl: Decimal,
    /// Update timestamp in UTC
    pub timestamp: DateTime<Utc>,
    /// Monotonic sequence number for ordering
    pub sequence_number: u64,
    /// Trading session ID for correlation
    pub session_id: Uuid,
}

impl PositionUpdate {
    /// Creates a new PositionUpdate with validation.
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol
    /// * `quantity` - Position quantity
    /// * `avg_price` - Average entry price
    /// * `unrealized_pnl` - Unrealized P&L
    /// * `timestamp` - Update timestamp
    /// * `sequence_number` - Sequence number
    /// * `session_id` - Session identifier
    ///
    /// # Returns
    /// * `Ok(PositionUpdate)` if all validations pass
    /// * `Err(String)` if validation fails
    pub fn new(
        symbol: String,
        quantity: Decimal,
        avg_price: Decimal,
        unrealized_pnl: Decimal,
        timestamp: DateTime<Utc>,
        sequence_number: u64,
        session_id: Uuid,
    ) -> Result<Self, String> {
        // Validate symbol is not empty
        if symbol.trim().is_empty() {
            return Err("Symbol cannot be empty".to_string());
        }

        // For non-zero positions, average price must be positive
        if quantity != Decimal::ZERO && avg_price <= Decimal::ZERO {
            return Err("Average price must be positive for non-zero positions".to_string());
        }

        // For zero positions, average price should be zero
        if quantity == Decimal::ZERO && avg_price != Decimal::ZERO {
            return Err("Average price should be zero for zero positions".to_string());
        }

        Ok(PositionUpdate {
            symbol,
            quantity,
            avg_price,
            unrealized_pnl,
            timestamp,
            sequence_number,
            session_id,
        })
    }

    /// Validates the position update.
    pub fn is_valid(&self) -> bool {
        !self.symbol.trim().is_empty()
            && if self.quantity == Decimal::ZERO {
                self.avg_price == Decimal::ZERO
            } else {
                self.avg_price > Decimal::ZERO
            }
    }

    /// Returns true if this represents a long position.
    pub fn is_long(&self) -> bool {
        self.quantity > Decimal::ZERO
    }

    /// Returns true if this represents a short position.
    pub fn is_short(&self) -> bool {
        self.quantity < Decimal::ZERO
    }

    /// Returns true if this represents a flat (no) position.
    pub fn is_flat(&self) -> bool {
        self.quantity == Decimal::ZERO
    }
}

/// Risk alert severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Risk alert type enumeration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskAlertType {
    PositionSizeLimit,
    DrawdownLimit,
    DailyLossLimit,
    MarginRequirement,
    VaRLimit,
    ConcentrationLimit,
    CustomRule,
}

/// Risk alert event for risk management violations and warnings.
///
/// This structure represents risk management events that require attention,
/// from informational notices to critical violations requiring immediate action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RiskAlert {
    /// Type of risk alert
    pub alert_type: RiskAlertType,
    /// Human-readable alert message
    pub message: String,
    /// Alert severity level
    pub severity: RiskSeverity,
    /// Alert timestamp in UTC
    pub timestamp: DateTime<Utc>,
    /// Monotonic sequence number for ordering
    pub sequence_number: u64,
    /// Trading session ID for correlation
    pub session_id: Uuid,
}

impl RiskAlert {
    /// Creates a new RiskAlert with validation.
    ///
    /// # Arguments
    /// * `alert_type` - Type of risk alert
    /// * `message` - Alert message
    /// * `severity` - Alert severity
    /// * `timestamp` - Alert timestamp
    /// * `sequence_number` - Sequence number
    /// * `session_id` - Session identifier
    ///
    /// # Returns
    /// * `Ok(RiskAlert)` if all validations pass
    /// * `Err(String)` if validation fails
    pub fn new(
        alert_type: RiskAlertType,
        message: String,
        severity: RiskSeverity,
        timestamp: DateTime<Utc>,
        sequence_number: u64,
        session_id: Uuid,
    ) -> Result<Self, String> {
        // Validate message is not empty
        if message.trim().is_empty() {
            return Err("Alert message cannot be empty".to_string());
        }

        Ok(RiskAlert {
            alert_type,
            message,
            severity,
            timestamp,
            sequence_number,
            session_id,
        })
    }

    /// Validates the risk alert.
    pub fn is_valid(&self) -> bool {
        !self.message.trim().is_empty()
    }

    /// Returns true if this is a critical alert requiring immediate action.
    pub fn is_critical(&self) -> bool {
        matches!(self.severity, RiskSeverity::Critical)
    }

    /// Returns true if this alert should trigger automated responses.
    pub fn requires_action(&self) -> bool {
        matches!(self.severity, RiskSeverity::Error | RiskSeverity::Critical)
    }
}

/// Control command type enumeration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlCommandType {
    Start,
    Stop,
    Pause,
    Resume,
    EmergencyStop,
    HealthCheck,
    ConfigUpdate,
    StatusRequest,
}

/// Control command event for system lifecycle and configuration management.
///
/// This structure represents system control commands that manage the trading
/// agent's lifecycle, configuration, and operational state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ControlCommand {
    /// Type of control command
    pub command_type: ControlCommandType,
    /// Command parameters as key-value pairs
    pub parameters: HashMap<String, String>,
    /// Command timestamp in UTC
    pub timestamp: DateTime<Utc>,
    /// Monotonic sequence number for ordering
    pub sequence_number: u64,
    /// Trading session ID for correlation
    pub session_id: Uuid,
}

impl ControlCommand {
    /// Creates a new ControlCommand with validation.
    ///
    /// # Arguments
    /// * `command_type` - Type of control command
    /// * `parameters` - Command parameters
    /// * `timestamp` - Command timestamp
    /// * `sequence_number` - Sequence number
    /// * `session_id` - Session identifier
    ///
    /// # Returns
    /// * `Ok(ControlCommand)` if all validations pass
    /// * `Err(String)` if validation fails
    pub fn new(
        command_type: ControlCommandType,
        parameters: HashMap<String, String>,
        timestamp: DateTime<Utc>,
        sequence_number: u64,
        session_id: Uuid,
    ) -> Result<Self, String> {
        // Validate parameters don't contain empty keys
        for key in parameters.keys() {
            if key.trim().is_empty() {
                return Err("Parameter keys cannot be empty".to_string());
            }
        }

        Ok(ControlCommand {
            command_type,
            parameters,
            timestamp,
            sequence_number,
            session_id,
        })
    }

    /// Validates the control command.
    pub fn is_valid(&self) -> bool {
        self.parameters.keys().all(|k| !k.trim().is_empty())
    }

    /// Gets a parameter value by key.
    pub fn get_parameter(&self, key: &str) -> Option<&String> {
        self.parameters.get(key)
    }

    /// Sets a parameter value.
    pub fn set_parameter(&mut self, key: String, value: String) -> Result<(), String> {
        if key.trim().is_empty() {
            return Err("Parameter key cannot be empty".to_string());
        }
        self.parameters.insert(key, value);
        Ok(())
    }

    /// Returns true if this is an emergency command requiring immediate action.
    pub fn is_emergency(&self) -> bool {
        matches!(self.command_type, ControlCommandType::EmergencyStop)
    }
}

/// Type aliases for Arc-wrapped event structures for efficient sharing.
pub type SharedMarketData = Arc<MarketData>;
pub type SharedOrderCommand = Arc<OrderCommand>;
pub type SharedExecutionReport = Arc<ExecutionReport>;
pub type SharedPositionUpdate = Arc<PositionUpdate>;
pub type SharedRiskAlert = Arc<RiskAlert>;
pub type SharedControlCommand = Arc<ControlCommand>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_data_creation_and_validation() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Valid market data
        let market_data = MarketData::new(
            "BTCUSDT".to_string(),
            timestamp,
            1,
            session_id,
            Decimal::new(50000, 0), // open
            Decimal::new(51000, 0), // high
            Decimal::new(49000, 0), // low
            Decimal::new(50500, 0), // close
            Decimal::new(100, 0),   // volume
        );

        assert!(market_data.is_ok());
        let data = market_data.unwrap();
        assert!(data.is_valid());
        assert_eq!(data.symbol, "BTCUSDT");
        assert_eq!(data.open, Decimal::new(50000, 0));

        // Invalid market data - high < low
        let invalid_data = MarketData::new(
            "BTCUSDT".to_string(),
            timestamp,
            1,
            session_id,
            Decimal::new(50000, 0), // open
            Decimal::new(48000, 0), // high (invalid - less than low)
            Decimal::new(49000, 0), // low
            Decimal::new(50500, 0), // close
            Decimal::new(100, 0),   // volume
        );

        assert!(invalid_data.is_err());

        // Invalid market data - empty symbol
        let invalid_symbol = MarketData::new(
            "".to_string(),
            timestamp,
            1,
            session_id,
            Decimal::new(50000, 0),
            Decimal::new(51000, 0),
            Decimal::new(49000, 0),
            Decimal::new(50500, 0),
            Decimal::new(100, 0),
        );

        assert!(invalid_symbol.is_err());

        // Invalid market data - negative volume
        let negative_volume = MarketData::new(
            "BTCUSDT".to_string(),
            timestamp,
            1,
            session_id,
            Decimal::new(50000, 0),
            Decimal::new(51000, 0),
            Decimal::new(49000, 0),
            Decimal::new(50500, 0),
            Decimal::new(-100, 0), // negative volume
        );

        assert!(negative_volume.is_err());
    }

    #[test]
    fn test_order_command_creation_and_validation() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Valid limit order
        let limit_order = OrderCommand::new(
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::new(1, 1), // 0.1
            OrderType::Limit,
            Some(Decimal::new(50000, 0)),
            timestamp,
            1,
            session_id,
        );

        assert!(limit_order.is_ok());
        let order = limit_order.unwrap();
        assert!(order.is_valid());
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.order_type, OrderType::Limit);

        // Valid market order
        let market_order = OrderCommand::new(
            "BTCUSDT".to_string(),
            OrderSide::Sell,
            Decimal::new(1, 1),
            OrderType::Market,
            None, // No price for market order
            timestamp,
            2,
            session_id,
        );

        assert!(market_order.is_ok());
        assert!(market_order.unwrap().is_valid());

        // Invalid limit order - no price
        let invalid_limit = OrderCommand::new(
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::new(1, 1),
            OrderType::Limit,
            None, // Missing price for limit order
            timestamp,
            3,
            session_id,
        );

        assert!(invalid_limit.is_err());

        // Invalid order - zero quantity
        let zero_quantity = OrderCommand::new(
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::ZERO,
            OrderType::Market,
            None,
            timestamp,
            4,
            session_id,
        );

        assert!(zero_quantity.is_err());
    }

    #[test]
    fn test_execution_report_creation_and_validation() {
        let order_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Valid filled execution
        let filled_report = ExecutionReport::new(
            order_id,
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::new(1, 1),
            Some(Decimal::new(50000, 0)),
            OrderStatus::Filled,
            timestamp,
            1,
            session_id,
        );

        assert!(filled_report.is_ok());
        let report = filled_report.unwrap();
        assert!(report.is_valid());
        assert_eq!(report.status, OrderStatus::Filled);

        // Invalid filled report - no price
        let invalid_filled = ExecutionReport::new(
            order_id,
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::new(1, 1),
            None, // Missing price for filled order
            OrderStatus::Filled,
            timestamp,
            2,
            session_id,
        );

        assert!(invalid_filled.is_err());

        // Valid pending report - no price needed
        let pending_report = ExecutionReport::new(
            order_id,
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::new(1, 1),
            None,
            OrderStatus::Pending,
            timestamp,
            3,
            session_id,
        );

        assert!(pending_report.is_ok());
        assert!(pending_report.unwrap().is_valid());
    }

    #[test]
    fn test_position_update_creation_and_validation() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Valid long position
        let long_position = PositionUpdate::new(
            "BTCUSDT".to_string(),
            Decimal::new(5, 1),     // 0.5 BTC
            Decimal::new(50000, 0), // avg price
            Decimal::new(100, 0),   // unrealized PnL
            timestamp,
            1,
            session_id,
        );

        assert!(long_position.is_ok());
        let pos = long_position.unwrap();
        assert!(pos.is_valid());
        assert!(pos.is_long());
        assert!(!pos.is_short());
        assert!(!pos.is_flat());

        // Valid flat position
        let flat_position = PositionUpdate::new(
            "BTCUSDT".to_string(),
            Decimal::ZERO,  // no position
            Decimal::ZERO,  // no avg price
            Decimal::ZERO,  // no PnL
            timestamp,
            2,
            session_id,
        );

        assert!(flat_position.is_ok());
        let flat = flat_position.unwrap();
        assert!(flat.is_valid());
        assert!(flat.is_flat());

        // Valid short position
        let short_position = PositionUpdate::new(
            "BTCUSDT".to_string(),
            Decimal::new(-5, 1),    // -0.5 BTC
            Decimal::new(50000, 0), // avg price
            Decimal::new(-100, 0),  // unrealized PnL
            timestamp,
            3,
            session_id,
        );

        assert!(short_position.is_ok());
        let short = short_position.unwrap();
        assert!(short.is_valid());
        assert!(short.is_short());
        assert!(!short.is_long());
        assert!(!short.is_flat());

        // Invalid position - non-zero quantity with zero price
        let invalid_position = PositionUpdate::new(
            "BTCUSDT".to_string(),
            Decimal::new(5, 1), // non-zero quantity
            Decimal::ZERO,      // zero avg price (invalid)
            Decimal::ZERO,
            timestamp,
            4,
            session_id,
        );

        assert!(invalid_position.is_err());
    }

    #[test]
    fn test_risk_alert_creation_and_validation() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Valid risk alert
        let alert = RiskAlert::new(
            RiskAlertType::PositionSizeLimit,
            "Position size exceeds limit".to_string(),
            RiskSeverity::Warning,
            timestamp,
            1,
            session_id,
        );

        assert!(alert.is_ok());
        let risk_alert = alert.unwrap();
        assert!(risk_alert.is_valid());
        assert!(!risk_alert.is_critical());
        assert!(!risk_alert.requires_action());

        // Critical alert
        let critical_alert = RiskAlert::new(
            RiskAlertType::DrawdownLimit,
            "Critical drawdown reached".to_string(),
            RiskSeverity::Critical,
            timestamp,
            2,
            session_id,
        );

        assert!(critical_alert.is_ok());
        let critical = critical_alert.unwrap();
        assert!(critical.is_critical());
        assert!(critical.requires_action());

        // Invalid alert - empty message
        let invalid_alert = RiskAlert::new(
            RiskAlertType::CustomRule,
            "".to_string(), // empty message
            RiskSeverity::Info,
            timestamp,
            3,
            session_id,
        );

        assert!(invalid_alert.is_err());
    }

    #[test]
    fn test_control_command_creation_and_validation() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();
        let mut parameters = HashMap::new();
        parameters.insert("strategy".to_string(), "moving_average".to_string());

        // Valid control command
        let command = ControlCommand::new(
            ControlCommandType::Start,
            parameters.clone(),
            timestamp,
            1,
            session_id,
        );

        assert!(command.is_ok());
        let mut ctrl_cmd = command.unwrap();
        assert!(ctrl_cmd.is_valid());
        assert!(!ctrl_cmd.is_emergency());
        assert_eq!(
            ctrl_cmd.get_parameter("strategy"),
            Some(&"moving_average".to_string())
        );

        // Test parameter setting
        assert!(ctrl_cmd
            .set_parameter("timeout".to_string(), "30".to_string())
            .is_ok());
        assert_eq!(ctrl_cmd.get_parameter("timeout"), Some(&"30".to_string()));

        // Emergency command
        let emergency = ControlCommand::new(
            ControlCommandType::EmergencyStop,
            HashMap::new(),
            timestamp,
            2,
            session_id,
        );

        assert!(emergency.is_ok());
        assert!(emergency.unwrap().is_emergency());

        // Invalid command - empty parameter key
        let mut invalid_params = HashMap::new();
        invalid_params.insert("".to_string(), "value".to_string());

        let invalid_command = ControlCommand::new(
            ControlCommandType::Start,
            invalid_params,
            timestamp,
            3,
            session_id,
        );

        assert!(invalid_command.is_err());
    }

    #[test]
    fn test_serialization_deserialization() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Test MarketData serialization
        let market_data = MarketData::new(
            "BTCUSDT".to_string(),
            timestamp,
            1,
            session_id,
            Decimal::new(50000, 0),
            Decimal::new(51000, 0),
            Decimal::new(49000, 0),
            Decimal::new(50500, 0),
            Decimal::new(100, 0),
        )
        .unwrap();

        let json = serde_json::to_string(&market_data).expect("Failed to serialize MarketData");
        let deserialized: MarketData =
            serde_json::from_str(&json).expect("Failed to deserialize MarketData");
        assert_eq!(market_data, deserialized);

        // Test OrderCommand serialization
        let order_command = OrderCommand::new(
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            Decimal::new(1, 1),
            OrderType::Limit,
            Some(Decimal::new(50000, 0)),
            timestamp,
            1,
            session_id,
        )
        .unwrap();

        let json = serde_json::to_string(&order_command).expect("Failed to serialize OrderCommand");
        let deserialized: OrderCommand =
            serde_json::from_str(&json).expect("Failed to deserialize OrderCommand");
        assert_eq!(order_command, deserialized);
    }

    #[test]
    fn test_arc_wrapped_types() {
        let session_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let market_data = MarketData::new(
            "BTCUSDT".to_string(),
            timestamp,
            1,
            session_id,
            Decimal::new(50000, 0),
            Decimal::new(51000, 0),
            Decimal::new(49000, 0),
            Decimal::new(50500, 0),
            Decimal::new(100, 0),
        )
        .unwrap();

        // Test Arc wrapping
        let shared_data: SharedMarketData = Arc::new(market_data);
        let cloned_shared = Arc::clone(&shared_data);

        assert_eq!(shared_data.symbol, cloned_shared.symbol);
        assert_eq!(Arc::strong_count(&shared_data), 2);
    }
}