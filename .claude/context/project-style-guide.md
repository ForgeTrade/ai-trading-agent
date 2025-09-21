---
created: 2025-09-21T09:04:16Z
last_updated: 2025-09-21T09:04:16Z
version: 1.0
author: Claude Code PM System
---

# Project Style Guide

## Rust Code Style

### General Principles
- **Clarity over cleverness**: Write code that is easy to understand
- **Explicit over implicit**: Be clear about intent and behavior
- **Performance with safety**: Optimize hot paths but maintain safety
- **Minimal dependencies**: Only add dependencies when truly needed

### Naming Conventions

#### Files and Modules
- **Module files**: `snake_case.rs` (e.g., `event_bus.rs`, `market_data.rs`)
- **Module directories**: `snake_case/` with `mod.rs`
- **Test files**: `{module}_test.rs` or in `tests/` directory
- **Benchmark files**: `{module}_bench.rs` in `benches/`

#### Code Elements
```rust
// Structs: PascalCase
struct MarketDataEvent { ... }

// Enums: PascalCase
enum OrderStatus { ... }

// Traits: PascalCase
trait Strategy { ... }

// Functions: snake_case
fn calculate_moving_average() { ... }

// Constants: SCREAMING_SNAKE_CASE
const MAX_RECONNECT_ATTEMPTS: u32 = 5;

// Static: SCREAMING_SNAKE_CASE
static DEFAULT_CONFIG: &str = "config.toml";

// Variables: snake_case
let order_id = generate_id();

// Type parameters: Single capital letter or PascalCase
fn process<T: Strategy>(strategy: T) { ... }
fn convert<Input, Output>(data: Input) -> Output { ... }
```

### Code Organization

#### Module Structure
```rust
// 1. Imports (grouped and alphabetized)
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::events::EventPayload;
use crate::types::Price;

// 2. Module declarations
mod channels;
mod router;

// 3. Re-exports
pub use channels::ChannelConfig;
pub use router::Router;

// 4. Constants and statics
const BUFFER_SIZE: usize = 1024;

// 5. Type definitions
type Result<T> = std::result::Result<T, Error>;

// 6. Traits
pub trait MessageBus {
    async fn publish(&self, event: EventPayload) -> Result<()>;
}

// 7. Structs and enums
pub struct InProcessBus {
    // fields...
}

// 8. Implementations
impl InProcessBus {
    // methods...
}

impl MessageBus for InProcessBus {
    // trait methods...
}

// 9. Free functions
pub fn create_bus(config: Config) -> Result<InProcessBus> {
    // ...
}

// 10. Tests (or in separate file)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bus_creation() {
        // ...
    }
}
```

### Error Handling

#### Error Types
```rust
// Use thiserror for error definitions
#[derive(Debug, thiserror::Error)]
pub enum EventBusError {
    #[error("Channel full: {channel}")]
    ChannelFull { channel: String },

    #[error("Subscriber disconnected")]
    SubscriberDisconnected,

    #[error("Invalid topic: {0}")]
    InvalidTopic(String),
}

// Use Result type alias
pub type Result<T> = std::result::Result<T, EventBusError>;
```

#### Error Propagation
```rust
// Use ? for propagation
fn process_order(order: Order) -> Result<ExecutionReport> {
    validate_order(&order)?;
    let result = execute_order(order)?;
    Ok(result)
}

// Add context with anyhow
fn read_config(path: &str) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)
        .context("Failed to read config file")?;
    let config = toml::from_str(&content)
        .context("Failed to parse config")?;
    Ok(config)
}
```

### Async Code

#### Async Functions
```rust
// Always use async/await over raw futures
pub async fn fetch_market_data(symbol: &str) -> Result<MarketData> {
    let response = client.get(url).send().await?;
    let data = response.json::<MarketData>().await?;
    Ok(data)
}

// Use tokio::select! for multiple futures
tokio::select! {
    Some(msg) = receiver.recv() => handle_message(msg),
    _ = shutdown_signal() => break,
}
```

#### Task Spawning
```rust
// Name spawned tasks for debugging
tokio::spawn(async move {
    // task code
}.instrument(tracing::info_span!("market_data_processor")));

// Use JoinHandle for task management
let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
    // task code
});
```

### Performance Patterns

#### Zero-Copy with Arc
```rust
// Share immutable data with Arc
let market_data = Arc::new(MarketDataEvent { ... });
let data_clone = Arc::clone(&market_data);

// Use Arc for large structures
pub struct LargeEvent {
    data: Arc<MarketSnapshot>,
}
```

#### Avoid Allocations
```rust
// Reuse buffers
let mut buffer = Vec::with_capacity(1024);
buffer.clear();  // Reuse instead of reallocating

// Use SmallVec for small collections
use smallvec::SmallVec;
let items: SmallVec<[Order; 4]> = SmallVec::new();
```

### Testing Style

#### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_validation_valid() {
        // Arrange
        let order = Order {
            symbol: "BTC/USD".to_string(),
            quantity: Decimal::from(1),
            price: Decimal::from(50000),
        };

        // Act
        let result = validate_order(&order);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn test_order_validation_invalid_quantity() {
        // Test negative cases
        let order = Order {
            quantity: Decimal::from(-1),
            // ...
        };

        let result = validate_order(&order);
        assert!(matches!(result, Err(ValidationError::InvalidQuantity(_))));
    }
}
```

#### Integration Tests
```rust
// tests/integration/event_bus_test.rs
#[tokio::test]
async fn test_event_bus_pubsub() {
    // Setup
    let bus = create_test_bus().await;
    let mut subscriber = bus.subscribe(Topic::MarketData).await.unwrap();

    // Execute
    let event = create_test_event();
    bus.publish(Topic::MarketData, event.clone()).await.unwrap();

    // Verify
    let received = subscriber.recv().await.unwrap();
    assert_eq!(received, event);
}
```

### Documentation

#### Module Documentation
```rust
//! Event bus implementation for the trading system.
//!
//! This module provides the core messaging infrastructure that enables
//! communication between different components of the trading system.
//!
//! # Examples
//!
//! ```
//! use trading::event_bus::{EventBus, Topic};
//!
//! let bus = EventBus::new();
//! let subscriber = bus.subscribe(Topic::MarketData).await?;
//! ```
```

#### Function Documentation
```rust
/// Calculates the exponential moving average for the given period.
///
/// # Arguments
///
/// * `values` - Slice of price values
/// * `period` - Number of periods for the EMA
///
/// # Returns
///
/// The calculated EMA value or None if insufficient data
///
/// # Examples
///
/// ```
/// let prices = vec![100.0, 102.0, 101.0];
/// let ema = calculate_ema(&prices, 2);
/// assert_eq!(ema, Some(101.5));
/// ```
pub fn calculate_ema(values: &[f64], period: usize) -> Option<f64> {
    // Implementation
}
```

### Logging and Metrics

#### Structured Logging
```rust
use tracing::{info, warn, error, debug, instrument};

#[instrument(skip(order))]
pub async fn process_order(order: Order) -> Result<()> {
    info!(
        order_id = %order.id,
        symbol = %order.symbol,
        quantity = ?order.quantity,
        "Processing order"
    );

    // Process order

    info!(order_id = %order.id, "Order processed successfully");
    Ok(())
}
```

#### Metrics
```rust
use metrics::{counter, histogram, gauge};

pub fn record_order_metrics(order: &Order, latency: Duration) {
    counter!("orders_total", 1, "symbol" => order.symbol.clone());
    histogram!("order_latency_seconds", latency.as_secs_f64());
    gauge!("active_orders", active_count as f64);
}
```

## Git Commit Style

### Commit Message Format
```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation only
- **style**: Formatting, missing semi colons, etc
- **refactor**: Code change that neither fixes a bug nor adds a feature
- **perf**: Performance improvement
- **test**: Adding missing tests
- **chore**: Changes to build process or auxiliary tools

### Examples
```
feat(event-bus): add fan-out router for multi-subscriber topics

Implement TypedFanoutRouter using DashMap for concurrent routing.
Includes automatic slow subscriber detection and detachment after
3 consecutive timeouts.

Closes #5
```

## File Headers

### License Header (if applicable)
```rust
// Copyright (c) 2024 AI Trading Agent Contributors
// SPDX-License-Identifier: MIT
```

### File Description
```rust
//! Market data normalization and distribution.
//!
//! This module handles incoming market data from various sources,
//! normalizes it to a common format, and distributes it via the event bus.
```

## Configuration Files

### TOML Style
```toml
# Global configuration
[system]
mode = "backtest"  # Options: backtest, paper, live
log_level = "info"

# Market data configuration
[market]
source = "csv"
path = "data/btcusd_1h.csv"

  # Nested configuration
  [market.csv]
  delimiter = ","
  has_header = true

# Strategy configuration
[strategy.moving_average]
fast_period = 10
slow_period = 20
```

## Documentation Files

### Markdown Style
- Use ATX-style headers (`#`, `##`, `###`)
- Include YAML frontmatter for metadata
- Use code blocks with language hints
- Keep lines under 100 characters
- Use reference-style links for repeated URLs

### Example
```markdown
---
title: Event Bus Architecture
author: Trading Team
date: 2024-01-01
---

# Event Bus Architecture

## Overview
The event bus provides decoupled communication between components...

## Implementation Details
```rust
// Code example
```

See [Tokio documentation][tokio] for more details.

[tokio]: https://tokio.rs
```