# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

This is a high-performance AI Trading Agent written in Rust, implementing an event-driven architecture for automated trading with AutoML optimization capabilities. The system is designed to run unchanged across backtest, paper trading, and live trading modes.

## Architecture Overview

The system follows an event-driven architecture with these core components:

1. **Event Bus**: Central message bus using Tokio channels for in-process pub/sub communication
2. **Market Data Feed**: Normalizes data from exchanges/files into unified MarketDataEvent streams
3. **Strategy Agent**: Pluggable strategies implementing a common trait, consuming market data and producing OrderCommand events
4. **Execution Engine**: Translates commands to exchange API calls or simulated fills
5. **Risk Management**: Pre-trade validation and real-time portfolio protection
6. **Optimization Engine**: AutoML-style parameter optimization using grid search and future Bayesian methods

Key design principles:
- All components communicate via events, not direct calls
- Strategies are mode-agnostic (same code for backtest/live)
- Risk checks happen before execution
- Performance target: <10ms latency, 5k events/sec throughput

## Development Commands

```bash
# Project setup (once Cargo.toml exists)
cargo build                  # Build the project
cargo build --release       # Build optimized binary
cargo test                  # Run all tests
cargo test <module>         # Run specific module tests
cargo bench                 # Run performance benchmarks
cargo clippy                # Lint code
cargo fmt                   # Format code

# Running the trading agent (once implemented)
cargo run -- backtest --config config.toml    # Run backtest
cargo run -- optimize --config config.toml    # Run optimization
cargo run -- paper --config config.toml       # Paper trading
cargo run -- live --config config.toml        # Live trading (use with caution)

# Development workflow
cargo watch -x test         # Auto-run tests on file changes
cargo watch -x clippy       # Auto-lint on changes
```

## Critical Implementation Notes

### Event Types
All events should include:
- `timestamp`: When the event occurred
- `sequence`: Monotonic sequence number for ordering
- Appropriate typed payload (use rust_decimal for money)

### Strategy Interface
```rust
pub trait Strategy {
    fn on_market_data(&mut self, data: MarketDataEvent, ctx: &Context) -> Vec<OrderCommand>;
    fn on_execution_update(&mut self, update: ExecutionReport, ctx: &Context) -> Vec<OrderCommand>;
}
```

### Risk Management
Every OrderCommand must pass through risk validation before execution. Never bypass risk checks, even in backtest mode.

### Performance Considerations
- Use `rust_decimal` for financial calculations, not f64
- Prefer bounded channels to prevent memory issues
- Use last-N windows for indicators, not unbounded history
- Consider using `Arc<T>` for large shared data to avoid copies

### Testing Requirements
- Unit tests for all mathematical functions (indicators, P&L calculations)
- Integration tests for full backtest scenarios
- Deterministic backtests must produce identical results
- Test risk scenarios explicitly (drawdown limits, position limits)

## Configuration Structure

The system uses TOML configuration with environment variable interpolation:

```toml
[system]
mode = "backtest"  # backtest, paper, testnet, live

[market]
source = "csv"      # csv, binance, testnet
path = "data/..."   # for csv source

[strategy]
name = "moving_average"
# strategy-specific params

[risk]
max_position_size = 1.0
max_drawdown = 0.10

[execution]
commission = 0.001
```

## Project Status

Currently in planning phase. The PRD is complete, but implementation has not begun. Priority order:
1. Core event system and domain models
2. CSV-based backtesting engine
3. Simple strategy implementation
4. CLI interface
5. Risk management
6. Live exchange integration

## Exchange Integration Priority

First exchange to implement: Binance (most comprehensive API and documentation)

## Data Standards

- Timestamps: Always UTC, ISO 8601 format
- CSV format: timestamp,open,high,low,close,volume
- Missing data: Forward-fill prices, zero-fill volumes
- Decimal precision: 8 decimal places for crypto