# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> Think carefully and implement the most concise solution that changes as little code as possible.

## USE SUB-AGENTS FOR CONTEXT OPTIMIZATION

### 1. Always use the file-analyzer sub-agent when asked to read files.
The file-analyzer agent is an expert in extracting and summarizing critical information from files, particularly log files and verbose outputs.

### 2. Always use the code-analyzer sub-agent when asked to search code, analyze code, research bugs, or trace logic flow.
The code-analyzer agent is an expert in code analysis, logic tracing, and vulnerability detection.

### 3. Always use the test-runner sub-agent to run tests and analyze the test results.
Using the test-runner agent ensures full test output is captured for debugging while keeping the main conversation clean.

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
# Build & Compile
cargo build                           # Debug build
cargo build --release                 # Optimized build with LTO
cargo build --features "python-ml"    # With Python ML integration

# Code Quality (Required before commits)
cargo fmt --all                       # Format code (4-space indent)
cargo clippy -- -D warnings           # Lint with warnings as errors
cargo audit                           # Security audit dependencies

# Testing
cargo test                            # Run all tests
cargo test <module>                   # Run specific module tests
cargo test -- --ignored               # Run long-running tests
cargo test -- --nocapture            # Show test output
cargo bench                          # Run performance benchmarks

# Running the trading agent (once implemented)
cargo run -- backtest -c config/backtest.toml    # Backtest mode
cargo run -- optimize -c config/optimize.toml    # Parameter optimization
cargo run -- paper -c config/paper.toml          # Paper trading
cargo run -- testnet -c config/testnet.toml      # Exchange testnet
cargo run -- live -c config/prod.toml            # Live trading (requires confirmation)

# Development workflow
cargo watch -x test                  # Auto-run tests on changes
cargo watch -x "clippy -- -D warnings"  # Auto-lint on changes
```

## Critical Implementation Notes

### ABSOLUTE RULES
- **NO PARTIAL IMPLEMENTATION** - Complete every function fully
- **NO SIMPLIFICATION** - No placeholder code or "simplified for now" comments
- **NO CODE DUPLICATION** - Check existing codebase, reuse functions and constants
- **NO DEAD CODE** - Either use it or delete it completely
- **IMPLEMENT TESTS FOR EVERY FUNCTION** - No exceptions
- **NO CHEATER TESTS** - Tests must be accurate, reflect real usage, be verbose for debugging
- **NO INCONSISTENT NAMING** - Follow Rust conventions strictly
- **NO OVER-ENGINEERING** - Simple solutions over complex abstractions
- **NO MIXED CONCERNS** - Proper separation of concerns always
- **NO RESOURCE LEAKS** - Always clean up: close connections, drop guards, etc.

### Event Types
All events must include:
- `timestamp`: UTC timestamp (chrono::DateTime<Utc>)
- `sequence`: Monotonic u64 sequence number for ordering
- `instrument`: String identifier for the trading pair
- Appropriate typed payload (use rust_decimal::Decimal for money)

### Strategy Interface
```rust
pub trait Strategy: Send + Sync {
    fn on_market_data(&mut self, data: MarketDataEvent, ctx: &Context) -> Result<Vec<OrderCommand>>;
    fn on_execution_update(&mut self, update: ExecutionReport, ctx: &Context) -> Result<Vec<OrderCommand>>;
    fn on_risk_update(&mut self, alert: RiskAlert, ctx: &Context) -> Result<Vec<OrderCommand>>;
}
```

### Error Handling
- **Always return Result<T, Error>** - No unwrap() or expect() in production paths
- **Use thiserror for custom errors** - Proper error context propagation
- **Fail fast on critical config** - Missing required settings should error immediately
- **Log and continue on recoverable** - Network timeouts, reconnectable issues
- **Graceful degradation** - System remains operational with reduced functionality

### Financial Types & Math
- **Use rust_decimal::Decimal** - NEVER use f64 for money/prices
- **Explicit precision** - Maintain 8 decimal places for crypto
- **Check for overflow** - Use checked_* arithmetic operations
- **Idempotent operations** - All exchange operations must use client_order_id

### Performance Requirements
- **Latency target**: <10ms from market data to order signal (p50)
- **Throughput**: ≥5,000 events/sec sustained in backtest
- **Memory**: Bounded queues (max 10k items), last-N windows for indicators
- **Concurrency**: Prefer async tasks, isolate CPU work via spawn_blocking
- **Zero-copy where possible**: Use Arc<T> for large shared data

### Testing Requirements
- **Unit tests**: All math functions, indicators, risk rules (#[cfg(test)])
- **Integration tests**: End-to-end backtests with known outcomes (tests/)
- **Deterministic**: Backtests must be 100% reproducible (fix seeds, no randomness)
- **Property tests**: Use proptest for invariants (P&L conservation, position math)
- **Risk scenarios**: Explicit tests for drawdown, position limits, circuit breakers
- **Performance tests**: Benchmarks for hot paths (benches/)

## Configuration Structure

The system uses TOML configuration with environment variable interpolation (${VAR_NAME}):

```toml
[system]
mode = "backtest"           # backtest, paper, testnet, live
log_level = "info"          # trace, debug, info, warn, error

[market]
source = "csv"              # csv, binance, coinbase, kraken
path = "data/BTCUSDT.csv"   # for csv source
symbols = ["BTC/USDT", "ETH/USDT"]

[strategy]
name = "moving_average_crossover"
fast_period = 20
slow_period = 50
position_size = 0.1         # 10% of capital per trade

[risk]
max_position_size = 1.0     # Max 1 BTC position
max_drawdown = 0.10         # 10% drawdown limit
max_daily_loss = 0.05       # 5% daily loss limit
position_limit_usd = 50000  # $50k max per position

[execution]
commission = 0.001          # 0.1% taker fee
slippage = 0.0001          # Expected slippage
order_timeout_ms = 5000     # Cancel if not filled in 5s

[exchange.binance]
api_key = "${BINANCE_API_KEY}"
api_secret = "${BINANCE_API_SECRET}"
testnet = false
ws_url = "wss://stream.binance.com:9443/ws"
```

## Event Flow & Message Topics

```
market.* → Strategy → orders.request → RiskManager → orders.approved → ExecutionEngine
                                                  ↓ (if rejected)
                                              risk.alert

ExecutionEngine → orders.executed → PositionManager → positions.update
              ↓
        execution.report
```

Key event topics:
- `market.{symbol}` - Market data events per instrument
- `orders.request` - New order requests from strategies
- `orders.approved` - Risk-approved orders
- `orders.executed` - Completed executions
- `positions.update` - Position changes
- `risk.alert` - Risk violations or warnings
- `metrics.update` - Performance metrics updates

## Implementation Phases

### Phase 1: Core Infrastructure (MVP)
1. Event bus with Tokio channels
2. Domain models (Order, Position, Portfolio)
3. CSV market data reader
4. Simulated execution engine
5. Basic moving average strategy
6. CLI with backtest command

### Phase 2: Risk & Optimization
1. Risk management module
2. Position tracking & P&L
3. Grid search optimization
4. Performance metrics & reporting
5. Integration tests

### Phase 3: Live Trading
1. Binance WebSocket integration
2. Order lifecycle management
3. State synchronization
4. REST API & monitoring
5. Docker containerization

## Data Standards

- **Timestamps**: UTC only, chrono::DateTime<Utc>, ISO 8601 in logs/JSON
- **CSV Format**: `timestamp,symbol,open,high,low,close,volume,quote_volume`
- **Missing Data**: Forward-fill prices, zero-fill volumes, log gaps
- **Decimal Precision**: 8 places for crypto prices, 4 for quantities
- **Instrument Format**: `BASE/QUOTE` (e.g., "BTC/USDT")

## Key Dependencies

```toml
[dependencies]
tokio = { version = "1.40", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
rust_decimal = "1.36"
chrono = { version = "0.4", features = ["serde"] }
anyhow = "1.0"
thiserror = "2.0"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
clap = { version = "4.5", features = ["derive"] }

[dev-dependencies]
proptest = "1.5"
criterion = "0.5"
```