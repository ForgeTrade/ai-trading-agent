---
created: 2025-09-21T09:04:16Z
last_updated: 2025-09-21T09:04:16Z
version: 1.0
author: Claude Code PM System
---

# System Patterns

## Architectural Style
**Event-Driven Architecture (EDA)** with **Domain-Driven Design (DDD)**
- Loosely coupled components communicating via events
- Clear domain boundaries with bounded contexts
- Event sourcing for audit trail and replay capability

## Core Design Patterns

### 1. Message Bus Pattern
**Implementation**: In-process pub/sub with pluggable transport
```rust
trait MessageBus {
    async fn publish(&self, topic: Topic, event: EventPayload) -> Result<()>;
    async fn subscribe(&self, topic: Topic) -> Result<BusStream<EventPayload>>;
}
```
- Decouples publishers from subscribers
- Enables testing with mock bus
- Supports future migration to external bus (Kafka/NATS)

### 2. Strategy Pattern
**Implementation**: Trait-based strategy abstraction
```rust
trait Strategy {
    fn on_market_data(&mut self, data: MarketDataEvent, ctx: &Context) -> Vec<OrderCommand>;
    fn on_execution_update(&mut self, update: ExecutionReport, ctx: &Context) -> Vec<OrderCommand>;
}
```
- Strategies are pluggable and composable
- Same interface for backtest/paper/live modes
- Context provides mode-specific services

### 3. Adapter Pattern
**Implementation**: Mode-specific implementations behind common traits
- `MarketDataSource` trait with CSV/Exchange implementations
- `ExecutionEngine` trait with Simulated/Live implementations
- `RiskManager` trait with configurable rule sets

### 4. Observer Pattern
**Implementation**: Event subscription with typed streams
- Components subscribe to specific event topics
- Automatic cleanup on stream drop
- Backpressure handling via bounded channels

### 5. Command Pattern
**Implementation**: OrderCommand events encapsulate trading actions
- Commands validated before execution
- Support for undo/replay in backtest mode
- Audit trail of all commands issued

## Concurrency Patterns

### 1. Actor Model (Simplified)
- Each component runs in its own async task
- Communicates only via message passing
- No shared mutable state between actors

### 2. Fan-Out/Fan-In
**TypedFanoutRouter**: Distributes events to multiple subscribers
- DashMap for lock-free concurrent routing
- Automatic slow subscriber detection
- Configurable timeout and retry policies

### 3. Circuit Breaker
**For external services (exchanges)**
- Automatic failure detection
- Exponential backoff on errors
- Graceful degradation to paper mode

## Data Flow Patterns

### 1. Pipeline Pattern
```
Raw Data → Normalize → Validate → Process → Execute → Report
```
Each stage is independent and testable

### 2. Event Sourcing
- All state changes via events
- Complete audit trail
- Replay capability for debugging

### 3. CQRS-lite
- Commands (OrderCommand) separate from queries
- Event-driven state updates
- Read models for reporting

## Memory Management Patterns

### 1. Zero-Copy with Arc
```rust
Arc<MarketDataEvent>  // Shared across subscribers
```
- Avoids cloning large market data structures
- Reference counting for automatic cleanup

### 2. Object Pooling
- Pre-allocated event buffers
- Reusable order objects
- Reduced allocation pressure in hot paths

### 3. Copy-on-Write
- Configuration updates without restart
- Strategy parameter tuning
- Risk limit adjustments

## Error Handling Patterns

### 1. Result-Based Error Propagation
```rust
Result<T, Error>  // Explicit error handling
```
- No exceptions or panics in normal flow
- Errors are values, not control flow

### 2. Error Context Chain
```rust
.context("Failed to parse market data")
.context("Processing tick at timestamp X")
```
- Rich error messages for debugging
- Preserves error chain for root cause analysis

### 3. Graceful Degradation
- Network errors → retry with backoff
- Exchange unavailable → switch to paper mode
- Strategy error → isolate and continue others

## Testing Patterns

### 1. Dependency Injection
- All dependencies passed via constructor
- Easy mocking for unit tests
- Test doubles for external services

### 2. Deterministic Testing
- Controlled time advancement
- Seeded random number generation
- Reproducible event sequences

### 3. Property-Based Testing
```rust
proptest! {
    fn test_order_validation(order in arbitrary_order()) {
        // Properties that must hold for all orders
    }
}
```

## Performance Patterns

### 1. Lazy Evaluation
- Indicators calculated on-demand
- Cached results with TTL
- Incremental updates where possible

### 2. Batch Processing
- Group market data updates
- Bulk order submissions
- Amortized validation costs

### 3. Lock-Free Data Structures
- DashMap for concurrent access
- Arc for immutable sharing
- MPMC queues for work distribution

## Configuration Patterns

### 1. Hierarchical Configuration
```toml
[default]
[backtest]
[paper]
[live]
```
- Environment-specific overrides
- Sensible defaults
- Runtime reloading

### 2. Feature Flags
```rust
#[cfg(feature = "deterministic")]
```
- Compile-time feature selection
- Zero-cost abstractions
- Testability improvements

## Risk Management Patterns

### 1. Pre-Trade Validation
- Check limits before submission
- Validate strategy state
- Ensure market hours

### 2. Real-Time Monitoring
- Position limits
- Exposure tracking
- Drawdown detection

### 3. Kill Switch
- Emergency stop button
- Graceful position unwinding
- Audit trail of activation

## Observability Patterns

### 1. Structured Logging
```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "level": "INFO",
  "component": "execution_engine",
  "event": "order_filled",
  "order_id": "123",
  "correlation_id": "abc"
}
```

### 2. Distributed Tracing
- Span per event processing
- Correlation IDs threading through system
- Performance bottleneck identification

### 3. Metrics Collection
- RED metrics (Rate, Errors, Duration)
- Business metrics (P&L, win rate, Sharpe)
- System metrics (CPU, memory, latency)

## Security Patterns

### 1. Principle of Least Privilege
- Components only access what they need
- Read-only access where possible
- Separate credentials per environment

### 2. Defense in Depth
- Multiple validation layers
- Rate limiting at every boundary
- Audit logging of all actions

### 3. Secure by Default
- TLS everywhere
- Encrypted secrets
- No unsafe code without review