---
name: event-bus-and-domain-events-v1
status: backlog
created: 2025-09-21T08:42:57Z
progress: 0%
prd: .claude/prds/event-bus-and-domain-events-v1.md
github: https://github.com/ForgeTrade/ai-trading-agent/issues/1
---

# Epic: event-bus-and-domain-events-v1

## Overview
Implement a high-performance, type-safe event bus using Tokio channels optimized for trading system requirements. The architecture leverages Rust's zero-cost abstractions with mpsc for lossless critical paths and broadcast for high-throughput market data, achieving <1ms latency at 5-10k events/sec.

## Architecture Decisions

### Core Design Choices
- **Channel Strategy**: Tokio mpsc for lossless topics, broadcast for lossy market data
- **Type Safety**: Topic enum with EventPayload sum type eliminates runtime routing errors
- **Memory Optimization**: Arc<T> wrapping for hot paths avoids cloning large market data
- **Contention Mitigation**: DashMap for fan-out routing instead of RwLock<Vec>
- **Determinism**: Feature-gated synchronous bus for reproducible backtesting

### Technology Stack
- **Runtime**: Tokio 1.x (async runtime with channels)
- **Serialization**: Serde for structured logging only (no wire protocol)
- **Metrics**: metrics crate with Prometheus exporter
- **Tracing**: tracing crate with spans for flow visualization
- **Concurrency**: DashMap for low-contention concurrent access
- **Numerics**: rust_decimal for financial precision

### Simplification Opportunities
- **No External Broker**: Pure in-process channels eliminate network/serialization overhead
- **Single EventPayload Type**: Avoids type erasure and dynamic dispatch complexity
- **Static Topic Configuration**: Compile-time topic definitions prevent runtime errors
- **No Persistence Layer**: Events are ephemeral, simplifying state management

## Technical Approach

### Core Components

#### 1. Event Bus Core (`src/event_bus/mod.rs`)
- MessageBus trait defining publish/subscribe contract
- InProcessBus implementation with channel management
- Topic enum with associated event validation
- EventPayload sum type for all event variants

#### 2. Channel Management (`src/event_bus/channels.rs`)
- Lossless mpsc channels for critical paths (orders, executions)
- Broadcast channels with ring buffer for market data
- TypedFanoutRouter for multi-subscriber lossless topics
- Per-topic configuration loading from YAML

#### 3. Event Types (`src/events/mod.rs`)
- Domain event structs (MarketData, OrderCommand, ExecutionReport, etc.)
- Required fields: timestamp, sequence_number, session_id
- Arc wrapping for hot path events
- Serde derives for structured logging

#### 4. Stream Wrapper (`src/event_bus/stream.rs`)
- BusStream<T> unifying mpsc/broadcast receivers
- Implements Stream trait for async iteration
- recv()/try_recv() convenience methods
- Lagged error handling for broadcast

#### 5. Metrics & Observability (`src/event_bus/metrics.rs`)
- Per-topic event counters and latency histograms
- Queue depth gauges and backpressure tracking
- Prometheus exporter with bounded cardinality
- Tracing spans for event flow visualization

#### 6. Deterministic Mode (`src/event_bus/deterministic.rs`)
- Feature-gated synchronous bus for backtesting
- VecDeque-based event queue with controlled ordering
- No timeouts or async operations
- Time advancement under test control

## Implementation Strategy

### Phase 1: Foundation (Tasks 1-3)
- Define core types and traits
- Implement basic mpsc/broadcast channels
- Add event validation and routing

### Phase 2: Reliability (Tasks 4-6)
- Add fan-out router for multi-subscriber topics
- Implement backpressure and slow subscriber handling
- Add graceful shutdown with critical topic draining

### Phase 3: Observability (Tasks 7-8)
- Integrate metrics collection
- Add tracing spans and correlation IDs

### Phase 4: Testing (Tasks 9-10)
- Comprehensive unit and integration tests
- Performance benchmarks and deterministic mode

## Task Breakdown Preview

High-level implementation tasks (10 total):

- [ ] **Core Types**: Define Topic enum, EventPayload, and MessageBus trait
- [ ] **Event Structures**: Create domain event structs with required fields
- [ ] **Channel Setup**: Implement mpsc/broadcast channel management
- [ ] **Fan-out Router**: Build TypedFanoutRouter for multi-subscriber lossless topics
- [ ] **Backpressure**: Add timeout handling and slow subscriber detection
- [ ] **Shutdown**: Implement graceful shutdown with critical topic draining
- [ ] **Metrics**: Integrate metrics collection with Prometheus export
- [ ] **Stream Wrapper**: Create unified BusStream with error handling
- [ ] **Testing**: Write comprehensive unit and integration tests
- [ ] **Benchmarks**: Add performance tests and deterministic mode

## Dependencies

### External Crates
- tokio 1.x with full features
- serde 1.x for serialization
- metrics 0.23 + metrics-exporter-prometheus
- tracing 0.1 for instrumentation
- dashmap 6.1 for concurrent maps
- rust_decimal 1.35 for financial math
- chrono 0.4 for timestamps
- uuid 1.10 for correlation IDs

### Internal Dependencies
- None initially (foundational component)
- Future: Strategy, Execution, Risk modules will depend on this

## Success Criteria (Technical)

### Performance Benchmarks
- Event bus latency p99 < 1ms at 5k events/sec
- Throughput: 10k events/sec on 8-core machine
- Zero allocation in hot paths (Arc reuse)
- Memory stable over 24-hour test run

### Quality Gates
- 90% unit test coverage
- All integration tests passing
- Clippy warnings as errors
- No unsafe code outside of dependencies

### Acceptance Criteria
- Deterministic backtest replay
- Zero critical event loss under load
- Graceful degradation on overload
- Clean shutdown without data loss

## Estimated Effort

### Timeline
- **Total Duration**: 2-3 weeks
- **Phase 1 (Foundation)**: 3-4 days
- **Phase 2 (Reliability)**: 4-5 days
- **Phase 3 (Observability)**: 2-3 days
- **Phase 4 (Testing)**: 3-4 days

### Resource Requirements
- 1 senior Rust developer
- Code review from trading system expert
- Performance testing environment (8+ cores)

### Critical Path
1. Core types and traits (blocks everything)
2. Channel implementation (blocks router)
3. Fan-out router (blocks multi-subscriber)
4. Testing (blocks production use)

## Tasks Created
- [ ] [#2](https://github.com/ForgeTrade/ai-trading-agent/issues/2) - Core Types and Traits (parallel: true)
- [ ] [#3](https://github.com/ForgeTrade/ai-trading-agent/issues/3) - Domain Event Structures (parallel: true)
- [ ] [#4](https://github.com/ForgeTrade/ai-trading-agent/issues/4) - Basic Channel Implementation (parallel: false)
- [ ] [#5](https://github.com/ForgeTrade/ai-trading-agent/issues/5) - Fan-out Router Implementation (parallel: false)
- [ ] [#6](https://github.com/ForgeTrade/ai-trading-agent/issues/6) - Backpressure and Flow Control (parallel: false)
- [ ] [#7](https://github.com/ForgeTrade/ai-trading-agent/issues/7) - Graceful Shutdown (parallel: false)
- [ ] [#8](https://github.com/ForgeTrade/ai-trading-agent/issues/8) - Metrics Integration (parallel: true)
- [ ] [#9](https://github.com/ForgeTrade/ai-trading-agent/issues/9) - Stream Wrapper and Error Handling (parallel: true)
- [ ] [#10](https://github.com/ForgeTrade/ai-trading-agent/issues/10) - Comprehensive Testing (parallel: false)
- [ ] [#11](https://github.com/ForgeTrade/ai-trading-agent/issues/11) - Performance Benchmarks and Deterministic Mode (parallel: true)

Total tasks: 10
Parallel tasks: 5
Sequential tasks: 5
Estimated total effort: 108 hours (~2.5 weeks)