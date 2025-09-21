---
name: event-bus-and-domain-events-v1
description: High-performance in-memory event bus with domain events for AI trading agent architecture
status: backlog
created: 2025-09-21T07:58:17Z
---

# PRD: event-bus-and-domain-events-v1

## Executive Summary

This PRD defines the core event-driven architecture for the AI Trading Agent, implementing a high-performance in-memory event bus using Tokio channels with strongly-typed domain events. The system will enable loose coupling between components (Market Data Feed, Strategy Agent, Execution Engine, Risk Management) while maintaining sub-millisecond latencies and supporting 5-10k events/second throughput. Version 1 focuses on in-process pub/sub messaging with deterministic ordering for backtesting.

## Problem Statement

### What problem are we solving?
The trading system requires components to communicate efficiently without tight coupling. Direct function calls between Strategy, Risk, and Execution modules create maintenance challenges, make testing difficult, and prevent easy swapping of implementations. We need an architecture that allows the same code to run unchanged across backtest, paper, and live trading modes.

### Why is this important now?
- **Mode Agnosticism**: Strategies must run identically in backtest and production without code changes
- **Performance Critical**: Trading decisions require <10ms end-to-end latency to remain competitive
- **Testability**: Need deterministic event replay for backtesting validation
- **Scalability**: System must handle high-frequency market data (5k+ events/sec) without dropping critical events
- **Observability**: Need comprehensive metrics and tracing for debugging production issues

## User Stories

### Primary User Personas
1. **Quantitative Developer**: Building and testing trading strategies
2. **System Operator**: Running and monitoring live trading systems
3. **Risk Manager**: Monitoring positions and enforcing risk limits

### Detailed User Journeys

#### Strategy Developer Journey
**As a** quantitative developer
**I want to** write strategies that consume market data and produce orders via events
**So that** my strategy code works identically in backtest and production

**Acceptance Criteria:**
- Strategies receive MarketDataEvent and produce OrderCommand
- Same strategy binary runs in all modes (backtest/paper/live)
- Events include all necessary context (timestamps, sequence numbers)
- Can test strategies with recorded event streams
- Can filter subscriptions by instrument/strategy_id

#### System Operator Journey
**As a** system operator
**I want to** monitor event flow and system health in production
**So that** I can identify and resolve issues quickly

**Acceptance Criteria:**
- Metrics show event rates, queue depths, and latencies per topic
- Can trace individual orders through the system
- Failed events are logged with context
- System continues operating if individual subscribers fail
- Publisher blocked time alerts for backpressure detection

#### Risk Manager Journey
**As a** risk manager
**I want to** receive real-time risk alerts and position updates
**So that** I can enforce limits and prevent excessive losses

**Acceptance Criteria:**
- Risk alerts published immediately when limits approached
- Position updates arrive within 1ms of execution (bus overhead only)
- Critical risk events never dropped
- Can subscribe to specific risk event types

## Requirements

### Functional Requirements

#### Core Event Bus Features
1. **Publish/Subscribe Pattern**
   - Type-safe topic routing via Topic enum
   - Single or multiple subscribers depending on topic type
   - Publisher doesn't know subscribers
   - **Subscription Filters**: Applied at fan-out layer (lossless) or subscriber-side (broadcast)

2. **Topic Configuration** (Compile-time Safety)
   ```rust
   // Critical topics that must be drained on shutdown
   const CRITICAL_TOPICS: &[Topic] = &[
       Topic::OrdersExecuted,
       Topic::PositionsUpdate,
   ];

   enum Topic {
       // Lossy Topics (Broadcast - Multiple Subscribers)
       MarketData,        // Multiple: Strategy, Risk, Logging
       Metrics,           // Multiple: Monitoring systems
       Logs,              // Multiple: Log aggregators

       // Lossless Topics (MPSC with Fan-out - Multiple Subscribers via internal routing)
       OrdersExecuted,    // Multiple: Position, Risk, UI
       PositionsUpdate,   // Multiple: Risk, UI, Logging

       // Lossless Topics (MPSC - Single Consumer)
       OrdersRequest,     // Single: Risk validator
       OrdersApproved,    // Single: Execution engine
       RiskAlert,         // Single: Risk manager
       Control,           // Single: System controller
   }
   ```

3. **Multi-Subscriber Support for Lossless Topics**
   - **Typed Fan-out Layer**: For lossless topics requiring multiple subscribers:
     - Single mpsc receiver internally fans out to per-subscriber mpsc channels
     - **Lossless Guarantee**: Uses awaited send with timeout (not try_send)
     - Maintains ordering and no message loss
     - Each subscriber gets dedicated bounded queue
     - Backpressure propagates to publisher if any subscriber is slow
     - **Slow Subscriber Policy**: After N consecutive timeouts (configurable, default 3), auto-detach subscriber with alert
   - **Max Subscribers**: When limit reached, reject new subscriptions with error (log and metric)
   - **V1 Limitation**: Topics marked as single-consumer use direct mpsc without fan-out

4. **Event Types** (Strongly Typed with Arc for Hot Paths)
   ```rust
   // Unified event enum - no separate trait needed
   #[derive(Clone, Serialize)]
   pub enum EventPayload {
       MarketData(Arc<MarketData>),
       OrderCommand(OrderCommand),
       ExecutionReport(ExecutionReport),
       PositionUpdate(PositionUpdate),
       RiskAlert(RiskAlert),
       Control(ControlCommand),
       Metrics(Arc<Metrics>),
       Log(Arc<LogEvent>),
   }

   // All event structs derive Serialize for structured logging
   #[derive(Serialize, Clone)]
   struct MarketData { /* fields */ }

   #[derive(Serialize, Clone)]
   struct OrderCommand { /* fields */ }
   // etc.
   ```

5. **Event Structure**
   - Required fields:
     - `timestamp`: DateTime<Utc> (chrono)
     - `sequence_number`: u64 (per-publisher monotonic)
     - `session_id`: Uuid
   - Optional fields:
     - `correlation_id`: Uuid
     - `strategy_id`: String
     - `order_id`: String (exchange-assigned)
     - `client_order_id`: Uuid (idempotency key, managed by Execution Engine)
     - `instrument_id`: String (routing key)
     - `schema_version`: u16
   - Financials use rust_decimal::Decimal
   - Hot path events wrapped in Arc<T>

6. **Topic-to-Event Type Mapping** (Compile-time Enforcement)
   ```rust
   impl Topic {
       fn validate_event(&self, payload: &EventPayload) -> bool {
           match (self, payload) {
               (Topic::MarketData, EventPayload::MarketData(_)) => true,
               (Topic::OrdersRequest, EventPayload::OrderCommand(_)) => true,
               (Topic::OrdersApproved, EventPayload::OrderCommand(_)) => true,
               (Topic::OrdersExecuted, EventPayload::ExecutionReport(_)) => true,
               (Topic::PositionsUpdate, EventPayload::PositionUpdate(_)) => true,
               (Topic::RiskAlert, EventPayload::RiskAlert(_)) => true,
               (Topic::Control, EventPayload::Control(_)) => true,
               (Topic::Metrics, EventPayload::Metrics(_)) => true,
               (Topic::Logs, EventPayload::Log(_)) => true,
               _ => false,
           }
       }
   }
   ```

7. **Receiver API**
   ```rust
   // Concrete wrapper for unified consumption
   pub struct BusStream<T> {
       inner: Pin<Box<dyn Stream<Item = Result<T, RecvError>> + Send>>,
   }

   impl<T> BusStream<T> {
       pub async fn recv(&mut self) -> Result<T, RecvError> { ... }
       pub fn try_recv(&mut self) -> Result<Option<T>, RecvError> { ... }
   }

   enum RecvError {
       Closed,
       Lagged(u64),  // Broadcast only - see handling playbook below
       Timeout,
   }
   ```

8. **Lagged Subscriber Handling Playbook**
   - On `RecvError::Lagged(n)`:
     1. Increment `events_lagged_total{topic}` metric
     2. Log at WARN level with rate limiting (1 per 1000)
     3. **Continue processing** (do NOT panic or restart)
     4. If stateful and n > 100, spawn task for snapshot request
     5. If n > 1000, alert operator for investigation

9. **Subscription Filtering**
   - **Broadcast Topics**: Filters applied subscriber-side after receive
   - **Fan-out Topics**: Pre-filters at fan-out layer to reduce copies
   - Filter types: instrument_id, strategy_id, or custom predicate
   - Hot instruments: Dedicated broadcast channel (LRU-100 cache)

10. **Ordering Model**
    - **Sequence Numbers**: Per-publisher monotonic counter (u64)
    - **Optional**: Per-instrument logical clock for causality
    - Deterministic ordering within single publisher
    - No ordering guarantees across publishers

11. **Channel Configuration & Backpressure**

    **Lossless Topics (MPSC)**:
    - OrdersRequest: 1024 buffer, 10ms timeout
    - OrdersApproved: 1024 buffer, 10ms timeout
    - OrdersExecuted: 1024 buffer, 10ms timeout (fan-out)
    - PositionsUpdate: 1024 buffer, 25ms timeout (fan-out)
    - RiskAlert: 512 buffer, 25ms timeout
    - Control: 256 buffer, 50ms timeout

    **Lossy Topics (Broadcast)**:
    - MarketData: 4096 ring buffer, no timeout (never blocks)
    - Metrics: 2048 ring buffer, no timeout
    - Logs: 4096 ring buffer, no timeout

    **Backpressure Behavior**:
    - Lossless: Publisher blocks with timeout, returns `PublishError::Timeout`
    - Lossy: Oldest messages dropped, lagging receivers notified
    - Metrics: `publisher_blocked_duration_ms{topic,percentile}`
    - Alert: >10 timeouts/minute triggers alert

12. **Integration Flow**
    ```
    MarketData → [Strategy, Risk, Logging]  (broadcast)
    Strategy → OrdersRequest                 (mpsc to Risk)
    Risk → OrdersApproved                   (mpsc to Execution)
    Execution → OrdersExecuted              (mpsc+fan-out to [Position, Risk, UI])
    Execution → PositionsUpdate             (mpsc+fan-out to [Risk, UI, Logging])
    Risk → RiskAlert                        (mpsc to Manager)
    Control → Control                        (mpsc to Controller)
    ```

13. **Idempotency** (Managed by Execution Engine)
    - OrderCommand includes client_order_id (Uuid)
    - Execution Engine maintains: `HashMap<Uuid, String>` (client_order_id → exchange_order_id)
    - Duplicate requests return cached exchange_order_id
    - ExecutionReport includes both IDs for reconciliation
    - **Note**: Idempotency map is NOT part of bus, it's Execution's responsibility

14. **Snapshot Support** (For Stateful Recovery)
    - Optional callback registration: `on_high_lag(topic, lag_count)`
    - Triggered when lag exceeds threshold (configurable, default 100)
    - Runs in spawned task to avoid blocking
    - Subscriber can request full state snapshot from source
    - Used for Position/Risk state recovery after lag

### Non-Functional Requirements

#### Performance
- **Throughput**: Sustained 5k events/sec, target 10k on 8-core
- **Latency**:
  - Event bus operations: <1ms (sub-millisecond)
  - Fan-out overhead: <100μs per subscriber
  - End-to-end simple flow: <10ms total
  - Position update bus overhead: <1ms
- **Memory**:
  - Bounded memory via channel limits
  - Arc<T> for market data (no cloning)
  - Small events for critical paths
  - Consider `tokio::sync::mpsc::Sender::reserve()` for critical paths

#### Reliability
- **Subscriber Isolation**:
  - Each handler in separate tokio task
  - Task panics don't crash runtime
  - Supervised restart: exponential backoff (1ms, 10ms, 100ms, 1s, max 5 attempts)
  - No catch_unwind in async paths (let task fail cleanly)
- **Head-of-Line Blocking Prevention**:
  - Slow subscriber auto-detach after 3 consecutive timeouts
  - Alert on detach with subscriber type (not raw ID)
  - Optional re-subscribe with backoff
- **Error Handling**:
  - Count all errors via metrics
  - Rate-limited logs (1 per 1000 for high-frequency)
  - Lagged subscribers continue (don't restart)
- **Backpressure**:
  - Bounded channels prevent exhaustion
  - Per-topic configurable timeouts
  - Circuit breaker on sustained pressure
- **Graceful Shutdown**:
  - `accepting_publishes` atomic flag
  - publish() returns `PublishError::ShuttingDown` immediately when set
  - CancellationToken signals all tasks
  - Drain CRITICAL_TOPICS (5s timeout)
  - Metrics: `events_drained_total{topic}` vs `events_dropped_on_shutdown_total{topic}`

#### Observability
- **Metrics Stack**:
  - `metrics` crate for collection
  - `metrics-exporter-prometheus` for export
  - Bounded cardinality: Use role/type instead of unique subscriber IDs
- **Core Metrics** (Bounded Labels):
  ```
  events_published_total{topic}
  events_consumed_total{topic,subscriber_type}
  events_dropped_total{topic}
  events_lagged_total{topic}
  events_drained_total{topic}
  events_dropped_on_shutdown_total{topic}
  queue_depth{topic}
  queue_max_depth{topic}
  fanout_subscribers{topic}
  fanout_slow_detached_total{topic}
  fanout_max_subscribers_rejected_total{topic}
  event_latency_ms{topic,percentile}
  publisher_blocked_ms{topic,percentile}
  subscriber_restarts_total{topic,subscriber_type}
  ```
- **Tracing**:
  - Spans: `bus.publish`, `bus.handle`
  - Fields: session_id, correlation_id, topic
  - Event flow DAG visualization

#### Scalability
- In-memory for v1 (single process)
- MessageBus trait for future distributed
- Hot instrument optimization:
  - LRU-100 cache of per-instrument channels
  - Automatic eviction on capacity
  - Metrics for cache hit rate
- Low-contention fan-out via DashMap or arc-swap
- Subscriber-side filtering for broadcast

### Testing Requirements
1. **Unit Tests**
   - Per-publisher ordering
   - Fan-out delivery to all subscribers
   - **Lossless fan-out under pressure** (no drops)
   - Slow subscriber auto-detach
   - Max subscribers rejection
   - Backpressure timeout behavior
   - Broadcast lagging (verify Lagged error)
   - Topic-to-event validation
   - Reserve() for critical paths

2. **Integration Tests**
   - End-to-end all topic flows
   - Multi-subscriber on fan-out topics
   - Subscriber churn (subscribe/unsubscribe during publish)
   - Graceful shutdown sequence
   - Performance smoke test (release)
   - Instrument LRU eviction

3. **Property Tests**
   - Ordering invariants
   - No loss on lossless topics under sustained load
   - Fan-out delivers to all active subscribers

4. **Fuzz Tests** (Optional/Nightly)
   - Random pub/sub patterns
   - Concurrent subscribe/unsubscribe
   - Panicking handlers

5. **Determinism Tests**
   - Identical backtest results
   - Fixed seeds and timing

6. **Acceptance Tests**
   - **Lossless topic with 2+ subscribers via fan-out**: Verify no drops under load
   - **Broadcast Lagged recovery**: Verify metrics increment, processing continues
   - **Slow subscriber detach**: Verify auto-detach after timeouts
   - **Max subscribers**: Verify rejection with error
   - Shutdown drains critical events

## Success Criteria

### Measurable Outcomes
- Bus latency p99 < 1ms at 5k events/sec
- Fan-out overhead < 100μs per subscriber
- Zero critical event loss (orders, executions)
- Backtest 100% deterministic
- <10ms end-to-end market → order
- 10k events/sec on 8-core
- Publisher blocked p99 < 5ms
- Subscriber restart rate < 0.1/hour
- Graceful shutdown success > 99%
- Slow subscriber detach rate < 1/day

### Key Metrics and KPIs
- Throughput (events/second by topic)
- Latency percentiles (p50, p95, p99)
- Drop rate < 0.01% for non-critical
- Lagged rate < 1% for broadcast
- Memory stable over 24h
- Zero critical events lost on shutdown
- Fan-out router lock contention < 1%

## Constraints & Assumptions

### Technical Limitations
- Single process (no distributed in v1)
- Rust-only (no FFI)
- No persistence (restart = loss)
- Pub/sub only (no req/reply)
- mpsc requires fan-out for multi-subscriber
- Broadcast can lag under load

### Assumptions
- Tokio runtime
- Async handlers (or spawn_blocking)
- NTP synchronized clocks
- Sufficient RAM for buffers
- Subscribers handle Lagged gracefully
- Execution Engine manages idempotency

## Out of Scope

- Distributed bus (Kafka, NATS)
- Event sourcing/persistence
- Request/reply patterns
- Dead letter queues
- Content-based routing
- Cross-process IPC
- Network transport
- Schema registry
- Exactly-once semantics
- Transactional publishing
- Priority within topics
- Dynamic topic creation
- Client order ID mapping (Execution's domain)

## Dependencies

### External Dependencies
```toml
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"
rust_decimal = "1.35"
chrono = "0.4"
uuid = { version = "1.10", features = ["v4"] }
serde = { version = "1", features = ["derive"] }
tracing = "0.1"
metrics = "0.23"
metrics-exporter-prometheus = "0.15"
arc-swap = "1.7"        # For low-contention fan-out
dashmap = "6.1"         # Alternative to RwLock<Vec>
```

### Internal Dependencies
- Domain models
- Config system
- Logger setup
- Execution Engine (for idempotency)

## Implementation Notes

### Core API
```rust
// Critical topics for shutdown
const CRITICAL_TOPICS: &[Topic] = &[
    Topic::OrdersExecuted,
    Topic::PositionsUpdate,
];

// Unified event enum - no separate Event trait
#[derive(Clone, Serialize)]
pub enum EventPayload {
    MarketData(Arc<MarketData>),
    OrderCommand(OrderCommand),
    ExecutionReport(ExecutionReport),
    PositionUpdate(PositionUpdate),
    RiskAlert(RiskAlert),
    Control(ControlCommand),
    Metrics(Arc<Metrics>),
    Log(Arc<LogEvent>),
}

pub trait MessageBus: Send + Sync {
    async fn publish(
        &self,
        topic: Topic,
        event: EventPayload,
    ) -> Result<(), PublishError>;

    async fn subscribe(
        &self,
        topic: Topic,
    ) -> Result<BusStream<EventPayload>, SubscribeError>;

    // Optional: typed helpers for better ergonomics
    async fn subscribe_market_data(&self) -> Result<BusStream<Arc<MarketData>>, SubscribeError>;
    async fn subscribe_orders_executed(&self) -> Result<BusStream<ExecutionReport>, SubscribeError>;
}

pub struct InProcessBus {
    mpsc_senders: HashMap<Topic, mpsc::Sender<EventPayload>>,
    broadcast_senders: HashMap<Topic, broadcast::Sender<Arc<EventPayload>>>,
    fanout_routers: HashMap<Topic, Arc<TypedFanoutRouter>>,
    accepting: AtomicBool,
    cancel_token: CancellationToken,
}
```

### Fan-out Router with Fixed Timeout Flow
```rust
use dashmap::DashMap;
use uuid::Uuid;

struct TypedFanoutRouter {
    // DashMap reduces contention vs RwLock<Vec>
    subscribers: DashMap<Uuid, mpsc::Sender<EventPayload>>,
    slow_count: DashMap<Uuid, u8>,
    max_subscribers: usize,
}

impl TypedFanoutRouter {
    async fn route(&self, event: EventPayload, timeout: Duration) -> Result<(), PublishError> {
        let mut results = Vec::new();
        let mut slow_subscribers = Vec::new();
        let mut closed_subscribers = Vec::new();

        // Attempt all sends, collecting results
        for entry in self.subscribers.iter() {
            let (id, tx) = entry.pair();
            let event_clone = event.clone();

            let result = tokio::time::timeout(timeout, tx.send(event_clone)).await;
            results.push((*id, result));
        }

        // Process results and update slow counts
        let mut any_timeout = false;
        for (id, result) in results {
            match result {
                Ok(Ok(_)) => {
                    // Success - reset slow count
                    self.slow_count.remove(&id);
                }
                Ok(Err(_)) => {
                    // Channel closed
                    closed_subscribers.push(id);
                }
                Err(_) => {
                    // Timeout
                    any_timeout = true;
                    let count = self.slow_count.entry(id)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);

                    if *count >= 3 {
                        slow_subscribers.push(id);
                    }
                }
            }
        }

        // Clean up closed subscribers
        for id in closed_subscribers {
            self.subscribers.remove(&id);
            self.slow_count.remove(&id);
        }

        // Auto-detach slow subscribers
        for id in slow_subscribers {
            self.subscribers.remove(&id);
            self.slow_count.remove(&id);
            warn!("Detached slow subscriber: {}", id);
            metrics::counter!("fanout_slow_detached_total")
                .with_label("topic", "...") // Add topic name
                .increment(1);
        }

        // Return timeout error if any subscriber timed out
        if any_timeout {
            Err(PublishError::Timeout)
        } else {
            Ok(())
        }
    }

    fn add_subscriber(&self, tx: mpsc::Sender<EventPayload>) -> Result<Uuid, SubscribeError> {
        if self.subscribers.len() >= self.max_subscribers {
            metrics::counter!("fanout_max_subscribers_rejected_total").increment(1);
            return Err(SubscribeError::MaxSubscribersReached);
        }

        let id = Uuid::new_v4();
        self.subscribers.insert(id, tx);
        Ok(id)
    }
}
```

### Per-Topic Configuration
```yaml
topics:
  market_data:
    type: broadcast
    buffer: 4096
    # No timeout for broadcast

  orders_request:
    type: mpsc
    buffer: 1024
    timeout_ms: 10
    max_subscribers: 1

  orders_executed:
    type: mpsc_fanout
    buffer: 1024
    timeout_ms: 10
    max_subscribers: 10
    slow_threshold: 3  # Detach after 3 timeouts

  positions_update:
    type: mpsc_fanout
    buffer: 1024
    timeout_ms: 25
    max_subscribers: 10
    slow_threshold: 3
```

### Shutdown Implementation
```rust
impl InProcessBus {
    pub async fn shutdown(&self, timeout: Duration) {
        // 1. Stop accepting immediately
        self.accepting.store(false, Ordering::SeqCst);

        // 2. Cancel all tasks
        self.cancel_token.cancel();

        // 3. Drain critical topics
        let drain = async {
            for topic in CRITICAL_TOPICS {
                if let Err(e) = self.drain_topic(*topic).await {
                    warn!("Failed to drain {:?}: {}", topic, e);
                }
                metrics::counter!("events_drained_total")
                    .with_label("topic", topic.as_str())
                    .increment(1);
            }
        };

        // 4. Record outcome
        match tokio::time::timeout(timeout, drain).await {
            Ok(_) => {
                info!("Graceful shutdown completed");
            }
            Err(_) => {
                for topic in CRITICAL_TOPICS {
                    metrics::counter!("events_dropped_on_shutdown_total")
                        .with_label("topic", topic.as_str())
                        .increment(1);
                }
                warn!("Shutdown timeout, some events may be lost");
            }
        }
    }
}
```

### Deterministic Bus (For Backtesting)
```rust
#[cfg(feature = "deterministic")]
pub struct DeterministicBus {
    events: VecDeque<(Topic, EventPayload)>,
    // No async, no timeouts
    // Single-threaded processing
    // Controlled time advancement
}
```

### Reserve for Critical Paths
```rust
// For critical paths, reserve capacity upfront
let permit = tx.reserve().await?;
permit.send(event);  // Cannot fail
```

### Snapshot Callback (Async)
```rust
pub trait SnapshotProvider: Send + Sync {
    async fn request_snapshot(&self, topic: Topic, lag: u64);
}

impl Subscriber {
    fn on_lagged(&mut self, lag: u64) {
        if lag > 100 {
            if let Some(provider) = self.snapshot_provider.clone() {
                // Spawn task to avoid blocking
                let topic = self.topic;
                tokio::spawn(async move {
                    provider.request_snapshot(topic, lag).await;
                });
            }
        }
    }
}
```

### Typed Subscribe Helpers
```rust
impl InProcessBus {
    pub async fn subscribe_market_data(&self)
        -> Result<BusStream<Arc<MarketData>>, SubscribeError>
    {
        let stream = self.subscribe(Topic::MarketData).await?;
        // Filter and cast EventPayload::MarketData
        Ok(stream.filter_map(|result| {
            match result {
                Ok(EventPayload::MarketData(data)) => Some(Ok(data)),
                Err(e) => Some(Err(e)),
                _ => None,
            }
        }))
    }

    pub async fn subscribe_orders_executed(&self)
        -> Result<BusStream<ExecutionReport>, SubscribeError>
    {
        let stream = self.subscribe(Topic::OrdersExecuted).await?;
        // Filter and cast EventPayload::ExecutionReport
        Ok(stream.filter_map(|result| {
            match result {
                Ok(EventPayload::ExecutionReport(report)) => Some(Ok(report)),
                Err(e) => Some(Err(e)),
                _ => None,
            }
        }))
    }
}
```

### CI/CD Configuration
```yaml
# Regular CI (every commit)
test:
  - cargo test --lib --bins
  - cargo test --doc
  - cargo clippy -- -D warnings

# Nightly CI (optional)
nightly:
  - cargo test --features fuzz -- --ignored  # 1hr fuzz tests
  - cargo bench -- --save-baseline            # Performance regression

# Release gate
release:
  - cargo test --release
  - cargo bench -- --baseline=main            # ±10% threshold
```

## Risk Mitigations

### Head-of-Line Blocking
- Auto-detach after 3 timeouts (configurable)
- Alert on detach with subscriber type
- Optional re-subscribe with backoff
- Monitor `fanout_slow_detached_total`

### Fan-out Contention
- Use DashMap vs RwLock<Vec>
- Consider arc-swap for immutable snapshots if needed
- Monitor lock contention metrics

### Metrics Cardinality
- Use bounded identifiers (subscriber_type, not UUID)
- Never export raw UUIDs to Prometheus
- Monitor cardinality growth

### Broadcast Backlogs
- Spawn snapshot callbacks (async)
- Alert on sustained lag > 1000
- Consider increasing ring buffer size

### Memory Protection
- Hard channel bounds
- Monitor RSS/heap
- Alert on growth >10%/hour

### Testing Coverage
- Unit: 90% line coverage
- Integration: All paths including churn
- Bench regression: Optional nightly
- Fuzz: Optional nightly (1hr)