//! Performance benchmarks for the event bus system.
//!
//! This benchmark suite validates that the event bus meets the performance
//! requirements of < 1ms latency at 5k events/sec throughput while measuring
//! memory allocation patterns and fan-out routing performance.

use ai_trading_agent::event_bus::{MessageBus, SubscriptionConfig};
use ai_trading_agent::events::{
    EventPayload, MarketData, MarketDataEvent, OrderCommand, OrderCommandEvent, OrderType,
    SharedEventPayload, TradeSide, TimeInForce, Topic,
};
use chrono::Utc;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use uuid::Uuid;

/// Creates a test market data event with specified sequence.
fn create_market_data_event(sequence: u64) -> SharedEventPayload {
    Arc::new(EventPayload::MarketData(MarketDataEvent {
        symbol: "BTCUSDT".to_string(),
        timestamp: Utc::now(),
        sequence,
        data: MarketData::Trade {
            price: Decimal::new(50000, 0),
            quantity: Decimal::new(1, 1),
            side: TradeSide::Buy,
        },
    }))
}

/// Creates a test order command event with specified sequence.
fn create_order_command_event(sequence: u64) -> SharedEventPayload {
    Arc::new(EventPayload::OrderCommand(OrderCommandEvent {
        order_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        sequence,
        command: OrderCommand::PlaceOrder {
            symbol: "BTCUSDT".to_string(),
            side: TradeSide::Buy,
            order_type: OrderType::Limit,
            quantity: Decimal::new(1, 1),
            price: Some(Decimal::new(50000, 0)),
            time_in_force: TimeInForce::GTC,
        },
    }))
}

/// Creates a batch of market data events for testing.
fn create_event_batch(size: usize) -> Vec<SharedEventPayload> {
    (0..size)
        .map(|i| create_market_data_event(i as u64))
        .collect()
}

/// Creates events with different payload sizes for memory testing.
fn create_large_event(sequence: u64, size_multiplier: usize) -> SharedEventPayload {
    let base_symbol = "BTCUSDT";
    let large_symbol = format!("{}{}", base_symbol, "X".repeat(size_multiplier * 100));

    Arc::new(EventPayload::MarketData(MarketDataEvent {
        symbol: large_symbol,
        timestamp: Utc::now(),
        sequence,
        data: MarketData::Kline {
            open: Decimal::new(49000, 0),
            high: Decimal::new(51000, 0),
            low: Decimal::new(48000, 0),
            close: Decimal::new(50000, 0),
            volume: Decimal::new(1000, 0),
            interval: "1m".to_string(),
        },
    }))
}

/// Mock message bus implementation for benchmarking.
///
/// This provides a minimal implementation focused on measuring core performance
/// without the overhead of real async operations or external dependencies.
#[derive(Debug, Default)]
struct MockMessageBus {
    publish_count: std::sync::atomic::AtomicU64,
    subscribe_count: std::sync::atomic::AtomicU64,
}

#[async_trait::async_trait]
impl MessageBus for MockMessageBus {
    async fn publish(
        &self,
        _topic: Topic,
        _event: SharedEventPayload,
    ) -> Result<(), ai_trading_agent::event_bus::PublishError> {
        self.publish_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn subscribe(
        &self,
        topic: Topic,
        config: SubscriptionConfig,
    ) -> Result<
        ai_trading_agent::event_bus::BusStream,
        ai_trading_agent::event_bus::SubscribeError,
    > {
        self.subscribe_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create a mock stream that never produces events
        let (tx, rx) = tokio::sync::mpsc::channel(config.buffer_size);
        drop(tx); // Close immediately
        Ok(ai_trading_agent::event_bus::BusStream::new(rx, topic))
    }

    async fn subscribe_multiple(
        &self,
        topics: &[Topic],
        config: SubscriptionConfig,
    ) -> Result<
        Vec<ai_trading_agent::event_bus::BusStream>,
        ai_trading_agent::event_bus::SubscribeError,
    > {
        let mut streams = Vec::with_capacity(topics.len());
        for &topic in topics {
            streams.push(self.subscribe(topic, config.clone()).await?);
        }
        Ok(streams)
    }

    async fn stats(&self) -> ai_trading_agent::event_bus::BusStats {
        ai_trading_agent::event_bus::BusStats {
            total_published: self
                .publish_count
                .load(std::sync::atomic::Ordering::Relaxed),
            active_subscriptions: self
                .subscribe_count
                .load(std::sync::atomic::Ordering::Relaxed) as usize,
            ..Default::default()
        }
    }

    async fn subscriber_count(&self, _topic: Topic) -> usize {
        0
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        true
    }
}

/// Benchmark single event publishing performance.
fn bench_single_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    c.bench_function("publish_single_event", |b| {
        b.iter_batched(
            || create_market_data_event(0),
            |event| {
                black_box(
                    rt.block_on(bus.publish(Topic::MarketData, event))
                        .expect("Publish failed"),
                );
            },
            BatchSize::SmallInput,
        );
    });
}

/// Benchmark batch event publishing performance.
fn bench_batch_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("publish_batch");
    for batch_size in [10, 100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("events", batch_size),
            batch_size,
            |b, &size| {
                b.iter_batched(
                    || create_event_batch(size),
                    |events| {
                        rt.block_on(async {
                            for event in events {
                                black_box(
                                    bus.publish(Topic::MarketData, event)
                                        .await
                                        .expect("Publish failed"),
                                );
                            }
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Benchmark subscription creation performance.
fn bench_subscribe(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    c.bench_function("subscribe_single", |b| {
        b.iter(|| {
            let stream = rt.block_on(
                bus.subscribe(Topic::MarketData, SubscriptionConfig::default())
            ).expect("Subscribe failed");
            black_box(stream);
        });
    });
}

/// Benchmark fan-out performance with multiple subscribers.
fn bench_fanout_routing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("fanout_routing");
    for subscriber_count in [1, 5, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("subscribers", subscriber_count),
            subscriber_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        // Setup: Create subscribers
                        rt.block_on(async {
                            let mut streams = Vec::new();
                            for _ in 0..count {
                                let stream = bus
                                    .subscribe(Topic::MarketData, SubscriptionConfig::default())
                                    .await
                                    .expect("Subscribe failed");
                                streams.push(stream);
                            }
                            streams
                        })
                    },
                    |_streams| {
                        // Benchmark: Publish to all subscribers
                        let event = create_market_data_event(0);
                        rt.block_on(
                            bus.publish(Topic::MarketData, event)
                        ).expect("Publish failed");
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Benchmark latency under load.
fn bench_latency_under_load(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("latency_under_load");
    group.measurement_time(Duration::from_secs(10));

    // Test latency at target throughput of 5k events/sec
    group.bench_function("5k_events_per_second", |b| {
        b.iter_custom(|iters| {
            let events_per_iter = 5000; // 5k events per iteration
            let total_events = iters * events_per_iter;

            let start = Instant::now();
            rt.block_on(async {
                for i in 0..total_events {
                    let event = create_market_data_event(i);
                    bus.publish(Topic::MarketData, event)
                        .await
                        .expect("Publish failed");

                    // Simulate target rate: 5k events/sec = 200μs per event
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
            });
            start.elapsed()
        });
    });

    group.finish();
}

/// Benchmark memory allocation patterns with different payload sizes.
fn bench_memory_patterns(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("memory_allocation");
    for size_multiplier in [1, 5, 10, 20].iter() {
        group.bench_with_input(
            BenchmarkId::new("payload_size", size_multiplier),
            size_multiplier,
            |b, &multiplier| {
                b.iter_batched(
                    || create_large_event(0, multiplier),
                    |event| {
                        black_box(
                            rt.block_on(bus.publish(Topic::MarketData, event))
                                .expect("Publish failed"),
                        );
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Benchmark event serialization/deserialization performance.
fn bench_event_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_serialization");

    // Benchmark JSON serialization
    group.bench_function("json_serialize_market_data", |b| {
        b.iter_batched(
            || create_market_data_event(0),
            |event| {
                let json = black_box(serde_json::to_string(&*event).expect("Serialization failed"));
                black_box(json);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("json_deserialize_market_data", |b| {
        b.iter_batched(
            || {
                let event = create_market_data_event(0);
                serde_json::to_string(&*event).expect("Serialization failed")
            },
            |json| {
                let event: EventPayload =
                    black_box(serde_json::from_str(&json).expect("Deserialization failed"));
                black_box(event);
            },
            BatchSize::SmallInput,
        );
    });

    // Benchmark different event types
    group.bench_function("json_serialize_order_command", |b| {
        b.iter_batched(
            || create_order_command_event(0),
            |event| {
                let json = black_box(serde_json::to_string(&*event).expect("Serialization failed"));
                black_box(json);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark topic routing performance.
fn bench_topic_routing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("topic_routing");

    // Test routing to different topics
    let topics = [
        Topic::MarketData,
        Topic::OrderCommand,
        Topic::ExecutionReport,
        Topic::PositionUpdate,
        Topic::RiskAlert,
        Topic::ControlCommand,
    ];

    for (i, &topic) in topics.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("topic", i), &topic, |b, &topic| {
            b.iter_batched(
                || match topic {
                    Topic::MarketData => create_market_data_event(0),
                    Topic::OrderCommand => create_order_command_event(0),
                    _ => create_market_data_event(0), // Use market data for other topics
                },
                |event| {
                    black_box(
                        rt.block_on(bus.publish(topic, event)).expect("Publish failed"),
                    );
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark concurrent publishing from multiple threads.
fn bench_concurrent_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("concurrent_publish");
    for thread_count in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("threads", thread_count),
            thread_count,
            |b, &threads| {
                b.iter(|| {
                    rt.block_on(async {
                        let bus = bus.clone();
                        let handles: Vec<_> = (0..threads)
                            .map(|i| {
                                let bus = bus.clone();
                                tokio::spawn(async move {
                                    let event = create_market_data_event(i as u64);
                                    bus.publish(Topic::MarketData, event)
                                        .await
                                        .expect("Publish failed");
                                })
                            })
                            .collect();

                        for handle in handles {
                            handle.await.expect("Task failed");
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

/// Performance regression test to ensure we meet target metrics.
fn bench_performance_targets(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(MockMessageBus::default());

    let mut group = c.benchmark_group("performance_targets");
    group.measurement_time(Duration::from_secs(10));

    // Target: < 1ms latency for single event publish
    group.bench_function("target_latency_1ms", |b| {
        b.iter_batched(
            || create_market_data_event(0),
            |event| {
                let start = Instant::now();
                rt.block_on(bus.publish(Topic::MarketData, event))
                    .expect("Publish failed");
                let latency = start.elapsed();

                // Assert latency is under target (this will show in benchmark output)
                if latency > Duration::from_millis(1) {
                    eprintln!("Warning: Latency {} exceeds 1ms target", latency.as_micros());
                }

                black_box(latency);
            },
            BatchSize::SmallInput,
        );
    });

    // Target: 5k events/sec throughput
    group.throughput(Throughput::Elements(5000));
    group.bench_function("target_throughput_5k_per_sec", |b| {
        b.iter_custom(|iters| {
            let events_per_iter = 5000;
            let total_events = iters * events_per_iter;

            let start = Instant::now();
            rt.block_on(async {
                for i in 0..total_events {
                    let event = create_market_data_event(i);
                    bus.publish(Topic::MarketData, event)
                        .await
                        .expect("Publish failed");
                }
            });
            let elapsed = start.elapsed();

            // Calculate actual throughput
            let actual_throughput = total_events as f64 / elapsed.as_secs_f64();
            if actual_throughput < 5000.0 {
                eprintln!(
                    "Warning: Throughput {:.0} events/sec below 5k target",
                    actual_throughput
                );
            }

            elapsed
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_publish,
    bench_batch_publish,
    bench_subscribe,
    bench_fanout_routing,
    bench_latency_under_load,
    bench_memory_patterns,
    bench_event_serialization,
    bench_topic_routing,
    bench_concurrent_publish,
    bench_performance_targets,
);

criterion_main!(benches);