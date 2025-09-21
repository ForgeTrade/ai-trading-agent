---
created: 2025-09-21T09:04:16Z
last_updated: 2025-09-21T09:04:16Z
version: 1.0
author: Claude Code PM System
---

# Technology Context

## Language and Runtime
- **Primary Language**: Rust (edition 2021)
- **Runtime**: Tokio 1.x async runtime
- **Minimum Rust Version**: 1.70+ (for stable async traits)

## Core Dependencies (Planned)

### Async Runtime
- `tokio = { version = "1", features = ["full"] }` - Async runtime with channels
- `futures = "0.3"` - Future combinators and streams

### Serialization and Data
- `serde = { version = "1", features = ["derive"] }` - Serialization framework
- `serde_json = "1"` - JSON support
- `toml = "0.8"` - Configuration file parsing
- `csv = "1"` - CSV file reading for backtests
- `rust_decimal = "1.35"` - Precise decimal calculations
- `chrono = { version = "0.4", features = ["serde"] }` - DateTime handling
- `uuid = { version = "1.10", features = ["v4", "serde"] }` - Correlation IDs

### Observability
- `tracing = "0.1"` - Structured logging and spans
- `tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }`
- `metrics = "0.23"` - Metrics collection
- `metrics-exporter-prometheus = "0.15"` - Prometheus exporter

### Concurrency
- `dashmap = "6.1"` - Concurrent HashMap for fan-out routing
- `arc-swap = "1"` - Atomic Arc swapping for hot reloads
- `parking_lot = "0.12"` - Faster synchronization primitives

### Networking (Future)
- `reqwest = { version = "0.11", features = ["json", "stream"] }` - HTTP client
- `tokio-tungstenite = "0.21"` - WebSocket support
- `tonic = "0.11"` - gRPC (future microservices)

### CLI and Configuration
- `clap = { version = "4", features = ["derive"] }` - CLI argument parsing
- `config = "0.14"` - Hierarchical configuration
- `dotenv = "0.15"` - Environment variable loading

### Testing
- `criterion = "0.5"` - Benchmarking framework
- `proptest = "1"` - Property-based testing
- `wiremock = "0.6"` - HTTP mocking for tests
- `tempfile = "3"` - Temporary files for testing

## Development Tools

### Build and Package Management
- **Cargo**: Rust package manager
- **cargo-watch**: Auto-rebuild on file changes
- **cargo-expand**: Macro expansion viewer
- **cargo-audit**: Security vulnerability scanner

### Code Quality
- **rustfmt**: Code formatter (configured via rustfmt.toml)
- **clippy**: Linter with strict settings
- **cargo-tarpaulin**: Code coverage tool
- **cargo-deny**: Dependency license/security checker

### Performance
- **cargo-flamegraph**: CPU profiling
- **valgrind/massif**: Memory profiling
- **criterion**: Micro-benchmarking

## Architectural Technologies

### Event-Driven Patterns
- **Message Bus**: In-process pub/sub using Tokio channels
- **MPSC Channels**: Lossless message passing for critical events
- **Broadcast Channels**: Lossy high-throughput for market data
- **DashMap Router**: Lock-free concurrent routing

### Data Management
- **Arc<T>**: Reference-counted smart pointers for zero-copy
- **Copy-on-Write**: For configuration hot-reloading
- **Ring Buffers**: For market data windowing
- **VecDeque**: For deterministic event queues

### Error Handling
- **thiserror**: Ergonomic error definitions
- **anyhow**: Error context propagation
- **Result<T, E>**: Explicit error handling everywhere

## External Integrations (Planned)

### Exchanges
- **Binance**: Primary exchange (REST + WebSocket)
- **Exchange Abstraction Layer**: Unified interface for multiple exchanges

### Data Sources
- **CSV Files**: Historical data for backtesting
- **PostgreSQL**: Time-series data storage (future)
- **Redis**: Session state and caching (future)

### Monitoring
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization
- **Jaeger**: Distributed tracing (future)
- **Sentry**: Error tracking (future)

## Performance Targets

### Latency
- Event publishing: < 100μs
- Event routing: < 500μs
- End-to-end (market to order): < 10ms

### Throughput
- Backtest mode: 10k events/sec
- Live mode: 5k events/sec sustained
- Burst capacity: 20k events/sec for 10 seconds

### Resource Usage
- Memory: < 500MB baseline
- CPU: < 50% on 4-core during normal operation
- Network: < 10Mbps for market data

## Security Considerations

### API Keys
- Never hardcoded, always from environment
- Encrypted at rest using OS keyring
- Separate keys for test/live environments

### Network
- TLS 1.3 for all external connections
- Certificate pinning for exchange APIs
- Rate limiting on all endpoints

### Code
- No unsafe blocks without justification
- All inputs validated and sanitized
- Constant-time comparisons for secrets

## CI/CD Pipeline (Planned)

### Build Pipeline
1. Lint (clippy)
2. Format check (rustfmt)
3. Security audit (cargo-audit)
4. Unit tests
5. Integration tests
6. Benchmarks (regression check)

### Release Pipeline
1. Version tagging
2. Build release binaries
3. Generate SBOM
4. Docker image creation
5. Deployment to staging
6. Smoke tests
7. Production deployment

## Development Environment

### Recommended Setup
- **OS**: Linux/macOS (Windows via WSL2)
- **IDE**: VS Code with rust-analyzer
- **RAM**: 16GB minimum
- **CPU**: 8+ cores for parallel compilation
- **Disk**: SSD with 50GB free space