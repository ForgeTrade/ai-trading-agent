---
created: 2025-09-21T09:04:16Z
last_updated: 2025-09-21T09:04:16Z
version: 1.0
author: Claude Code PM System
---

# Project Structure

## Current Directory Layout

```
ai-trading-agent/
├── .claude/                     # Claude Code PM system files
│   ├── context/                 # Project context documentation
│   │   └── README.md           # Context usage guide
│   ├── epics/                  # Epic definitions and tasks
│   │   └── event-bus-and-domain-events-v1/
│   │       ├── epic.md         # Epic overview and metadata
│   │       ├── github-mapping.md # GitHub issue mapping
│   │       └── [2-11].md       # Individual task files (by issue number)
│   ├── prds/                   # Product Requirements Documents
│   │   └── event-bus-and-domain-events-v1.md
│   └── rules/                  # PM system rules and templates
│       └── datetime.md         # DateTime handling rules
├── .git/                       # Git repository
├── AGENTS.md                   # Agent command documentation
├── CLAUDE.md                   # Claude Code configuration
├── COMMANDS.md                 # PM command reference
└── prd.md                      # Main product requirements document
```

## Planned Structure (After Implementation)

```
ai-trading-agent/
├── src/
│   ├── main.rs                # Application entry point
│   ├── lib.rs                 # Library root
│   ├── event_bus/             # Event bus implementation
│   │   ├── mod.rs             # MessageBus trait and InProcessBus
│   │   ├── channels.rs        # Channel management
│   │   ├── router.rs          # TypedFanoutRouter
│   │   ├── stream.rs          # BusStream wrapper
│   │   ├── metrics.rs         # Metrics collection
│   │   └── deterministic.rs   # Deterministic mode for backtesting
│   ├── events/                # Domain events
│   │   ├── mod.rs             # EventPayload enum and Topic enum
│   │   ├── market_data.rs     # Market data events
│   │   ├── order.rs           # Order command events
│   │   ├── execution.rs       # Execution report events
│   │   └── risk.rs            # Risk management events
│   ├── strategies/            # Trading strategies
│   │   ├── mod.rs             # Strategy trait
│   │   └── sample/            # Sample strategies
│   ├── execution/             # Execution engine
│   │   ├── mod.rs             # Execution trait
│   │   ├── simulated.rs       # Backtest/paper execution
│   │   └── live.rs            # Live exchange execution
│   ├── market_data/           # Market data handling
│   │   ├── mod.rs             # Market data traits
│   │   ├── csv.rs             # CSV file reader
│   │   └── exchange.rs        # Live exchange feeds
│   ├── risk/                  # Risk management
│   │   ├── mod.rs             # Risk manager trait
│   │   ├── limits.rs          # Position/exposure limits
│   │   └── portfolio.rs       # Portfolio-level controls
│   └── cli/                   # CLI interface
│       ├── mod.rs             # CLI commands
│       └── config.rs          # Configuration handling
├── tests/                      # Integration tests
│   ├── integration/
│   └── benchmarks/
├── config/                     # Configuration files
│   └── default.toml
├── data/                       # Sample data for testing
├── Cargo.toml                  # Rust dependencies
├── Cargo.lock                  # Dependency lock file
├── README.md                   # Project documentation
└── .github/                    # GitHub configuration
    └── workflows/              # CI/CD pipelines
```

## File Naming Conventions

- **Rust modules**: snake_case (e.g., `event_bus.rs`, `market_data.rs`)
- **Documentation**: kebab-case for compound names (e.g., `event-bus-design.md`)
- **Configuration**: lowercase with extensions (e.g., `config.toml`)
- **Task files**: Issue number as filename (e.g., `2.md`, `3.md`)

## Module Organization

### Core Modules
- `event_bus`: Central messaging infrastructure
- `events`: Domain event definitions
- `strategies`: Trading strategy implementations
- `execution`: Order execution logic
- `market_data`: Data ingestion and normalization
- `risk`: Risk management and controls

### Support Modules
- `cli`: Command-line interface
- `config`: Configuration management
- `metrics`: Performance monitoring
- `logging`: Structured logging

## Key Design Patterns

1. **Trait-based abstraction**: All major components defined as traits
2. **Event-driven communication**: Components communicate via events only
3. **Mode abstraction**: Same code runs in backtest/paper/live modes
4. **Dependency injection**: Components receive dependencies via constructor
5. **Builder pattern**: Complex objects use builders for construction

## Data Flow Architecture

```
Market Data → Event Bus → Strategies → Order Commands → Risk Check → Execution
     ↓            ↓           ↓              ↓              ↓           ↓
  [Events]    [Topics]   [Subscribers]  [Publishers]   [Validators]  [Reports]
```

## Configuration Structure

- Runtime configuration via TOML files
- Environment variable interpolation supported
- Hierarchical configuration with defaults
- Per-environment overrides (backtest/paper/live)