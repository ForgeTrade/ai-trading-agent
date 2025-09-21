# AGENTS – Reliable Coding Practices for Our Stack (Rust)

## Project Overview & Stack
This repository is a high‑performance Rust project (Rust 2021) for an event‑driven AI trading agent. Core technologies: Tokio (async runtime), Serde (serialization), TOML/JSON config, Clap (CLI). Optional: Actix‑web/Tonic for APIs, ONNX/PyO3 for ML integration. We use AI coding assistants (OpenAI Codex CLI, Anthropic Claude Code) to accelerate development under strict quality gates.

## Environment Setup & Build
- Toolchain: install via `rustup`; use the pinned toolchain if `rust-toolchain.toml` exists (`rustup show active-toolchain`).
- Build: `cargo build` (dev) or `cargo build --release` (optimized).
- Lint/Format: `cargo clippy -- -D warnings` and `cargo fmt --all -- --check` (use `cargo fmt` to fix).
- Run examples:
  - Backtest: `cargo run -- backtest -c config/backtest.toml`
  - Paper: `cargo run -- paper -c config/paper.toml`
  - Live: `cargo run -- live -c config/prod.toml`
- Config & Secrets: keep profiles under `config/*.toml`; load secrets from environment (do not commit real keys).

## Testing Strategy (TDD & Continuous Testing)
- Unit tests: `#[cfg(test)]` inside modules for indicators, math, risk checks, parsing.
- Integration tests: `tests/` for end‑to‑end backtests and execution/risk flows.
- Determinism: backtests must be repeatable with identical inputs; fix random seeds.
- Property tests (optional): `proptest` for invariants (e.g., P&L accounting, position math).
- Commands: `cargo test` (all), or target modules via `cargo test <name>`; use `-- --ignored` for long‑running tests.
- Gate: tests must pass locally and in CI; add regression tests for every bug fix.

## Code Style and Quality Guidelines
- Style: `rustfmt` enforced; 4‑space indentation; avoid `unsafe`.
- Naming: `CamelCase` for types/traits, `snake_case` for functions/files, `SCREAMING_SNAKE_CASE` for consts.
- Errors: return `Result<>`; prefer `thiserror`/`anyhow`; avoid `unwrap()`/`expect()` in production paths.
- Finance types: use `rust_decimal` (not `f64`) for money; maintain explicit precision.
- Concurrency: prefer async tasks; isolate CPU‑bound work via `spawn_blocking`; avoid global mutable state; DI via traits.
- Docs: use `///` doc comments; document non‑obvious logic and public APIs.

## Continuous Integration & Deployment
- CI steps: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test` on each push/PR.
- Security/Dependencies: `cargo audit` (and optionally `cargo deny`) in CI for vulnerabilities and license policy.
- Artifacts: build release binaries; optional container image (multi‑stage Dockerfile) running as non‑root.
- Monitoring: capture structured logs and metrics in staging/live; treat errors as high‑priority with test‑first fixes.

## Effective Use of AI Coding Agents (Codex & Claude)
- Context via AGENTS.md: this file provides stack, build, testing, and style rules. Keep it current and focused on agent needs.
- Small, Focused Tasks: break work into narrow, reviewable steps; avoid monolithic changes.
- Iterative Development: ask the agent to plan, implement incrementally, and run tests after each change.
- Conformity: ensure AI output matches our architecture and conventions; never alter or delete tests to “pass”.
- Review & VC Discipline: always review AI changes; use feature branches and PRs; meaningful commit messages; never push directly to main.
- Context Management: if responses drift, reduce scope or start a fresh session; open only relevant files to the agent.

### Agent Roles & Patterns (merged from ccpm)
> "Don't anthropomorphize subagents… Subagents are best when they can do lots of work but then provide small amounts of information back to the main conversation thread." — Adam Wolff, Anthropic

- Available agents (patterns):
  - code-analyzer: scan many files → report bugs/findings succinctly.
  - file-analyzer: read verbose logs/configs → return key insights.
  - test-runner: execute tests → summarize failures/root causes.
  - parallel-worker: coordinate multiple sub-agents → consolidate status.
- How to use: run heavy work in agents, return 10–20% summary to preserve main context. Prefer parallel worker streams for orthogonal tasks.
- Anti-patterns: no "specialist" personas, no verbose dumps, no agent-to-agent chatter, and don’t use agents for trivial edits.
- PM integration (if using Claude Code PM): `/pm:issue-start` spawns parallel work; `/testing:run` uses test-runner; keep progress synced via issue comments. In Codex-only flows, treat these as checklists and mirror behavior manually.

## Security Considerations
- Secrets: never hard‑code; use env vars and secret managers; avoid exposing secrets to AI prompts.
- Least Privilege: run agents/tools in restricted environments; log their actions where possible.
- Safe Code: scrutinize shell execution, input parsing, and external calls; prefer parameterized queries and vetted libs.
- Dependency Security: review new deps; use `npm audit`/Snyk; keep packages updated.
- Rust Security: use `cargo audit`/`cargo deny`; avoid `unsafe`; validate all external inputs; apply idempotent `client_order_id` for exchange ops.
- Security Testing: include security linters/scanners in CI as appropriate.
- Agent Policies: agents must not add deps, make network calls, or modify tests without explicit approval.

## Conclusion
Reliability comes from disciplined process: TDD, frequent testing, clean code, thorough reviews, CI gates, and vigilant security. AI agents amplify velocity when used within these guardrails. Keep changes small, test relentlessly, and maintain architectural consistency to ensure high‑quality, maintainable software.
