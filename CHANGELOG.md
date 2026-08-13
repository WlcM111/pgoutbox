# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-09

First public release, extracted from the House VPN platform where the pattern
has been running in production.

### Added

- `AddTx` stores an event inside the caller's transaction.
- `RunPublisher` delivers pending events with exponential backoff, per-key
  ordering and dead-lettering.
- `MarkProcessed` gives consumers idempotency without extra infrastructure.
- `RunCleanup` bounds table growth.
- `Publisher` interface decouples the core from any specific broker; a Kafka
  implementation ships as a separate module.
- Schema embedded via `go:embed` and exposed as `SchemaSQL`.
- `OnEvent` hook for metrics without a metrics dependency.