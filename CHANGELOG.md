# Changelog

## Unreleased

### Added
- `DASHBOARD_PASSWORD`: the observability dashboard (`/` and the `/monitor`
  SSE stream) had no access control at all — anyone who could reach the port
  saw live command data, including values unless `SENSITIVE_KEYS` was set.
  Setting this env var now requires HTTP Basic Auth (any username, that
  password, constant-time compared) on every dashboard route. Empty by
  default for backward compatibility, matching the rest of the server's
  opt-in pattern — but this is a real exposure if left unset on anything
  beyond localhost/a trusted network, not just a convenience default.

## v0.1.0

Entries below are grouped by the development phase they landed in rather
than finer-grained version numbers.

## Known limitations

These are measured, not speculative — see the Performance section of the
README for the numbers and methodology behind them.

- **Global store `sync.RWMutex` is an architectural throughput ceiling.**
  Every write (`SET`/`INCR`/`LPUSH`/`HSET`/...) takes one process-wide
  exclusive lock, so writes cannot proceed in parallel no matter how many
  cores are available. A load test showed a read-only workload sustaining
  ~96K ops/sec vs. ~71K for a mixed read/write workload at 300 connections
  (~36% gap attributable to this lock), and write p99 latency climbing
  roughly linearly with connection count. Addressing this (lock striping /
  sharding the keyspace across independent locks) is a candidate for a future
  version; it was deliberately **not** done for the current work, which
  prioritized correctness and observability over write concurrency.
- **There is a second, unexplained throughput ceiling below the CPU limit.**
  Even the lock-free read path plateaued around ~96K ops/sec while the server
  process used only ~6–7 of 12 logical cores — meaning something other than
  the write lock is also capping total throughput (candidates: per-connection
  goroutine scheduling, per-command RESP parse/encode allocation, TCP
  round-trip overhead). This has not been isolated with a CPU profile and is
  an **open question** for anyone who wants to profile it further.

## Phase 2 — Observability

The differentiator: built-in visibility into what the server is doing, with
zero extra infrastructure (no RedisInsight, no Prometheus + redis_exporter,
no manual `MONITOR` grep-fu).

### Added
- New `events` package: the shared observability core. Depends on nothing
  else in the project, so neither `commands` nor `monitor` has to import the
  other.
- Live command stream: the dashboard's `/monitor` endpoint is now a
  Server-Sent-Events connection that pushes every executed command in real
  time (command, args, client address, duration), not a polling refresh.
  Costs nothing when no one is watching — the broadcast is skipped entirely
  (not just left unsent) when the subscriber count is zero.
- Per-command latency histograms with p50/p95/p99 estimates (fixed 7-bucket
  histogram, bounded memory regardless of traffic volume), surfaced via
  `INFO`'s new `# Commandstats` section and a live bar-chart-style table on
  the dashboard.
- `SLOWLOG GET [n]` / `SLOWLOG LEN` / `SLOWLOG RESET` — a bounded ring
  buffer (default 128 entries) of commands exceeding `SLOWLOG_THRESHOLD_MS`
  (default 10ms), matching real Redis's semantics.
- `HOTKEYS [n]` — the top-N most-accessed keys over a rolling ~60-second
  window (sliding time-bucketed counters, not a single unbounded counter or
  a raw access log).
- `SENSITIVE_KEYS` env var (comma-separated glob patterns): any command
  whose key argument matches redacts every other argument to `[REDACTED]`
  across all three observability surfaces (live stream, slow log, hotkeys)
  — the key name itself stays visible, only values are hidden. Verified
  with the actual secret value never appearing in any captured stream/log
  output.
- Dashboard (`monitor/dashboard.go`) rewritten as a live single-page app:
  connection count, uptime, ops/sec, memory usage bar, live command stream,
  latency table, slow log, and hotkeys — all updating from one SSE
  connection. Inline CSS/JS only, no CDN dependency, works with zero
  internet access.

### Fixed during Phase 2 testing
- `INCR`/`DECR`/`INCRBY`/`DECRBY` were mapping *any* store error — including
  the Phase 1 `OOM` error — to `"value is not an integer or out of range"`.
  Now correctly distinguishes the two.

## Phase 1 — Correctness and durability

Fixes for real gaps in the original AOF and expiry implementation, found
before they'd matter at any real scale.

### Added
- `AOF.Rewrite()` / `BGREWRITEAOF`: compacts the append-only file to the
  minimal command set needed to reconstruct current state (one `SET` per
  live key instead of its full mutation history; `RPUSH`/`HSET` chunked in
  batches of 500 elements for large lists/hashes). Auto-triggers when the
  file exceeds `AOF_REWRITE_SIZE_MB` (default 64MB), checked every 30s.
- Configurable AOF fsync policy via `AOF_FSYNC`: `always` (fsync every
  write), `everysec` (default — buffer every write, fsync once/sec in the
  background), `no` (let the OS decide).
- Sampling-based active expiry: replaced the full-keyspace scan every 100ms
  with the same probabilistic algorithm real Redis uses — sample up to 20
  keys from a tracked set of keys-with-a-TTL, repeat immediately if the hit
  rate is high, otherwise wait for the next tick. Per-tick cost is now
  bounded by sample size, not keyspace size.
- `SCAN cursor [MATCH pattern] [COUNT n]`: cursor-based keyspace iteration
  that only holds the store lock long enough to copy key names, not for the
  full scan/match/paginate. `KEYS` is unchanged for compatibility but is now
  documented as the "fine for small/offline use, not for production" option.
- `MAXMEMORY_MB` ceiling: approximate byte-length accounting across all
  three data-type maps; writes that would exceed it are rejected with
  `-OOM command not allowed when used memory > 'maxmemory'` instead of
  letting the process grow unbounded. Surfaced via `INFO`'s `# Memory`
  section.

### Fixed
- **AOF rewrite could double-apply non-idempotent commands on replay.**
  Store mutation and its AOF log entry were two separate, non-atomic steps;
  a rewrite landing between them could snapshot a mutation and then see it
  logged again afterward. Harmless for `SET`, but would silently corrupt the
  replayed value of `INCR`/`DECR`/`LPUSH`/`RPUSH`. Fixed by making
  "mutate + log" one atomic unit with respect to a concurrent rewrite, and
  giving `Rewrite()` an in-memory buffer (the same technique real Redis
  uses) so writes landing during the slow disk-write phase are captured
  exactly once without blocking other commands for that duration.
- **Overlapping `BGREWRITEAOF` calls could corrupt each other's bookkeeping
  and silently drop writes.** Found by a concurrency stress test (500
  sequential `INCR`+`RPUSH` pairs racing 60 concurrent `BGREWRITEAOF`
  calls), not by inspection — nothing previously stopped two rewrites from
  running at once, and each one reset the shared rewrite buffer
  independently. Rewrites are now serialized against each other.
- **A panic anywhere in the rewrite path would have crashed the entire
  server**, not just failed one rewrite attempt — neither background
  rewrite goroutine had the same panic isolation `server.go` already
  applies per-connection. Added matching `recover()` guards to both.
- `HSET`/`LPUSH`/`RPUSH`'s memory-ceiling check happens once, up front, for
  the whole batch — confirmed a rejected batch never partially writes some
  fields/elements before hitting the limit.

### Verified
- Concurrency stress test: 500 sequential `INCR`+`RPUSH` pairs against one
  connection, racing 60 concurrent `BGREWRITEAOF` calls from another, then a
  hard restart forcing full AOF replay — run 4 consecutive times after the
  fixes above, exactly correct every time (before the fixes, this same test
  reproducibly showed a lost update).
- AOF durability: kill-and-restart with data intact; compacted-file replay
  producing byte-for-byte-equivalent state to the pre-rewrite store.
- Docker: real `docker build` + `docker compose up --build`, RESP client
  AUTH/SET/GET against the running container, `docker compose restart`
  (real `SIGTERM` on Linux) proving graceful shutdown and AOF flush together.
