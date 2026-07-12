# Contributing to redis-glass

Thanks for considering a contribution. This project has a narrow, deliberate
scope — please read the "What this is NOT" section of the README before
proposing anything large, so we're aligned on direction before you invest time.

## Ground rules

- **stdlib only.** No external dependencies, anywhere, for any reason. This is
  a hard constraint, not a preference — it's core to what the project is.
- **Every new feature must be optional and off-by-default-safe.** If it needs
  configuration, it should be an environment variable with a default that
  reproduces the exact previous behavior. Nothing should require a config
  change just to keep working as before.
- **Don't break RESP wire compatibility.** Existing Redis clients (redis-cli,
  go-redis, redis-py, node-redis, ...) must keep working against this server
  unmodified. If you're adding a command, match real Redis's argument order,
  error message format, and reply type as closely as practical.
- **No performance claims without benchmarks.** Don't add or edit
  documentation claiming this is faster/better than Redis or Valkey unless
  you've run a real, reproducible benchmark and included the numbers and
  methodology. Absence of a claim is always safe; an unverified claim is not.

## Code organization

The package boundaries are intentional — see `info.md` for the full
architecture writeup before moving code between packages:

- `resp` — wire protocol only, no knowledge of commands or storage.
- `store` — the in-memory data + AOF persistence, no knowledge of networking.
- `events` — the observability core (histograms, slow log, hotkeys, live
  stream), no knowledge of `store` or `commands` (both of those depend on it,
  not the other way around).
- `commands` — the glue: dispatches parsed RESP onto `store` and `events`.
- `server` — TCP/TLS listener, auth gating, per-connection metrics.
- `monitor` — the HTTP dashboard, reads from `events` only.

Adding a package that creates a cycle between these (e.g. having `store`
import `commands`) will be rejected regardless of how clean the resulting
code looks — the acyclic layering is what makes each package independently
testable and reasoned-about.

## Before submitting a change

1. `go build ./...` and `go vet ./...` must both pass with zero output.
2. `gofmt -l .` must return nothing (run `gofmt -w .` to fix).
3. If you touched command behavior, test it against a real RESP client, not
   just by reading the code — a hand-rolled TCP client is enough; there's no
   project-standard test harness yet (see "Tests" below).
4. If you touched `store/aof.go`'s rewrite or replay logic specifically: this
   is the trickiest correctness surface in the project (see the long comment
   on `AOF.Rewrite`). Changes here need a concurrency stress test — hammer a
   non-idempotent command (`INCR`, `LPUSH`) against concurrent
   `BGREWRITEAOF` calls, then restart and confirm the replayed value is
   exactly right, not just "close."

## Tests

There's no `_test.go` suite yet. If you're adding one, the natural starting
points are `store` (pure functions, no network, easiest to unit test) and
`resp` (round-trip encode/decode). Contributions that add real test coverage
are very welcome even without an accompanying feature.

## Reporting bugs / proposing features

Open an issue describing the problem or use case before sending a large PR —
especially for anything touching persistence correctness or the observability
data structures, since those have the most subtle failure modes in this
codebase.
