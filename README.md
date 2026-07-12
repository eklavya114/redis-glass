# redis-glass

![Go](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue)
![build](https://img.shields.io/badge/build-passing%20locally-brightgreen)
![dependencies](https://img.shields.io/badge/dependencies-stdlib%20only-lightgrey)

A Redis-protocol-compatible key-value store, written from scratch in Go with
zero external dependencies — and the one you can actually *see inside of*.

Every Redis-compatible server has the same blind spot: you can't see what's
happening inside it without bolting on RedisInsight, standing up Prometheus +
`redis_exporter`, or grep-ing through a live `MONITOR` session. redis-glass
ships that visibility as a built-in, zero-config dashboard — a live command
stream, per-command latency percentiles, a slow query log, and a "what's hot
right now" leaderboard — over one Server-Sent-Events connection, with no extra
infrastructure to run.

<!-- TODO: add dashboard screenshot (a real capture of the running dashboard under load) -->

## Quick start

```
go run .
```

No environment variables required. Open the dashboard at
`http://localhost:8080` and you'll see live stats, with every command you send
via `redis-cli` (or any Redis client) streaming in underneath in real time.

With Docker:

```
docker compose up --build
```

Same dashboard at `http://localhost:8080`, RESP server on `localhost:6379`,
persistence to a named volume, and a default password (`changeme` — change it
in `docker-compose.yml` or a `.env` file based on `.env.example`).

## Observability

This is the reason redis-glass exists. Open `http://localhost:8080` while the
server is running: it's a single page, no build step, no CDN dependency —
connected over one `EventSource` (SSE) stream and updating live.

- **Server panel** — uptime, connection count, ops/sec, memory usage against
  the configured ceiling, key counts.
- **Live command stream** — every command executed anywhere on the server, as
  it happens: command, arguments, client address, execution time. A `tail -f`
  of your database, not a page you refresh.
- **Per-command latency** — calls, average, and p50/p95/p99 for each command
  type, so you can see *which* command is slow, not just that "something" is.
  Also exposed over the wire via `INFO`'s `# Commandstats` section.
- **Slow query log** — anything crossing `SLOWLOG_THRESHOLD_MS` (default 10ms),
  with full command, arguments, and client address. Also queryable with
  `SLOWLOG GET`.
- **Hot keys** — the top-N most-accessed keys over roughly the last 60 seconds.
  This is the question most "why is my cache under load" incidents start with,
  and it isn't something Redis exposes out of the box. Also queryable with
  `HOTKEYS`.

Any key matching a `SENSITIVE_KEYS` glob pattern has its *value* redacted
(`[REDACTED]`) everywhere in the observability layer — the live stream, the
slow log, and the hot-keys panel. The key name stays visible; the value never
appears. This activates the moment you set the env var: a live command stream
without it would be a real liability, so it isn't left to chance.

> ⚠️ **The dashboard has no access control unless you set `DASHBOARD_PASSWORD`.**
> It shows live command data — keys, and values unless `SENSITIVE_KEYS` is set —
> to anyone who can reach the port. This is fine on `localhost` or a fully
> trusted network; it is not fine on anything else. Set `DASHBOARD_PASSWORD` and
> the dashboard requires HTTP Basic Auth (any username, that password) on every
> route, including the SSE stream.

## Connecting from your app

redis-glass speaks standard RESP, so any Redis client library works unmodified:

**Go** ([go-redis](https://github.com/redis/go-redis)):
```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "secret"})
```

**Python** ([redis-py](https://github.com/redis/redis-py)):
```python
r = redis.Redis(host='localhost', port=6379, password='secret')
```

**Node.js** ([node-redis](https://github.com/redis/node-redis)):
```js
const client = createClient({ socket: { host: 'localhost', port: 6379 }, password: 'secret' });
```

If `REDIS_PASSWORD` isn't set, omit the password entirely.

## Command reference

| Command | Notes |
|---|---|
| `PING [msg]` | Replies `PONG`, or echoes `msg`. Always works, even before `AUTH`. |
| `ECHO msg` | Replies with `msg` |
| `AUTH password` | Authenticates the connection when `REDIS_PASSWORD` is set |
| `SET key value [EX seconds \| PX ms]` | Optional expiry |
| `GET key` | Returns the value, or nil if missing/expired |
| `DEL key [key ...]` | Deletes keys of any type, returns count deleted |
| `EXISTS key [key ...]` | Returns count of keys that exist (any type) |
| `EXPIRE key seconds` | Sets TTL on an existing string key |
| `TTL key` | Seconds remaining, `-1` if no expiry, `-2` if key doesn't exist |
| `KEYS pattern` | `*`/`prefix*` glob match. Fine for small/offline use — see `SCAN` for production |
| `SCAN cursor [MATCH pattern] [COUNT n]` | Cursor-based iteration; never holds the store lock for a full scan |
| `FLUSHALL` | Deletes all keys of every type |
| `INCR` / `DECR key` | Increments/decrements an integer-valued key |
| `INCRBY` / `DECRBY key n` | Increments/decrements by `n` |
| `LPUSH` / `RPUSH key val [val ...]` | Pushes onto a list, returns new length |
| `LRANGE key start stop` | Range of list elements (negative indices supported) |
| `HSET key field value [field value ...]` | Sets hash fields, returns count added |
| `HGET key field` | Returns a hash field's value |
| `HDEL key field [field ...]` | Deletes hash fields, returns count removed |
| `INFO [section]` | Server/clients/memory/stats/commandstats/keyspace info |
| `SLOWLOG GET [n] / LEN / RESET` | Inspect or clear the slow query log |
| `HOTKEYS [n]` | Top-N most-accessed keys over the last ~60 seconds |
| `BGREWRITEAOF` | Manually triggers AOF compaction (auto-triggers by size too) |
| `COMMAND COUNT` | Stub reply for client library startup handshakes |

Commands are case-insensitive (`SET`, `set`, `Set` are equivalent).

## Persistence (AOF)

When `AOF_PATH` is set, every mutating command is appended to the file as RESP
and (depending on `AOF_FSYNC`) flushed/synced to disk. On startup the file is
replayed to rebuild state before the server accepts connections. It is
periodically compacted (`BGREWRITEAOF`, automatic above `AOF_REWRITE_SIZE_MB`)
down to the minimal command set needed to rebuild current state, rather than
growing forever. On `SIGINT`/`SIGTERM` (including `docker stop`), the AOF is
synced before exit.

> ⚠️ **`AOF_FSYNC=always` is not "a bit safer, a bit slower" — it is
> dramatically slower under concurrent load.** In the load test below it dropped
> sustained throughput ~17x (**~50,000 → ~2,877 ops/sec**) and pushed `SET` p99
> latency from **22ms to 219ms**, because every mutation blocks on a synchronous
> `fsync` while holding the AOF lock, serializing all writes through one syscall
> at a time. `everysec` (the default) performed on par with running no AOF at
> all and is the right choice for almost everyone. Only pick `always` if
> per-write, survive-a-power-loss durability genuinely outweighs a ~17x
> throughput cost.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_PASSWORD` | (empty = no auth) | Connection password |
| `AOF_PATH` | (empty = no persistence) | Append-only-file path; set to enable persistence |
| `AOF_FSYNC` | `everysec` | `always` / `everysec` / `no` — durability vs. speed tradeoff |
| `AOF_REWRITE_SIZE_MB` | `64` | AOF size (MB) that triggers automatic compaction |
| `DASHBOARD_PORT` | `8080` | Monitoring dashboard port; set empty to disable |
| `DASHBOARD_PASSWORD` | (empty = no auth) | Requires HTTP Basic Auth on the dashboard; **set this before exposing the dashboard beyond localhost** |
| `TLS_CERT_FILE` | (empty = no TLS) | Path to TLS cert PEM |
| `TLS_KEY_FILE` | (empty = no TLS) | Path to TLS key PEM |
| `MAXMEMORY_MB` | (empty = unlimited) | Approximate memory ceiling; writes past it are rejected with `-OOM` |
| `SLOWLOG_THRESHOLD_MS` | `10` | Minimum command duration logged to the slow query log |
| `SENSITIVE_KEYS` | (empty = no redaction) | Comma-separated glob patterns (`session:*,token:*`) whose values are redacted in all observability surfaces |

All variables are optional. Omitting every one of them gives a plain-TCP,
unauthenticated, non-persistent server — every feature is additive, never
load-bearing for existing usage.

### TLS

Set `TLS_CERT_FILE` and `TLS_KEY_FILE` together to enable TLS. For local
testing, generate a self-signed certificate:

```
openssl req -x509 -newkey rsa:4096 -keyout key.pem \
  -out cert.pem -days 365 -nodes \
  -subj "/CN=localhost"

TLS_CERT_FILE=cert.pem TLS_KEY_FILE=key.pem go run .
```

If either variable is missing or empty, the server runs plain TCP.

## Performance

These are this implementation's **own** measured numbers — a single-node,
deliberately unoptimized Go implementation — on one machine (12 logical CPUs,
client and server sharing the same box). They are **not** compared against
Redis or Valkey; no head-to-head benchmark on the same hardware has been run,
so read them as "what this code does here," not as a competitive claim.

Methodology: a Go load-test client holding N persistent connections, each
running a 40/40/10/10 mix of `GET`/`SET`/`INCR`/`LPUSH` against a 2,000-key
pool, latency measured client-side. (`redis-benchmark` wasn't available in the
test environment.)

**Sustained throughput** (300 connections, 2.5 minutes, no AOF): after an
initial burst of ~75K ops/sec, throughput settled to a stable **~48–50K
ops/sec** and held there. Latency at that sustained load:

| Command | p50 | p95 | p99 |
|---|---|---|---|
| `GET` | 1.8ms | 5.5ms | 10ms |
| `SET` / `INCR` / `LPUSH` | 7.5ms | 18ms | 22ms |

**Where it degrades:** throughput peaks around ~50 connections and then
plateaus while p99 latency on writes climbs roughly linearly with added
connections (`SET` p99: 2.6ms at 50 conns → 6.7ms at 100 → 12.7ms at 200 →
21ms at 400). Isolating the cause: an all-`GET` workload (concurrent-reader
`RLock`) hit ~96K ops/sec vs. ~71K for the mixed workload at 300 connections —
a ~36% gap directly attributable to the exclusive write lock. At 12 connections
read-only and mixed performed identically, so the write lock only becomes a
limiter as concurrency rises. Server CPU during the 300-connection run sat at
~6–7 of 12 cores — real headroom that added connections did not convert into
throughput, so the write lock is **a** ceiling but not the only one (see
[Known limitations](CHANGELOG.md#known-limitations)).

**AOF fsync mode matters enormously** (same 300-connection workload):

| Config | Sustained ops/sec | `SET` p50 / p99 |
|---|---|---|
| No AOF | ~50,000 | 7.5ms / 22ms |
| `AOF_FSYNC=everysec` (default) | ~54,000–66,000 | 5.9ms / 20ms |
| `AOF_FSYNC=always` | **~2,877** | **167ms / 219ms** |

`everysec` performs on par with no persistence at all. `always` is a ~17x
collapse — see the warning under [Persistence](#persistence-aof) before
choosing it.

## Why this exists — and what it isn't

redis-glass is a Redis-protocol-compatible store built to be **understood** and
**observed**, not to compete with Redis or Valkey on raw throughput,
clustering, or ecosystem maturity.

**What it is NOT:**
- Not a Redis Cluster replacement — this is a single node, full stop.
- Not claiming to be faster than Redis or Valkey. The
  [Performance](#performance) section reports this implementation's own numbers
  on one machine; it does **not** compare them against Redis or Valkey, because
  no head-to-head benchmark on the same hardware has been run. Treat them as
  "what this Go implementation does," never as a competitive claim.
- Not aiming for command-set parity with Redis. It covers strings, lists,
  hashes, and expiry — not sorted sets, streams, pub/sub, Lua scripting, or
  clustering.

**What it IS:**
- A real RESP-protocol server any standard Redis client can talk to unmodified.
- The version of this idea where you can see everything the store is doing —
  every command, every latency percentile, every slow query, every hot key —
  without installing anything else.
- Small, dependency-free, and readable end to end.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the ground rules — stdlib-only, no
unverified performance claims, and the package layering — before opening a PR.

## License

[MIT](LICENSE) © eklavya114
