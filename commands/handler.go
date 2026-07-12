// Package commands dispatches parsed RESP commands against the store,
// writes AOF entries for mutations, and reports each command to the
// observability recorder. It is the glue between the resp, store, and
// events packages.
package commands

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"redis-glass/events"
	"redis-glass/resp"
	"redis-glass/store"
)

// ConnState tracks per-connection state, currently just auth status.
type ConnState struct {
	Authenticated bool
}

// Metrics exposes read-only server metrics for the INFO command.
type Metrics interface {
	Uptime() time.Duration
	Connections() int64
	Commands() int64
}

// errSkipAOFLog is a sentinel a mutate closure passed to mutateAndLog can
// return to signal "the mutation completed with no error, but there's
// nothing to log" (e.g. EXPIRE on a key that doesn't exist) — mutateAndLog
// treats it as success without writing an AOF entry.
var errSkipAOFLog = errors.New("redis-glass: skip aof log")

// writeStoreErr translates a store-layer error (currently only ErrOOM) into
// the matching RESP error reply. Returns true if err was handled (a reply
// was written), false if err was nil.
func writeStoreErr(w *bufio.Writer, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if errors.Is(err, store.ErrOOM) {
		return true, resp.WriteError(w, "OOM command not allowed when used memory > 'maxmemory'")
	}
	return true, resp.WriteError(w, "ERR "+err.Error())
}

// Handler dispatches parsed RESP commands against a Store.
type Handler struct {
	store          *store.Store
	password       string
	aof            *store.AOF
	metrics        Metrics
	maxMemoryBytes int64
	events         *events.Recorder
}

// New returns a Handler backed by store s. password enables AUTH when
// non-empty; aof may be nil to run without persistence. Metrics, memory
// limit, and observability are wired in separately via the Set* methods.
func New(s *store.Store, password string, aof *store.AOF) *Handler {
	return &Handler{store: s, password: password, aof: aof}
}

// SetMetrics wires up the metrics source used by INFO. Optional.
func (h *Handler) SetMetrics(m Metrics) {
	h.metrics = m
}

// SetMaxMemory records the configured ceiling (bytes, 0 = unlimited) purely
// for display in INFO's maxmemory field — the actual enforcement lives in
// store.Store, which is configured separately via store.SetMaxMemory.
func (h *Handler) SetMaxMemory(bytes int64) {
	h.maxMemoryBytes = bytes
}

// SetEvents wires up the observability recorder (live command stream,
// latency histograms, slow log, hotkeys). Optional — if never set, Handle
// just skips recording with no behavior change.
func (h *Handler) SetEvents(r *events.Recorder) {
	h.events = r
}

// Handle executes a command and writes its RESP reply to w. clientAddr is
// used only for observability (the live command stream and slow log show
// which client issued a command) — it never affects command semantics.
func (h *Handler) Handle(cmd string, args []resp.Value, w *bufio.Writer, state *ConnState, clientAddr string) error {
	start := time.Now()
	upper := strings.ToUpper(cmd)

	var err error
	switch upper {
	case "PING":
		err = h.ping(args, w)
	case "ECHO":
		err = h.echo(args, w)
	case "AUTH":
		err = h.auth(args, w, state)
	case "SET":
		err = h.set(args, w)
	case "GET":
		err = h.get(args, w)
	case "DEL":
		err = h.del(args, w)
	case "EXISTS":
		err = h.exists(args, w)
	case "EXPIRE":
		err = h.expire(args, w)
	case "TTL":
		err = h.ttl(args, w)
	case "KEYS":
		err = h.keys(args, w)
	case "SCAN":
		err = h.scan(args, w)
	case "FLUSHALL":
		err = h.flushAll(args, w)
	case "INCR":
		err = h.incrBy(args, w, 1, false)
	case "DECR":
		err = h.incrBy(args, w, -1, false)
	case "INCRBY":
		err = h.incrBy(args, w, 0, true)
	case "DECRBY":
		err = h.decrBy(args, w)
	case "LPUSH":
		err = h.push(args, w, "LPUSH", h.store.LPush)
	case "RPUSH":
		err = h.push(args, w, "RPUSH", h.store.RPush)
	case "LRANGE":
		err = h.lrange(args, w)
	case "HSET":
		err = h.hset(args, w)
	case "HGET":
		err = h.hget(args, w)
	case "HDEL":
		err = h.hdel(args, w)
	case "INFO":
		err = h.info(args, w)
	case "BGREWRITEAOF":
		err = h.bgRewriteAOF(args, w)
	case "SLOWLOG":
		err = h.slowlog(args, w)
	case "HOTKEYS":
		err = h.hotkeys(args, w)
	case "COMMAND":
		err = h.command(args, w)
	default:
		err = resp.WriteError(w, "ERR unknown command '"+cmd+"'")
	}

	if h.events != nil {
		h.events.Record(upper, argStrings(args), clientAddr, time.Since(start))
	}
	return err
}

func argStrings(args []resp.Value) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = a.Str
	}
	return out
}

// mutateAndLog runs mutate — which performs a store mutation — and, if it
// succeeds, logs cmd/args to the AOF. The mutation and the log entry are
// atomic with respect to a concurrent AOF rewrite (see AOF.Rewrite's doc
// comment for why this matters: without it, a rewrite could observe this
// mutation in its snapshot and then see this same command logged again
// afterward, which would double-apply non-idempotent commands like INCR or
// LPUSH on replay). If AOF is disabled, mutate just runs with no locking.
//
// mutate can return errSkipAOFLog to indicate "succeeded, but don't log"
// (e.g. EXPIRE on a missing key) — that's reported to the caller as success
// (nil) with no AOF entry written.
func (h *Handler) mutateAndLog(cmd string, args []string, mutate func() error) error {
	logic := func() error {
		err := mutate()
		if err == errSkipAOFLog {
			return nil
		}
		if err != nil {
			return err
		}
		if h.aof != nil {
			return h.aof.WriteLocked(cmd, args)
		}
		return nil
	}
	if h.aof == nil {
		return logic()
	}
	return h.aof.WithLock(logic)
}

func (h *Handler) ping(args []resp.Value, w *bufio.Writer) error {
	if len(args) == 0 {
		return resp.WriteSimpleString(w, "PONG")
	}
	return resp.WriteBulkString(w, args[0].Str)
}

func (h *Handler) echo(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'echo' command")
	}
	return resp.WriteBulkString(w, args[0].Str)
}

func (h *Handler) auth(args []resp.Value, w *bufio.Writer, state *ConnState) error {
	if len(args) != 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'auth' command")
	}
	if h.password == "" {
		return resp.WriteError(w, "ERR Client sent AUTH, but no password is set")
	}
	if args[0].Str != h.password {
		return resp.WriteError(w, "WRONGPASS invalid username-password pair")
	}
	if state != nil {
		state.Authenticated = true
	}
	return resp.WriteSimpleString(w, "OK")
}

func (h *Handler) set(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'set' command")
	}
	key, val := args[0].Str, args[1].Str
	ttl := time.Duration(0)

	rest := argStrings(args[2:])
	for i := 0; i < len(rest); i++ {
		switch strings.ToUpper(rest[i]) {
		case "EX":
			if i+1 >= len(rest) {
				return resp.WriteError(w, "ERR syntax error")
			}
			seconds, err := strconv.Atoi(rest[i+1])
			if err != nil {
				return resp.WriteError(w, "ERR value is not an integer or out of range")
			}
			ttl = time.Duration(seconds) * time.Second
			i++
		case "PX":
			if i+1 >= len(rest) {
				return resp.WriteError(w, "ERR syntax error")
			}
			ms, err := strconv.Atoi(rest[i+1])
			if err != nil {
				return resp.WriteError(w, "ERR value is not an integer or out of range")
			}
			ttl = time.Duration(ms) * time.Millisecond
			i++
		default:
			return resp.WriteError(w, "ERR syntax error")
		}
	}

	err := h.mutateAndLog("SET", argStrings(args), func() error {
		return h.store.Set(key, val, ttl)
	})
	if err != nil {
		_, werr := writeStoreErr(w, err)
		return werr
	}
	return resp.WriteSimpleString(w, "OK")
}

func (h *Handler) get(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'get' command")
	}
	val, ok := h.store.Get(args[0].Str)
	if !ok {
		return resp.WriteNullBulk(w)
	}
	return resp.WriteBulkString(w, val)
}

func (h *Handler) del(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'del' command")
	}
	var n int
	h.mutateAndLog("DEL", argStrings(args), func() error {
		n = h.store.Del(argStrings(args)...)
		return nil
	})
	return resp.WriteInteger(w, n)
}

func (h *Handler) exists(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'exists' command")
	}
	return resp.WriteInteger(w, h.store.Exists(argStrings(args)...))
}

func (h *Handler) expire(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'expire' command")
	}
	seconds, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.WriteError(w, "ERR value is not an integer or out of range")
	}
	var existed bool
	h.mutateAndLog("EXPIRE", argStrings(args), func() error {
		existed = h.store.Expire(args[0].Str, seconds)
		if !existed {
			return errSkipAOFLog
		}
		return nil
	})
	if existed {
		return resp.WriteInteger(w, 1)
	}
	return resp.WriteInteger(w, 0)
}

func (h *Handler) ttl(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'ttl' command")
	}
	return resp.WriteInteger(w, h.store.TTL(args[0].Str))
}

func (h *Handler) keys(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'keys' command")
	}
	return resp.WriteArray(w, h.store.Keys(args[0].Str))
}

// scan implements SCAN cursor [MATCH pattern] [COUNT n]. See store.Scan for
// why this is the production-safe alternative to KEYS on a large keyspace.
func (h *Handler) scan(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'scan' command")
	}
	cursor, err := strconv.Atoi(args[0].Str)
	if err != nil {
		return resp.WriteError(w, "ERR invalid cursor")
	}

	pattern := "*"
	count := 10
	rest := argStrings(args[1:])
	for i := 0; i < len(rest); i++ {
		switch strings.ToUpper(rest[i]) {
		case "MATCH":
			if i+1 >= len(rest) {
				return resp.WriteError(w, "ERR syntax error")
			}
			pattern = rest[i+1]
			i++
		case "COUNT":
			if i+1 >= len(rest) {
				return resp.WriteError(w, "ERR syntax error")
			}
			n, err := strconv.Atoi(rest[i+1])
			if err != nil {
				return resp.WriteError(w, "ERR value is not an integer or out of range")
			}
			count = n
			i++
		default:
			return resp.WriteError(w, "ERR syntax error")
		}
	}

	next, keys := h.store.Scan(cursor, pattern, count)
	if _, err := w.WriteString("*2\r\n"); err != nil {
		return err
	}
	if err := resp.WriteBulkString(w, strconv.Itoa(next)); err != nil {
		return err
	}
	return resp.WriteArray(w, keys)
}

func (h *Handler) bgRewriteAOF(args []resp.Value, w *bufio.Writer) error {
	if h.aof == nil {
		return resp.WriteError(w, "ERR AOF is not enabled")
	}
	go func() {
		// Isolate this background rewrite the same way server.go isolates a
		// single connection: a bug here should fail this one rewrite attempt
		// (logged), not take down the whole process. Without this, a panic
		// anywhere in Rewrite/Snapshot/file I/O would propagate out of this
		// unrecovered goroutine and crash the entire server — worse than any
		// bookkeeping issue it might otherwise cause.
		defer func() {
			if r := recover(); r != nil {
				log.Printf("aof: BGREWRITEAOF panicked (recovered): %v", r)
			}
		}()
		if err := h.aof.Rewrite(h.store); err != nil {
			log.Printf("aof: BGREWRITEAOF failed: %v", err)
		}
	}()
	return resp.WriteSimpleString(w, "Background append only file rewriting started")
}

// slowlog implements SLOWLOG GET [n] / SLOWLOG LEN / SLOWLOG RESET.
func (h *Handler) slowlog(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'slowlog' command")
	}
	if h.events == nil {
		return resp.WriteError(w, "ERR SLOWLOG is not enabled")
	}
	switch strings.ToUpper(args[0].Str) {
	case "LEN":
		return resp.WriteInteger(w, h.events.SlowlogLen())
	case "RESET":
		h.events.SlowlogReset()
		return resp.WriteSimpleString(w, "OK")
	case "GET":
		n := 10
		if len(args) >= 2 {
			parsed, err := strconv.Atoi(args[1].Str)
			if err != nil {
				return resp.WriteError(w, "ERR value is not an integer or out of range")
			}
			n = parsed
		}
		entries := h.events.SlowlogGet(n)
		if _, err := w.WriteString("*" + strconv.Itoa(len(entries)) + "\r\n"); err != nil {
			return err
		}
		for _, e := range entries {
			fields := append([]string{e.Command}, e.Args...)
			if _, err := w.WriteString("*5\r\n"); err != nil {
				return err
			}
			if err := resp.WriteInteger(w, int(e.ID)); err != nil {
				return err
			}
			if err := resp.WriteInteger(w, int(e.Time.Unix())); err != nil {
				return err
			}
			if err := resp.WriteInteger(w, int(e.DurationUs)); err != nil {
				return err
			}
			if err := resp.WriteArray(w, fields); err != nil {
				return err
			}
			if err := resp.WriteBulkString(w, e.ClientAddr); err != nil {
				return err
			}
		}
		return nil
	default:
		return resp.WriteError(w, "ERR unknown SLOWLOG subcommand")
	}
}

// hotkeys implements HOTKEYS [n]: the top-n most-accessed keys over roughly
// the last 60 seconds, as a flat array of alternating key/count-as-string.
func (h *Handler) hotkeys(args []resp.Value, w *bufio.Writer) error {
	if h.events == nil {
		return resp.WriteError(w, "ERR HOTKEYS is not enabled")
	}
	n := 10
	if len(args) >= 1 {
		parsed, err := strconv.Atoi(args[0].Str)
		if err != nil {
			return resp.WriteError(w, "ERR value is not an integer or out of range")
		}
		n = parsed
	}
	top := h.events.HotKeys(n)
	flat := make([]string, 0, len(top)*2)
	for _, hk := range top {
		flat = append(flat, hk.Key, strconv.FormatInt(hk.Count, 10))
	}
	return resp.WriteArray(w, flat)
}

func (h *Handler) flushAll(args []resp.Value, w *bufio.Writer) error {
	h.mutateAndLog("FLUSHALL", nil, func() error {
		h.store.FlushAll()
		return nil
	})
	return resp.WriteSimpleString(w, "OK")
}

// incrBy handles INCR (delta=1), DECR (delta=-1), and INCRBY (delta ignored, read from args[1]).
func (h *Handler) incrBy(args []resp.Value, w *bufio.Writer, delta int, readDelta bool) error {
	name := "incr"
	if delta < 0 {
		name = "decr"
	}
	if readDelta {
		name = "incrby"
		if len(args) != 2 {
			return resp.WriteError(w, "ERR wrong number of arguments for '"+name+"' command")
		}
		n, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.WriteError(w, "ERR value is not an integer or out of range")
		}
		delta = n
	} else if len(args) != 1 {
		return resp.WriteError(w, "ERR wrong number of arguments for '"+name+"' command")
	}

	var result int
	err := h.mutateAndLog(strings.ToUpper(name), argStrings(args), func() error {
		r, err := h.store.IncrBy(args[0].Str, delta)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		_, werr := writeStoreErr(w, err)
		return werr
	}
	return resp.WriteInteger(w, result)
}

func (h *Handler) decrBy(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'decrby' command")
	}
	n, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.WriteError(w, "ERR value is not an integer or out of range")
	}
	var result int
	mutErr := h.mutateAndLog("DECRBY", argStrings(args), func() error {
		r, err := h.store.IncrBy(args[0].Str, -n)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	if mutErr != nil {
		_, werr := writeStoreErr(w, mutErr)
		return werr
	}
	return resp.WriteInteger(w, result)
}

func (h *Handler) push(args []resp.Value, w *bufio.Writer, cmdName string, op func(string, ...string) (int, error)) error {
	if len(args) < 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for '"+strings.ToLower(cmdName)+"' command")
	}
	var n int
	err := h.mutateAndLog(cmdName, argStrings(args), func() error {
		r, err := op(args[0].Str, argStrings(args[1:])...)
		if err != nil {
			return err
		}
		n = r
		return nil
	})
	if err != nil {
		_, werr := writeStoreErr(w, err)
		return werr
	}
	return resp.WriteInteger(w, n)
}

func (h *Handler) lrange(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 3 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'lrange' command")
	}
	start, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.WriteError(w, "ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2].Str)
	if err != nil {
		return resp.WriteError(w, "ERR value is not an integer or out of range")
	}
	return resp.WriteArray(w, h.store.LRange(args[0].Str, start, stop))
}

func (h *Handler) hset(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'hset' command")
	}
	var n int
	err := h.mutateAndLog("HSET", argStrings(args), func() error {
		r, err := h.store.HSet(args[0].Str, argStrings(args[1:])...)
		if err != nil {
			return err
		}
		n = r
		return nil
	})
	if err != nil {
		_, werr := writeStoreErr(w, err)
		return werr
	}
	return resp.WriteInteger(w, n)
}

func (h *Handler) hget(args []resp.Value, w *bufio.Writer) error {
	if len(args) != 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'hget' command")
	}
	val, ok := h.store.HGet(args[0].Str, args[1].Str)
	if !ok {
		return resp.WriteNullBulk(w)
	}
	return resp.WriteBulkString(w, val)
}

func (h *Handler) hdel(args []resp.Value, w *bufio.Writer) error {
	if len(args) < 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'hdel' command")
	}
	var n int
	h.mutateAndLog("HDEL", argStrings(args), func() error {
		n = h.store.HDel(args[0].Str, argStrings(args[1:])...)
		return nil
	})
	return resp.WriteInteger(w, n)
}

func (h *Handler) info(args []resp.Value, w *bufio.Writer) error {
	var uptime time.Duration
	var conns, cmds int64
	if h.metrics != nil {
		uptime = h.metrics.Uptime()
		conns = h.metrics.Connections()
		cmds = h.metrics.Commands()
	}

	body := fmt.Sprintf(
		"# Server\r\nredis_version:0.1.0\r\nos:linux\r\ngo_version:%s\r\nuptime_in_seconds:%d\r\n\r\n"+
			"# Clients\r\nconnected_clients:%d\r\n\r\n"+
			"# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\n\r\n"+
			"# Stats\r\ntotal_commands_processed:%d\r\n\r\n"+
			"# Commandstats\r\n%s\r\n"+
			"# Keyspace\r\ndb0:keys=%d,expires=%d\r\n",
		runtime.Version(), int(uptime.Seconds()), conns,
		h.store.ApproxMemoryBytes(), h.maxMemoryBytes,
		cmds, h.commandStatsLines(), h.store.KeyCount(), h.store.ExpiryCount(),
	)
	return resp.WriteBulkString(w, body)
}

// commandStatsLines renders each command's stats in real Redis's
// cmdstat_<name>:calls=N,usec_per_call=F format, plus percentile fields
// (which real Redis reports separately under LATENCYSTATS, but folding them
// in here keeps this project's INFO output to one section instead of two).
func (h *Handler) commandStatsLines() string {
	if h.events == nil {
		return ""
	}
	stats := h.events.CommandStats()
	var b strings.Builder
	for _, s := range stats {
		usecPerCall := float64(0)
		if s.Calls > 0 {
			usecPerCall = float64(s.TotalUs) / float64(s.Calls)
		}
		fmt.Fprintf(&b, "cmdstat_%s:calls=%d,usec_per_call=%.2f,p50_usec=%d,p95_usec=%d,p99_usec=%d\r\n",
			strings.ToLower(s.Command), s.Calls, usecPerCall, s.P50Us, s.P95Us, s.P99Us)
	}
	return b.String()
}

func (h *Handler) command(args []resp.Value, w *bufio.Writer) error {
	if len(args) >= 1 && strings.ToUpper(args[0].Str) == "COUNT" {
		return resp.WriteInteger(w, 0)
	}
	return resp.WriteArray(w, []string{})
}
