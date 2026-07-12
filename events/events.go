// Package events is redis-glass's observability core: it's the shared,
// dependency-free data structure that commands.Handler writes to after every
// command (latency histograms, the slow query log, per-key hotness, the live
// command stream) and that monitor's dashboard reads from. It depends on
// nothing else in this project — that's deliberate, so neither commands nor
// monitor has to import the other; both import events instead.
package events

import (
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Event is one executed command, as shown on the live dashboard stream.
type Event struct {
	Time       time.Time `json:"time"`
	Command    string    `json:"command"`
	Args       []string  `json:"args"`
	ClientAddr string    `json:"clientAddr"`
	DurationUs int64     `json:"durationUs"`
}

// SlowEntry is one recorded slow-query-log entry.
type SlowEntry struct {
	ID         int64     `json:"id"`
	Time       time.Time `json:"time"`
	DurationUs int64     `json:"durationUs"`
	Command    string    `json:"command"`
	Args       []string  `json:"args"`
	ClientAddr string    `json:"clientAddr"`
}

// HotKey is one entry in the top-N-most-accessed-keys leaderboard.
type HotKey struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// bucketBoundsUs are the fixed latency histogram bucket upper bounds, in
// microseconds: <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, >=500ms.
var bucketBoundsUs = [7]int64{1000, 5000, 10000, 50000, 100000, 500000, math.MaxInt64}

// CommandStat is the per-command latency summary shown in INFO's
// Commandstats section and the dashboard's latency bar chart.
type CommandStat struct {
	Command string   `json:"command"`
	Calls   int64    `json:"calls"`
	TotalUs int64    `json:"totalUs"`
	P50Us   int64    `json:"p50Us"`
	P95Us   int64    `json:"p95Us"`
	P99Us   int64    `json:"p99Us"`
	Buckets [7]int64 `json:"buckets"`
}

// keyBearingCommands are the commands whose first argument is a data key,
// for hotkey-tracking purposes. Commands not in this set (PING, INFO,
// FLUSHALL, SLOWLOG, ...) don't have a meaningful "key" to attribute
// hotness to, so they're excluded rather than polluting the leaderboard.
var keyBearingCommands = map[string]bool{
	"SET": true, "GET": true, "DEL": true, "EXISTS": true, "EXPIRE": true, "TTL": true,
	"INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
	"LPUSH": true, "RPUSH": true, "LRANGE": true,
	"HSET": true, "HGET": true, "HDEL": true,
}

// histogram is one command's latency distribution: total calls/time for the
// average, plus fixed buckets for the p50/p95/p99 estimate. Fixed buckets
// (rather than storing every sample) keep memory bounded regardless of
// traffic volume, at the cost of the percentiles being estimates rounded to
// a bucket boundary rather than exact values — an accepted, documented
// tradeoff for a lightweight built-in stat, not a precision instrument.
type histogram struct {
	calls   int64
	totalUs int64
	buckets [7]int64
}

func (h *histogram) record(us int64) {
	h.calls++
	h.totalUs += us
	for i, bound := range bucketBoundsUs {
		if us < bound {
			h.buckets[i]++
			return
		}
	}
	h.buckets[len(h.buckets)-1]++
}

// percentile estimates the p-th percentile (p in [0,1]) as the upper bound
// of the first bucket whose cumulative count reaches that percentile.
func (h *histogram) percentile(p float64) int64 {
	if h.calls == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(h.calls)))
	var cum int64
	for i, c := range h.buckets {
		cum += c
		if cum >= target {
			if i == len(bucketBoundsUs)-1 {
				return bucketBoundsUs[i-1] // overflow bucket has no finite upper bound; report its lower edge
			}
			return bucketBoundsUs[i]
		}
	}
	return bucketBoundsUs[len(bucketBoundsUs)-2]
}

const (
	hotkeyBucketWidth = 5 * time.Second
	hotkeyBucketCount = 12 // 12 * 5s = 60s rolling window
)

// hotkeyTracker implements a cheap sliding-window counter: a fixed ring of
// time-sliced buckets, each holding per-key counts for its slice. Reading
// the top-N sums whichever buckets still fall inside the rolling window.
// This avoids two much worse alternatives: a single global counter (no way
// to "age out" old activity) or storing a raw timestamped log of every
// access (unbounded memory under sustained traffic).
type hotkeyTracker struct {
	mu       sync.Mutex
	buckets  [hotkeyBucketCount]map[string]int64
	slotTime [hotkeyBucketCount]int64 // which window-index each slot was last reset for
}

func newHotkeyTracker() *hotkeyTracker {
	t := &hotkeyTracker{}
	for i := range t.buckets {
		t.buckets[i] = make(map[string]int64)
	}
	return t
}

func (t *hotkeyTracker) access(key string) {
	window := time.Now().Unix() / int64(hotkeyBucketWidth/time.Second)
	slot := int(window % hotkeyBucketCount)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.slotTime[slot] != window {
		// This slot belongs to an old (or fresh) window; reusing it for the
		// current window means its previous contents have aged out.
		t.buckets[slot] = make(map[string]int64)
		t.slotTime[slot] = window
	}
	t.buckets[slot][key]++
}

func (t *hotkeyTracker) top(n int) []HotKey {
	currentWindow := time.Now().Unix() / int64(hotkeyBucketWidth/time.Second)
	totals := make(map[string]int64)

	t.mu.Lock()
	for i, bucket := range t.buckets {
		if currentWindow-t.slotTime[i] < hotkeyBucketCount {
			for k, v := range bucket {
				totals[k] += v
			}
		}
	}
	t.mu.Unlock()

	out := make([]HotKey, 0, len(totals))
	for k, v := range totals {
		out = append(out, HotKey{Key: k, Count: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}

// slowlog is a bounded ring of recorded slow commands, oldest entries
// dropped once maxLen is exceeded — matching real Redis's SLOWLOG behavior.
type slowlog struct {
	mu          sync.Mutex
	entries     []SlowEntry
	maxLen      int
	thresholdUs int64
	nextID      int64
}

func newSlowlog(threshold time.Duration, maxLen int) *slowlog {
	return &slowlog{thresholdUs: threshold.Microseconds(), maxLen: maxLen}
}

func (s *slowlog) maybeRecord(cmd string, args []string, clientAddr string, durationUs int64) {
	if s.thresholdUs < 0 || durationUs < s.thresholdUs {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	s.entries = append(s.entries, SlowEntry{
		ID: s.nextID, Time: time.Now(), DurationUs: durationUs,
		Command: cmd, Args: args, ClientAddr: clientAddr,
	})
	if len(s.entries) > s.maxLen {
		s.entries = s.entries[len(s.entries)-s.maxLen:]
	}
}

// get returns up to n entries, most recent first (matching real Redis's SLOWLOG GET order).
func (s *slowlog) get(n int) []SlowEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || n > len(s.entries) {
		n = len(s.entries)
	}
	out := make([]SlowEntry, 0, n)
	for i := len(s.entries) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, s.entries[i])
	}
	return out
}

func (s *slowlog) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *slowlog) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = nil
}

// Recorder is the single point every mutating and read command reports
// through. All its public methods are safe to call concurrently from many
// goroutines (one per client connection).
type Recorder struct {
	histMu sync.Mutex
	hist   map[string]*histogram

	hotkeys *hotkeyTracker
	slow    *slowlog

	sensitivePatterns []string

	subMu     sync.Mutex
	subs      map[int]chan Event
	nextSubID int
	subCount  atomic.Int32
}

// NewRecorder builds a Recorder. slowThreshold is the minimum duration for a
// command to be logged to the slow query log; slowMaxLen bounds how many
// entries it retains. sensitivePatterns are glob patterns (same syntax as
// KEYS: "*" or "prefix*") — a command whose first argument matches one of
// them has all its other arguments redacted in every observability surface
// (live stream, slow log, hotkeys), though the key name itself is still shown.
func NewRecorder(slowThreshold time.Duration, slowMaxLen int, sensitivePatterns []string) *Recorder {
	return &Recorder{
		hist:              make(map[string]*histogram),
		hotkeys:           newHotkeyTracker(),
		slow:              newSlowlog(slowThreshold, slowMaxLen),
		sensitivePatterns: sensitivePatterns,
		subs:              make(map[int]chan Event),
	}
}

// Record reports one completed command. cmd is upper-cased internally; args
// should be the raw (unredacted) argument strings — redaction for display
// happens inside Record, using the real values only for the parts of the
// pipeline (histograms, hotkeys) that don't need them.
func (r *Recorder) Record(cmd string, args []string, clientAddr string, duration time.Duration) {
	cmd = strings.ToUpper(cmd)
	us := duration.Microseconds()

	r.histMu.Lock()
	h, ok := r.hist[cmd]
	if !ok {
		h = &histogram{}
		r.hist[cmd] = h
	}
	h.record(us)
	r.histMu.Unlock()

	if keyBearingCommands[cmd] && len(args) > 0 {
		r.hotkeys.access(args[0])
	}

	displayArgs := redactArgs(args, r.sensitivePatterns)
	r.slow.maybeRecord(cmd, displayArgs, clientAddr, us)

	// Skip the broadcast entirely (not just the network write — the
	// allocation and struct-building too) when nobody's watching, so the
	// live-stream feature costs nothing when the dashboard is closed.
	if r.subCount.Load() > 0 {
		r.broadcast(Event{
			Time: time.Now(), Command: cmd, Args: displayArgs,
			ClientAddr: clientAddr, DurationUs: us,
		})
	}
}

// Subscribe registers for live command events. Call the returned function
// to unsubscribe when done (e.g. when the dashboard connection closes).
func (r *Recorder) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 32) // buffered so one slow reader can't stall Record on the hot path
	r.subMu.Lock()
	id := r.nextSubID
	r.nextSubID++
	r.subs[id] = ch
	r.subMu.Unlock()
	r.subCount.Add(1)

	unsubscribed := false
	var once sync.Mutex
	unsubscribe := func() {
		once.Lock()
		defer once.Unlock()
		if unsubscribed {
			return
		}
		unsubscribed = true
		r.subMu.Lock()
		delete(r.subs, id)
		r.subMu.Unlock()
		r.subCount.Add(-1)
		close(ch)
	}
	return ch, unsubscribe
}

func (r *Recorder) broadcast(evt Event) {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	for _, ch := range r.subs {
		select {
		case ch <- evt:
		default:
			// Subscriber's buffer is full — drop the event rather than
			// block command processing. This is a live tail, not a durable
			// queue: an occasional dropped event under heavy load is the
			// right tradeoff versus ever stalling a client's command.
		}
	}
}

// CommandStats returns a snapshot of every command's latency stats, sorted
// by command name.
func (r *Recorder) CommandStats() []CommandStat {
	r.histMu.Lock()
	defer r.histMu.Unlock()
	out := make([]CommandStat, 0, len(r.hist))
	for cmd, h := range r.hist {
		out = append(out, CommandStat{
			Command: cmd,
			Calls:   h.calls,
			TotalUs: h.totalUs,
			P50Us:   h.percentile(0.50),
			P95Us:   h.percentile(0.95),
			P99Us:   h.percentile(0.99),
			Buckets: h.buckets,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Command < out[j].Command })
	return out
}

// SlowlogGet returns up to n slow-log entries, most recent first.
func (r *Recorder) SlowlogGet(n int) []SlowEntry { return r.slow.get(n) }

// SlowlogLen returns the current number of retained slow-log entries.
func (r *Recorder) SlowlogLen() int { return r.slow.len() }

// SlowlogReset clears the slow log.
func (r *Recorder) SlowlogReset() { r.slow.reset() }

// HotKeys returns the top-n most-accessed keys over the last ~60 seconds.
func (r *Recorder) HotKeys(n int) []HotKey { return r.hotkeys.top(n) }

func redactArgs(args []string, patterns []string) []string {
	if len(args) == 0 || len(patterns) == 0 || !matchesAny(args[0], patterns) {
		return args
	}
	out := make([]string, len(args))
	out[0] = args[0]
	for i := 1; i < len(args); i++ {
		out[i] = "[REDACTED]"
	}
	return out
}

func matchesAny(key string, patterns []string) bool {
	for _, p := range patterns {
		if globMatch(p, key) {
			return true
		}
	}
	return false
}

func globMatch(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == key
}
