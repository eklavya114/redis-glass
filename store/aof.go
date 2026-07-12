package store

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"redis-glass/resp"
)

// FsyncPolicy controls how aggressively the AOF forces buffered writes to
// physical disk. This is the same tradeoff real Redis exposes via its
// appendfsync setting, and for the same reason: fsync is the only thing that
// survives a *power loss* (a plain buffered write survives a process crash —
// the OS still has the bytes — but not a hard power-off before the OS flushes
// its own buffers). More fsyncs = more durable = slower.
type FsyncPolicy int

const (
	// FsyncEverySec buffers writes normally (so a process crash loses
	// nothing — the OS already has the bytes after Flush) and fsyncs once
	// per second in the background. At most ~1s of writes are at risk in
	// a true power-loss event. This is the default, matching real Redis's
	// own default and its documented "good enough for almost everyone" tradeoff.
	FsyncEverySec FsyncPolicy = iota
	// FsyncAlways fsyncs after every single write. Strongest durability,
	// slowest — every command pays a disk-sync round trip.
	FsyncAlways
	// FsyncNo never explicitly fsyncs; the OS decides when buffered writes
	// hit disk (typically within 30s on Linux). Fastest, weakest durability.
	FsyncNo
)

// ParseFsyncPolicy parses the AOF_FSYNC env var value, defaulting to
// FsyncEverySec for an empty or unrecognized value.
func ParseFsyncPolicy(s string) FsyncPolicy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "always":
		return FsyncAlways
	case "no":
		return FsyncNo
	default:
		return FsyncEverySec
	}
}

// errRewriteAlreadyInProgress is returned by Rewrite when another rewrite is
// already running. Overlapping rewrites aren't just wasteful — each one
// resets the shared rewrite buffer, so running two at once would corrupt
// each other's bookkeeping and silently drop writes (see rewriteRunning).
var errRewriteAlreadyInProgress = errors.New("aof rewrite already in progress")

// AOF provides append-only-file persistence: every mutating command is
// appended as a RESP array, and the file can be replayed to rebuild state.
type AOF struct {
	path   string
	file   *os.File
	mu     sync.Mutex
	writer *bufio.Writer
	fsync  FsyncPolicy

	rewriteThreshold int64 // bytes; 0 disables size-triggered auto-rewrite

	// rewriting and rewriteBuf implement the same "rewrite buffer" technique
	// real Redis uses to keep BGREWRITEAOF correct without blocking writers
	// for the full disk-write duration. See the long comment on Rewrite for
	// why this is necessary, not just defensive extra code.
	rewriting  bool
	rewriteBuf bytes.Buffer

	// rewriteRunning serializes Rewrite() against itself. Without this, two
	// overlapping rewrites (e.g. BGREWRITEAOF called again before the first
	// finishes) would each reset rewriteBuf and flip `rewriting`
	// independently, corrupting each other's bookkeeping and silently
	// dropping writes that landed between the two — a real bug caught by a
	// concurrency stress test, not a theoretical one.
	rewriteRunning bool
	rewriteMu      sync.Mutex
}

// NewAOF opens (or creates) the AOF file at path with the given fsync policy.
func NewAOF(path string, fsync FsyncPolicy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &AOF{path: path, file: f, writer: bufio.NewWriter(f), fsync: fsync}, nil
}

// SetRewriteThreshold sets the file size (bytes) that triggers an automatic
// rewrite when checked by StartAutoRewrite. 0 disables size-triggered rewrites.
func (a *AOF) SetRewriteThreshold(bytes int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rewriteThreshold = bytes
}

// Write appends cmd and args to the AOF as a RESP array. Whether this also
// fsyncs to disk immediately depends on the configured FsyncPolicy — see
// StartFsyncLoop for the "everysec" background sync.
//
// Write alone does NOT make "mutate the store, then log it" atomic with
// respect to a concurrent Rewrite — callers that need that guarantee (i.e.
// every mutating command) must go through WithLock instead, wrapping both
// the store mutation and the writeLocked call in one critical section. See
// the comment on Rewrite for why this matters.
func (a *AOF) Write(cmd string, args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.writeLocked(cmd, args)
}

// WithLock runs fn while holding the AOF's lock, so a store mutation done
// inside fn and the AOF entry logged inside fn (via writeLocked-aware
// helpers, i.e. Write called from within fn would deadlock — use
// writeLockedFromGuard instead) are atomic with respect to Rewrite. If a is
// nil, fn just runs unlocked (AOF disabled — nothing to protect against).
func (a *AOF) WithLock(fn func() error) error {
	if a == nil {
		return fn()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return fn()
}

// WriteLocked appends cmd/args to the AOF. Callers must already hold a's
// lock (i.e. be running inside a WithLock callback) — this is the counterpart
// mutating command handlers call after a successful store mutation, still
// inside the same WithLock critical section, to make the pair atomic.
func (a *AOF) WriteLocked(cmd string, args []string) error {
	return a.writeLocked(cmd, args)
}

// writeLocked does the actual serialization. Caller must hold a.mu.
func (a *AOF) writeLocked(cmd string, args []string) error {
	all := append([]string{cmd}, args...)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(len(all)) + "\r\n")
	for _, s := range all {
		buf.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
	}

	if _, err := a.writer.Write(buf.Bytes()); err != nil {
		return err
	}
	if err := a.writer.Flush(); err != nil {
		return err
	}
	if a.fsync == FsyncAlways {
		if err := a.file.Sync(); err != nil {
			return err
		}
	}

	// A rewrite is in progress: mirror this entry into its buffer so it gets
	// appended to the fresh file once the rewrite's base snapshot is written,
	// instead of being silently dropped when the old file is replaced.
	if a.rewriting {
		a.rewriteBuf.Write(buf.Bytes())
	}
	return nil
}

// Flush ensures any buffered writes reach the OS (not necessarily fsynced to disk).
func (a *AOF) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.writer.Flush()
}

// Sync fsyncs the underlying file, forcing buffered OS writes to physical disk.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.writer.Flush(); err != nil {
		return err
	}
	return a.file.Sync()
}

// StartFsyncLoop runs the "everysec" background fsync ticker until ctx is
// canceled. It's a no-op loop (still started, but harmless) unless the
// configured policy is FsyncEverySec.
func (a *AOF) StartFsyncLoop(ctx context.Context) {
	if a.fsync != FsyncEverySec {
		return
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.Sync(); err != nil {
				log.Printf("aof: everysec fsync failed: %v", err)
			}
		}
	}
}

// StartAutoRewrite periodically checks the AOF file size and triggers
// Rewrite when it exceeds the configured threshold. Checking is done on a
// timer (not on every write) specifically so a burst of writes doesn't stat()
// the file on every single command.
func (a *AOF) StartAutoRewrite(ctx context.Context, s *Store) {
	if a.rewriteThreshold <= 0 {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(a.path)
			if err != nil {
				continue
			}
			if info.Size() >= a.rewriteThreshold {
				log.Printf("aof: size %d bytes exceeds threshold %d, rewriting", info.Size(), a.rewriteThreshold)
				a.runRewriteRecovered(s)
			}
		}
	}
}

// runRewriteRecovered calls Rewrite with a panic guard, so a bug in the
// rewrite path fails (and logs) just this one attempt on this ticker,
// instead of an unrecovered panic taking down the entire server — the same
// isolation bgRewriteAOF's goroutine applies for the manual command path.
func (a *AOF) runRewriteRecovered(s *Store) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("aof: auto-rewrite panicked (recovered): %v", r)
		}
	}()
	if err := a.Rewrite(s); err != nil {
		log.Printf("aof: auto-rewrite failed: %v", err)
	}
}

// Close flushes and closes the underlying file.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.writer.Flush(); err != nil {
		return err
	}
	return a.file.Close()
}

// Rewrite compacts the AOF to the minimal set of commands needed to
// reconstruct current state (one SET per live string key instead of its
// full mutation history, chunked RPUSH/HSET for lists/hashes instead of one
// command per historical push/field-set), then atomically swaps it in for
// the live file.
//
// Correctness (why this needs a rewrite buffer, not just a snapshot):
// store mutation and its AOF log entry are two separate steps in every
// command handler (mutate the store, *then* log it) — not one atomic
// transaction — because this server handles connections concurrently
// (goroutine-per-connection), unlike real Redis's single-threaded command
// loop where "mutate then log" is atomic for free. That means a naive
// "snapshot the store, write it out, swap the file in" rewrite has a race:
// if the snapshot runs *after* some command's mutation but *before* that
// command's own log call, the command's effect is captured in the snapshot
// AND then logged again into the fresh file — harmless for idempotent
// commands like SET, but a real correctness bug for INCR/DECR/LPUSH/RPUSH,
// whose replayed effect would be double-applied (e.g. an INCR replayed
// twice produces the wrong final value).
//
// The fix: command handlers call store mutation + AOF log as one atomic
// unit via WithLock/WriteLocked (see commands/handler.go), so a rewrite can
// never observe a "half-completed" command. And instead of blocking all
// writes for the (potentially slow) disk-write portion of a rewrite, this
// uses the same technique real Redis does: flip a.rewriting under the lock,
// take the snapshot, release the lock, do the slow disk I/O — and any
// command that logs *during* that window (checked in writeLocked) mirrors
// its entry into a.rewriteBuf instead of being silently dropped when the
// old file is replaced. Once the base snapshot is on disk, the buffered
// entries are appended to it (in the order they happened) before the swap.
// Net effect: every command is represented exactly once in the rewritten
// file, and writers are only blocked for two short, fixed-cost critical
// sections (the initial snapshot, and the final swap), never for the full
// duration of writing the base snapshot to disk.
func (a *AOF) Rewrite(s *Store) error {
	a.rewriteMu.Lock()
	if a.rewriteRunning {
		a.rewriteMu.Unlock()
		return errRewriteAlreadyInProgress
	}
	a.rewriteRunning = true
	a.rewriteMu.Unlock()
	defer func() {
		a.rewriteMu.Lock()
		a.rewriteRunning = false
		a.rewriteMu.Unlock()
	}()

	a.mu.Lock()
	a.rewriting = true
	a.rewriteBuf.Reset()
	snap := s.Snapshot()
	a.mu.Unlock()

	// Disk I/O for the base snapshot happens without holding a.mu — live
	// commands proceed normally against the current file during this window,
	// mirroring their AOF entries into a.rewriteBuf (see writeLocked).
	tmpPath := a.path + ".rewrite.tmp"
	rewriteErr := func() error {
		tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("aof rewrite: open temp file: %w", err)
		}
		defer tmpFile.Close()

		w := bufio.NewWriter(tmpFile)
		if err := writeSnapshot(w, snap); err != nil {
			return fmt.Errorf("aof rewrite: write temp file: %w", err)
		}

		// Re-acquire the lock just to append whatever was buffered during the
		// disk write above, and to finalize the swap — both fast, fixed-cost.
		a.mu.Lock()
		defer a.mu.Unlock()
		if _, err := w.Write(a.rewriteBuf.Bytes()); err != nil {
			return fmt.Errorf("aof rewrite: append buffered writes: %w", err)
		}
		bufferedCount := a.rewriteBuf.Len()
		a.rewriting = false
		a.rewriteBuf.Reset()

		if err := w.Flush(); err != nil {
			return fmt.Errorf("aof rewrite: flush temp file: %w", err)
		}
		if err := tmpFile.Sync(); err != nil {
			return fmt.Errorf("aof rewrite: sync temp file: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			return fmt.Errorf("aof rewrite: close temp file: %w", err)
		}

		// Close the old handle before rename: on Windows, renaming over a file
		// that's still open by another handle can fail, so drop ours first.
		if err := a.file.Close(); err != nil {
			return fmt.Errorf("aof rewrite: close live file: %w", err)
		}

		if err := os.Rename(tmpPath, a.path); err != nil {
			// Best-effort: reopen the original file so the server can keep running.
			if f, reopenErr := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644); reopenErr == nil {
				a.file = f
				a.writer = bufio.NewWriter(f)
			}
			return fmt.Errorf("aof rewrite: rename temp file into place: %w", err)
		}

		f, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("aof rewrite: reopen rewritten file: %w", err)
		}
		a.file = f
		a.writer = bufio.NewWriter(f)

		log.Printf("aof: rewrite complete (%d string keys, %d lists, %d hashes, %d bytes of concurrent writes replayed)",
			len(snap.Strings), len(snap.Lists), len(snap.Hashes), bufferedCount)
		return nil
	}()

	if rewriteErr != nil {
		// Ensure we don't leave rewriting stuck true (and keep buffering forever) on error paths
		// that returned before reaching the unlock above (e.g. the tmpFile open failure).
		a.mu.Lock()
		if a.rewriting {
			a.rewriting = false
			a.rewriteBuf.Reset()
		}
		a.mu.Unlock()
		os.Remove(tmpPath)
	}
	return rewriteErr
}

// chunkSize bounds how many list elements / hash field-value pairs go into a
// single rewritten RPUSH/HSET command. Without this, one very large list or
// hash would serialize as one enormous RESP array line — chunking keeps
// individual AOF lines (and the buffers needed to parse them back) bounded,
// at the cost of a few extra commands for big keys.
const chunkSize = 500

func writeSnapshot(w *bufio.Writer, snap Snapshot) error {
	now := time.Now()
	for key, entry := range snap.Strings {
		args := []string{key, entry.Value}
		if !entry.ExpiresAt.IsZero() {
			remaining := entry.ExpiresAt.Sub(now)
			if remaining <= 0 {
				continue // already expired as of the snapshot; omit it entirely
			}
			args = append(args, "PX", strconv.FormatInt(remaining.Milliseconds(), 10))
		}
		if err := writeRESPCommand(w, "SET", args); err != nil {
			return err
		}
	}

	for key, elems := range snap.Lists {
		for i := 0; i < len(elems); i += chunkSize {
			end := i + chunkSize
			if end > len(elems) {
				end = len(elems)
			}
			args := append([]string{key}, elems[i:end]...)
			if err := writeRESPCommand(w, "RPUSH", args); err != nil {
				return err
			}
		}
	}

	for key, fields := range snap.Hashes {
		pairs := make([]string, 0, len(fields)*2)
		for f, v := range fields {
			pairs = append(pairs, f, v)
		}
		for i := 0; i < len(pairs); i += chunkSize * 2 {
			end := i + chunkSize*2
			if end > len(pairs) {
				end = len(pairs)
			}
			args := append([]string{key}, pairs[i:end]...)
			if err := writeRESPCommand(w, "HSET", args); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeRESPCommand(w *bufio.Writer, cmd string, args []string) error {
	all := append([]string{cmd}, args...)
	if _, err := w.WriteString("*" + strconv.Itoa(len(all)) + "\r\n"); err != nil {
		return err
	}
	for _, s := range all {
		if _, err := w.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"); err != nil {
			return err
		}
	}
	return nil
}

// Replay reads the AOF from the beginning and re-executes each command
// directly against store, rebuilding state. Malformed or unsupported
// entries are logged and skipped rather than aborting the replay.
func (a *AOF) Replay(s *Store) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(a.file)

	count := 0
	for {
		val, err := resp.Parse(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Printf("aof: stopping replay after parse error: %v", err)
			break
		}
		if val.Typ != '*' || len(val.Array) == 0 {
			continue
		}
		cmd := strings.ToUpper(val.Array[0].Str)
		args := make([]string, len(val.Array)-1)
		for i, v := range val.Array[1:] {
			args[i] = v.Str
		}
		if err := replayCommand(s, cmd, args); err != nil {
			log.Printf("aof: skipping %s during replay: %v", cmd, err)
			continue
		}
		count++
	}

	if _, err := a.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	log.Printf("aof: replayed %d command(s)", count)
	return nil
}

func replayCommand(s *Store, cmd string, args []string) error {
	switch cmd {
	case "SET":
		if len(args) < 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		ttl, err := parseSetTTL(args[2:])
		if err != nil {
			return err
		}
		return s.Set(args[0], args[1], ttl)
	case "DEL":
		if len(args) < 1 {
			return fmt.Errorf("wrong number of arguments")
		}
		s.Del(args...)
	case "EXPIRE":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		seconds, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		s.Expire(args[0], seconds)
	case "INCR":
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments")
		}
		_, err := s.IncrBy(args[0], 1)
		return err
	case "DECR":
		if len(args) != 1 {
			return fmt.Errorf("wrong number of arguments")
		}
		_, err := s.IncrBy(args[0], -1)
		return err
	case "INCRBY":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		_, err = s.IncrBy(args[0], n)
		return err
	case "DECRBY":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		_, err = s.IncrBy(args[0], -n)
		return err
	case "LPUSH":
		if len(args) < 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		_, err := s.LPush(args[0], args[1:]...)
		return err
	case "RPUSH":
		if len(args) < 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		_, err := s.RPush(args[0], args[1:]...)
		return err
	case "HSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return fmt.Errorf("wrong number of arguments")
		}
		_, err := s.HSet(args[0], args[1:]...)
		return err
	case "HDEL":
		if len(args) < 2 {
			return fmt.Errorf("wrong number of arguments")
		}
		s.HDel(args[0], args[1:]...)
	case "FLUSHALL":
		s.FlushAll()
	default:
		return fmt.Errorf("unsupported command in AOF replay")
	}
	return nil
}

func parseSetTTL(rest []string) (time.Duration, error) {
	ttl := time.Duration(0)
	for i := 0; i < len(rest); i++ {
		switch strings.ToUpper(rest[i]) {
		case "EX":
			if i+1 >= len(rest) {
				return 0, fmt.Errorf("syntax error")
			}
			seconds, err := strconv.Atoi(rest[i+1])
			if err != nil {
				return 0, err
			}
			ttl = time.Duration(seconds) * time.Second
			i++
		case "PX":
			if i+1 >= len(rest) {
				return 0, fmt.Errorf("syntax error")
			}
			ms, err := strconv.Atoi(rest[i+1])
			if err != nil {
				return 0, err
			}
			ttl = time.Duration(ms) * time.Millisecond
			i++
		default:
			return 0, fmt.Errorf("syntax error")
		}
	}
	return ttl, nil
}
