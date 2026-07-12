// Package store is the in-memory data layer: a thread-safe key-value store
// for strings, lists, and hashes with TTL support, plus append-only-file
// (AOF) persistence. It has no knowledge of networking or the RESP protocol.
package store

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrOOM is returned when a write would exceed the configured memory ceiling.
var ErrOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'")

// Entry is a single stored string value with an optional expiry.
type Entry struct {
	Value     string
	ExpiresAt time.Time // zero value means no expiry
}

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu     sync.RWMutex
	data   map[string]Entry
	lists  map[string][]string
	hashes map[string]map[string]string

	// expiryKeys tracks which string keys currently have a TTL set, so the
	// active-expiry sampler (see sampleExpire) can sample from this small
	// set instead of scanning the entire keyspace every tick.
	expiryKeys map[string]struct{}

	maxMemoryBytes int64 // 0 means unlimited
}

// Snapshot is a point-in-time, deep copy of all store data, used by AOF
// rewrite to serialize minimal-form commands without holding the store
// lock during (comparatively slow) disk I/O.
type Snapshot struct {
	Strings map[string]Entry
	Lists   map[string][]string
	Hashes  map[string]map[string]string
}

// New returns an empty Store with no memory limit.
func New() *Store {
	return &Store{
		data:       make(map[string]Entry),
		lists:      make(map[string][]string),
		hashes:     make(map[string]map[string]string),
		expiryKeys: make(map[string]struct{}),
	}
}

// SetMaxMemory sets the approximate memory ceiling in bytes. 0 disables the limit.
func (s *Store) SetMaxMemory(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxMemoryBytes = bytes
}

// Set stores key/value. ttl <= 0 means no expiry.
func (s *Store) Set(key, value string, ttl time.Duration) error {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkMemoryLocked(len(key) + len(value)); err != nil {
		return err
	}

	s.data[key] = Entry{Value: value, ExpiresAt: expiresAt}
	if expiresAt.IsZero() {
		delete(s.expiryKeys, key)
	} else {
		s.expiryKeys[key] = struct{}{}
	}
	return nil
}

// Get returns the value for key, checking expiry.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	entry, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	if s.isExpired(entry) {
		s.mu.Lock()
		s.deleteExpiredLocked(key)
		s.mu.Unlock()
		return "", false
	}
	return entry.Value, true
}

func (s *Store) isExpired(e Entry) bool {
	return !e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)
}

// deleteExpiredLocked removes an expired string key and its expiry tracking
// entry. Caller must hold s.mu (write lock).
func (s *Store) deleteExpiredLocked(key string) {
	delete(s.data, key)
	delete(s.expiryKeys, key)
}

// Del removes the given keys (of any type) and returns the count actually deleted.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, k := range keys {
		found := false
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			delete(s.expiryKeys, k)
			found = true
		}
		if _, ok := s.lists[k]; ok {
			delete(s.lists, k)
			found = true
		}
		if _, ok := s.hashes[k]; ok {
			delete(s.hashes, k)
			found = true
		}
		if found {
			count++
		}
	}
	return count
}

// Exists returns the count of keys that currently exist (and are not expired).
func (s *Store) Exists(keys ...string) int {
	count := 0
	for _, k := range keys {
		if _, ok := s.Get(k); ok {
			count++
			continue
		}
		s.mu.RLock()
		_, listOK := s.lists[k]
		_, hashOK := s.hashes[k]
		s.mu.RUnlock()
		if listOK || hashOK {
			count++
		}
	}
	return count
}

// Expire sets a TTL (in seconds) on an existing string key. Returns true if the key existed.
func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data[key]
	if !ok {
		return false
	}
	entry.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	s.data[key] = entry
	s.expiryKeys[key] = struct{}{}
	return true
}

// TTL returns the remaining seconds for key, -1 if no expiry, -2 if the key doesn't exist.
func (s *Store) TTL(key string) int {
	s.mu.RLock()
	entry, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return -2
	}
	if s.isExpired(entry) {
		s.mu.Lock()
		s.deleteExpiredLocked(key)
		s.mu.Unlock()
		return -2
	}
	if entry.ExpiresAt.IsZero() {
		return -1
	}
	remaining := time.Until(entry.ExpiresAt).Seconds()
	if remaining < 0 {
		return -2
	}
	return int(remaining + 0.5)
}

// Keys returns all keys (of any type) matching a simple glob pattern: "*" matches
// everything, "prefix*" matches a literal prefix, anything else matches exactly.
//
// KEYS walks the entire keyspace under a single lock and is fine for small
// datasets or offline debugging, but on a large keyspace it blocks every
// other command for the duration of the scan. Prefer SCAN (see Scan below)
// for production use — it only ever holds the lock long enough to copy key
// names, never for the full matching/pagination work.
func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	now := time.Now()
	for k, e := range s.data {
		if !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt) {
			continue
		}
		if matchGlob(pattern, k) {
			result = append(result, k)
		}
	}
	for k := range s.lists {
		if matchGlob(pattern, k) {
			result = append(result, k)
		}
	}
	for k := range s.hashes {
		if matchGlob(pattern, k) {
			result = append(result, k)
		}
	}
	return result
}

// Scan implements a cursor-based keyspace iteration. cursor is an opaque
// offset (0 means "start"); the returned nextCursor is 0 once iteration is
// complete. Unlike Keys, the store lock is held only long enough to copy key
// names out — the sort, glob match, and pagination all happen outside the
// lock, so a slow or large SCAN never blocks other commands for long.
//
// This is not perfectly consistent under concurrent modification (keys
// added/removed mid-scan can shift the ordering), which matches real Redis's
// own documented SCAN guarantees — it only promises that keys present for
// the whole scan are returned at least once, not a frozen snapshot.
func (s *Store) Scan(cursor int, pattern string, count int) (nextCursor int, keys []string) {
	if count <= 0 {
		count = 10
	}

	s.mu.RLock()
	all := make([]string, 0, len(s.data)+len(s.lists)+len(s.hashes))
	now := time.Now()
	for k, e := range s.data {
		if e.ExpiresAt.IsZero() || now.Before(e.ExpiresAt) {
			all = append(all, k)
		}
	}
	for k := range s.lists {
		all = append(all, k)
	}
	for k := range s.hashes {
		all = append(all, k)
	}
	s.mu.RUnlock()

	sort.Strings(all)

	if cursor < 0 || cursor >= len(all) {
		return 0, []string{}
	}
	end := cursor + count
	if end > len(all) {
		end = len(all)
	}
	page := all[cursor:end]
	next := end
	if next >= len(all) {
		next = 0
	}

	matched := make([]string, 0, len(page))
	for _, k := range page {
		if matchGlob(pattern, k) {
			matched = append(matched, k)
		}
	}
	return next, matched
}

func matchGlob(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == key
}

// FlushAll deletes every key of every type.
func (s *Store) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]Entry)
	s.lists = make(map[string][]string)
	s.hashes = make(map[string]map[string]string)
	s.expiryKeys = make(map[string]struct{})
}

// IncrBy adds delta to the integer value stored at key (default 0) and returns the result.
func (s *Store) IncrBy(key string, delta int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data[key]
	if ok && s.isExpired(entry) {
		ok = false
	}
	cur := 0
	if ok {
		n, err := strconv.Atoi(entry.Value)
		if err != nil {
			return 0, errors.New("value is not an integer or out of range")
		}
		cur = n
	}
	cur += delta
	newVal := strconv.Itoa(cur)
	if err := s.checkMemoryLocked(len(key) + len(newVal)); err != nil {
		return 0, err
	}
	s.data[key] = Entry{Value: newVal, ExpiresAt: entry.ExpiresAt}
	return cur, nil
}

// LPush prepends values (in argument order) to the list at key and returns its new length.
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkMemoryLocked(len(key) + sumLen(values)); err != nil {
		return 0, err
	}
	lst := s.lists[key]
	for _, v := range values {
		lst = append([]string{v}, lst...)
	}
	s.lists[key] = lst
	return len(lst), nil
}

// RPush appends values to the list at key and returns its new length.
func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkMemoryLocked(len(key) + sumLen(values)); err != nil {
		return 0, err
	}
	s.lists[key] = append(s.lists[key], values...)
	return len(s.lists[key]), nil
}

// LRange returns elements of the list at key between start and stop (inclusive),
// supporting negative indices as offsets from the end.
func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lst := s.lists[key]
	n := len(lst)
	if n == 0 {
		return []string{}
	}
	start = normalizeIndex(start, n)
	stop = normalizeIndex(stop, n)
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []string{}
	}
	out := make([]string, stop-start+1)
	copy(out, lst[start:stop+1])
	return out
}

func normalizeIndex(idx, n int) int {
	if idx < 0 {
		return n + idx
	}
	return idx
}

// HSet sets field/value pairs on the hash at key and returns the count of new fields added.
func (s *Store) HSet(key string, pairs ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkMemoryLocked(len(key) + sumLen(pairs)); err != nil {
		return 0, err
	}
	h, ok := s.hashes[key]
	if !ok {
		h = make(map[string]string)
		s.hashes[key] = h
	}
	added := 0
	for i := 0; i+1 < len(pairs); i += 2 {
		field, val := pairs[i], pairs[i+1]
		if _, exists := h[field]; !exists {
			added++
		}
		h[field] = val
	}
	return added, nil
}

// HGet returns the value of a hash field.
func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.hashes[key]
	if !ok {
		return "", false
	}
	v, ok := h[field]
	return v, ok
}

// HDel removes fields from the hash at key and returns the count actually removed.
func (s *Store) HDel(key string, fields ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hashes[key]
	if !ok {
		return 0
	}
	count := 0
	for _, f := range fields {
		if _, exists := h[f]; exists {
			delete(h, f)
			count++
		}
	}
	if len(h) == 0 {
		delete(s.hashes, key)
	}
	return count
}

// KeyCount returns the total number of keys across all types.
func (s *Store) KeyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data) + len(s.lists) + len(s.hashes)
}

// ExpiryCount returns the number of string keys with a non-zero expiry.
func (s *Store) ExpiryCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.expiryKeys)
}

// ApproxMemoryBytes returns a rough estimate of memory used by stored data:
// the sum of key and value byte lengths across all three maps. It does not
// account for Go's own map/slice overhead, so it undercounts actual RSS —
// it's meant as a relative signal for the maxmemory ceiling, not a precise figure.
func (s *Store) ApproxMemoryBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.approxMemoryBytesLocked()
}

func (s *Store) approxMemoryBytesLocked() int64 {
	var total int64
	for k, e := range s.data {
		total += int64(len(k) + len(e.Value))
	}
	for k, lst := range s.lists {
		total += int64(len(k))
		for _, v := range lst {
			total += int64(len(v))
		}
	}
	for k, h := range s.hashes {
		total += int64(len(k))
		for f, v := range h {
			total += int64(len(f) + len(v))
		}
	}
	return total
}

// checkMemoryLocked rejects a write that would push estimated usage past the
// configured ceiling. Caller must hold s.mu (write lock). It's a coarse,
// approximate check (see ApproxMemoryBytes) — deliberately so, per the
// project's "rough estimate is fine" scope.
func (s *Store) checkMemoryLocked(additionalBytes int) error {
	if s.maxMemoryBytes <= 0 {
		return nil
	}
	if s.approxMemoryBytesLocked()+int64(additionalBytes) > s.maxMemoryBytes {
		return ErrOOM
	}
	return nil
}

func sumLen(vals []string) int {
	total := 0
	for _, v := range vals {
		total += len(v)
	}
	return total
}

// Snapshot takes a consistent, deep-copied view of all data for AOF rewrite.
// The store lock is held only for the duration of the copy (fast, in-memory);
// the caller then serializes the snapshot to disk without holding this lock,
// so a rewrite never blocks live traffic for the length of a disk write.
func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	strs := make(map[string]Entry, len(s.data))
	for k, v := range s.data {
		strs[k] = v
	}
	lists := make(map[string][]string, len(s.lists))
	for k, v := range s.lists {
		cp := make([]string, len(v))
		copy(cp, v)
		lists[k] = cp
	}
	hashes := make(map[string]map[string]string, len(s.hashes))
	for k, v := range s.hashes {
		cp := make(map[string]string, len(v))
		for f, val := range v {
			cp[f] = val
		}
		hashes[k] = cp
	}
	return Snapshot{Strings: strs, Lists: lists, Hashes: hashes}
}

// StartExpiry runs the active-expiry sampler in the background until ctx is canceled.
func (s *Store) StartExpiry(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleExpire()
		}
	}
}

// sampleExpire implements the same probabilistic active-expiry algorithm real
// Redis uses, rather than a full keyspace scan. Scanning every key on every
// 100ms tick (the original, naive approach) is O(keyspace size) per tick —
// fine at a few thousand keys, a real bottleneck at millions, since it holds
// the write lock for the whole scan every single tick regardless of how many
// keys actually have a TTL.
//
// Instead: sample a small, bounded number of keys *from the set of keys that
// actually have a TTL* (s.expiryKeys, maintained incrementally by Set/Expire/
// Del/lazy-expiry), delete any that are due. If a large fraction of the
// sample was expired, keep going immediately — there's likely more expired
// work waiting — otherwise stop until the next tick. This keeps per-tick cost
// bounded by sample size, not keyspace size, while still aggressively
// reclaiming memory when there's a lot to reclaim.
func (s *Store) sampleExpire() {
	const sampleSize = 20
	const expiredRatioToRepeat = 0.25
	const maxRoundsPerTick = 10 // defensive cap; loop is already guaranteed to terminate as expiryKeys shrinks

	for round := 0; round < maxRoundsPerTick; round++ {
		s.mu.Lock()
		if len(s.expiryKeys) == 0 {
			s.mu.Unlock()
			return
		}

		sampled := 0
		expired := 0
		now := time.Now()
		// Go's map iteration order is randomized per-run, which is exactly
		// the "random sample" this algorithm wants — no extra shuffling needed.
		for k := range s.expiryKeys {
			if sampled >= sampleSize {
				break
			}
			sampled++
			if e, ok := s.data[k]; ok && !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt) {
				s.deleteExpiredLocked(k)
				expired++
			} else if !ok {
				// Tracking set drifted from data (shouldn't normally happen); reconcile it.
				delete(s.expiryKeys, k)
			}
		}
		s.mu.Unlock()

		if sampled == 0 || float64(expired)/float64(sampled) < expiredRatioToRepeat {
			return
		}
		// Otherwise loop again immediately — a high hit rate suggests more expired keys remain.
	}
}
