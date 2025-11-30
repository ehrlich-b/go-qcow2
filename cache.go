package qcow2

import (
	"sync"
	"sync/atomic"
)

// Default number of shards for the L2 cache.
// Must be a power of 2 for efficient modulo operation.
const defaultL2CacheShards = 8

// l2Cache is a sharded LRU cache for L2 tables.
// Uses multiple independent shards to reduce lock contention under concurrent access.
// Each shard maintains its own LRU list and lock.
type l2Cache struct {
	shards    []*l2CacheShard
	shardMask uint64 // shardCount - 1 for fast modulo

	// Statistics (atomic for lock-free access)
	hits       atomic.Uint64
	misses     atomic.Uint64
	evictions  atomic.Uint64
	insertions atomic.Uint64
}

// l2CacheShard is a single shard of the L2 cache.
// Uses []byte slices directly to avoid struct allocation overhead.
type l2CacheShard struct {
	mu      sync.RWMutex
	entries map[uint64]*cacheEntry
	head    *cacheEntry // Most recently used
	tail    *cacheEntry // Least recently used
	maxSize int
}

type cacheEntry struct {
	offset uint64
	data   []byte
	prev   *cacheEntry
	next   *cacheEntry
}

// newL2Cache creates a new L2 table cache with sharding.
// maxSize is the total number of entries across all shards.
// Entries are distributed across shards based on offset hash.
func newL2Cache(maxSize, _ int) *l2Cache {
	return newL2CacheWithShards(maxSize, defaultL2CacheShards)
}

// newL2CacheWithShards creates a new L2 table cache with a specific shard count.
// shardCount must be a power of 2.
func newL2CacheWithShards(maxSize, shardCount int) *l2Cache {
	if shardCount <= 0 {
		shardCount = defaultL2CacheShards
	}
	// Ensure shardCount is a power of 2
	if shardCount&(shardCount-1) != 0 {
		// Round up to next power of 2
		v := shardCount
		v--
		v |= v >> 1
		v |= v >> 2
		v |= v >> 4
		v |= v >> 8
		v |= v >> 16
		shardCount = v + 1
	}

	// Distribute maxSize across shards (minimum 1 per shard)
	perShard := maxSize / shardCount
	if perShard < 1 {
		perShard = 1
	}

	shards := make([]*l2CacheShard, shardCount)
	for i := range shards {
		shards[i] = &l2CacheShard{
			entries: make(map[uint64]*cacheEntry),
			maxSize: perShard,
		}
	}

	return &l2Cache{
		shards:    shards,
		shardMask: uint64(shardCount - 1),
	}
}

// getShard returns the shard for a given offset.
// Uses a simple hash to distribute offsets across shards.
func (c *l2Cache) getShard(offset uint64) *l2CacheShard {
	// Mix bits to improve distribution (offsets are often cluster-aligned)
	h := offset ^ (offset >> 16) ^ (offset >> 32)
	return c.shards[h&c.shardMask]
}

// get retrieves an L2 table from the cache.
// Returns nil if not found.
//
// IMPORTANT: The returned slice is a direct reference to cached data.
// Callers may read from it freely. Callers that modify the slice MUST
// call put() afterwards to ensure cache consistency. Concurrent access
// to different 8-byte entries within the same L2 table is safe.
func (c *l2Cache) get(offset uint64) []byte {
	data := c.getShard(offset).get(offset)
	if data != nil {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return data
}

// put adds or updates an L2 table in the cache.
func (c *l2Cache) put(offset uint64, data []byte) {
	inserted, evicted := c.getShard(offset).put(offset, data)
	if inserted {
		c.insertions.Add(1)
	}
	if evicted > 0 {
		c.evictions.Add(uint64(evicted))
	}
}

// invalidate removes an L2 table from the cache.
func (c *l2Cache) invalidate(offset uint64) {
	c.getShard(offset).invalidate(offset)
}

// clear removes all entries from the cache.
func (c *l2Cache) clear() {
	for _, shard := range c.shards {
		shard.clear()
	}
}

// get retrieves an L2 table from the shard.
func (s *l2CacheShard) get(offset uint64) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[offset]
	if !ok {
		return nil
	}

	// Move to front (most recently used)
	s.moveToFront(entry)

	// Return direct reference - no copy needed.
	// Callers reading don't modify, callers writing call put() after.
	return entry.data
}

// put adds or updates an L2 table in the shard.
// Returns (inserted, evictionCount) where inserted is true if a new entry was added.
func (s *l2CacheShard) put(offset uint64, data []byte) (bool, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists
	if entry, ok := s.entries[offset]; ok {
		// Update data
		copy(entry.data, data)
		s.moveToFront(entry)
		return false, 0
	}

	// Create new entry
	entry := &cacheEntry{
		offset: offset,
		data:   make([]byte, len(data)),
	}
	copy(entry.data, data)

	// Add to front
	s.addToFront(entry)
	s.entries[offset] = entry

	// Evict if necessary
	evicted := 0
	for len(s.entries) > s.maxSize {
		s.evictLRU()
		evicted++
	}

	return true, evicted
}

// invalidate removes an L2 table from the shard.
func (s *l2CacheShard) invalidate(offset uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[offset]
	if !ok {
		return
	}

	s.removeEntry(entry)
	delete(s.entries, offset)
}

// clear removes all entries from the shard.
func (s *l2CacheShard) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = make(map[uint64]*cacheEntry)
	s.head = nil
	s.tail = nil
}

// size returns the total number of entries across all shards.
func (c *l2Cache) size() int {
	total := 0
	for _, shard := range c.shards {
		total += shard.size()
	}
	return total
}

// CacheStats contains statistics about cache performance.
type CacheStats struct {
	// Hits is the number of successful cache lookups.
	Hits uint64
	// Misses is the number of failed cache lookups.
	Misses uint64
	// HitRate is the percentage of lookups that were hits (0.0-1.0).
	HitRate float64
	// Insertions is the number of new entries added to the cache.
	Insertions uint64
	// Evictions is the number of entries evicted to make room.
	Evictions uint64
	// Size is the current number of entries in the cache.
	Size int
	// MaxSize is the maximum number of entries the cache can hold.
	MaxSize int
}

// stats returns cache statistics.
func (c *l2Cache) stats() CacheStats {
	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	maxSize := 0
	for _, shard := range c.shards {
		maxSize += shard.maxSize
	}

	return CacheStats{
		Hits:       hits,
		Misses:     misses,
		HitRate:    hitRate,
		Insertions: c.insertions.Load(),
		Evictions:  c.evictions.Load(),
		Size:       c.size(),
		MaxSize:    maxSize,
	}
}

// resetStats resets all statistics counters to zero.
func (c *l2Cache) resetStats() {
	c.hits.Store(0)
	c.misses.Store(0)
	c.insertions.Store(0)
	c.evictions.Store(0)
}

// size returns the number of entries in the shard.
func (s *l2CacheShard) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// moveToFront moves an entry to the front of the LRU list.
func (s *l2CacheShard) moveToFront(entry *cacheEntry) {
	if entry == s.head {
		return
	}

	s.removeEntry(entry)
	s.addToFront(entry)
}

// addToFront adds an entry to the front of the LRU list.
func (s *l2CacheShard) addToFront(entry *cacheEntry) {
	entry.prev = nil
	entry.next = s.head

	if s.head != nil {
		s.head.prev = entry
	}
	s.head = entry

	if s.tail == nil {
		s.tail = entry
	}
}

// removeEntry removes an entry from the LRU list.
func (s *l2CacheShard) removeEntry(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		s.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		s.tail = entry.prev
	}
}

// evictLRU removes the least recently used entry.
func (s *l2CacheShard) evictLRU() {
	if s.tail == nil {
		return
	}

	entry := s.tail
	s.removeEntry(entry)
	delete(s.entries, entry.offset)
}
