package qcow2

import (
	"sync"
)

// l2Cache is a simple LRU cache for L2 tables.
// Uses []byte slices directly to avoid struct allocation overhead.
type l2Cache struct {
	mu       sync.RWMutex
	entries  map[uint64]*cacheEntry
	head     *cacheEntry // Most recently used
	tail     *cacheEntry // Least recently used
	maxSize  int
	tableLen int
}

type cacheEntry struct {
	offset uint64
	data   []byte
	prev   *cacheEntry
	next   *cacheEntry
}

// newL2Cache creates a new L2 table cache.
// maxSize is the maximum number of L2 tables to cache.
// tableLen is the size of each L2 table in bytes.
func newL2Cache(maxSize, tableLen int) *l2Cache {
	return &l2Cache{
		entries:  make(map[uint64]*cacheEntry),
		maxSize:  maxSize,
		tableLen: tableLen,
	}
}

// get retrieves an L2 table from the cache.
// Returns nil if not found.
func (c *l2Cache) get(offset uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[offset]
	if !ok {
		return nil
	}

	// Move to front (most recently used)
	c.moveToFront(entry)

	// Return a copy to avoid races
	result := make([]byte, len(entry.data))
	copy(result, entry.data)
	return result
}

// put adds or updates an L2 table in the cache.
func (c *l2Cache) put(offset uint64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if entry, ok := c.entries[offset]; ok {
		// Update data
		copy(entry.data, data)
		c.moveToFront(entry)
		return
	}

	// Create new entry
	entry := &cacheEntry{
		offset: offset,
		data:   make([]byte, len(data)),
	}
	copy(entry.data, data)

	// Add to front
	c.addToFront(entry)
	c.entries[offset] = entry

	// Evict if necessary
	for len(c.entries) > c.maxSize {
		c.evictLRU()
	}
}

// invalidate removes an L2 table from the cache.
func (c *l2Cache) invalidate(offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[offset]
	if !ok {
		return
	}

	c.removeEntry(entry)
	delete(c.entries, offset)
}

// clear removes all entries from the cache.
func (c *l2Cache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[uint64]*cacheEntry)
	c.head = nil
	c.tail = nil
}

// moveToFront moves an entry to the front of the LRU list.
func (c *l2Cache) moveToFront(entry *cacheEntry) {
	if entry == c.head {
		return
	}

	c.removeEntry(entry)
	c.addToFront(entry)
}

// addToFront adds an entry to the front of the LRU list.
func (c *l2Cache) addToFront(entry *cacheEntry) {
	entry.prev = nil
	entry.next = c.head

	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

// removeEntry removes an entry from the LRU list.
func (c *l2Cache) removeEntry(entry *cacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
}

// evictLRU removes the least recently used entry.
func (c *l2Cache) evictLRU() {
	if c.tail == nil {
		return
	}

	entry := c.tail
	c.removeEntry(entry)
	delete(c.entries, entry.offset)
}

// size returns the number of entries in the cache.
func (c *l2Cache) size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
