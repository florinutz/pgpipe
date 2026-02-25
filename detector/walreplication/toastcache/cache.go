package toastcache

import (
	"log/slog"
	"strings"

	"github.com/florinutz/pgcdc/metrics"
	"github.com/jackc/pglogrepl"
)

// key uniquely identifies a row by its relation and primary key values.
type key struct {
	RelationID uint32
	PK         string
}

type entry struct {
	k    key
	row  map[string]any
	prev *entry
	next *entry
}

// Cache is a bounded LRU cache that stores the most recent full row for each
// (RelationID, PK) pair. Used to backfill unchanged TOAST columns on UPDATE
// events when REPLICA IDENTITY is not FULL.
type Cache struct {
	maxEntries int
	items      map[key]*entry
	relIndex   map[uint32][]*entry // secondary index for EvictRelation
	head       *entry              // most recently used
	tail       *entry              // least recently used
	logger     *slog.Logger
}

// New creates a TOAST column cache with the given maximum number of entries.
func New(maxEntries int, logger *slog.Logger) *Cache {
	if logger == nil {
		logger = slog.Default()
	}
	return &Cache{
		maxEntries: maxEntries,
		items:      make(map[key]*entry, maxEntries),
		relIndex:   make(map[uint32][]*entry),
		logger:     logger.With("component", "toast_cache"),
	}
}

// BuildPK builds a cache key string from the primary key column values of a row.
// PK columns are identified by Flags==1 in the RelationMessage.
func BuildPK(rel *pglogrepl.RelationMessage, row map[string]any) string {
	var parts []string
	for _, col := range rel.Columns {
		if col.Flags == 1 { // part of replica identity / PK
			if v, ok := row[col.Name]; ok && v != nil {
				parts = append(parts, stringify(v))
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\x00")
}

func stringify(v any) string {
	switch val := v.(type) {
	case string:
		return val
	default:
		// All WAL tuple values come as strings from tupleToMap.
		return ""
	}
}

// Put stores (or updates) a full row in the cache for the given relation and PK.
func (c *Cache) Put(relationID uint32, pk string, row map[string]any) {
	k := key{RelationID: relationID, PK: pk}

	if e, ok := c.items[k]; ok {
		// Update existing entry.
		e.row = row
		c.moveToFront(e)
		return
	}

	// Evict if at capacity.
	for len(c.items) >= c.maxEntries {
		c.evictLRU()
	}

	e := &entry{k: k, row: row}
	c.items[k] = e
	c.relIndex[relationID] = append(c.relIndex[relationID], e)
	c.pushFront(e)
	metrics.ToastCacheEntries.Set(float64(len(c.items)))
}

// Get retrieves the cached row for the given relation and PK.
func (c *Cache) Get(relationID uint32, pk string) (map[string]any, bool) {
	k := key{RelationID: relationID, PK: pk}
	e, ok := c.items[k]
	if !ok {
		return nil, false
	}
	c.moveToFront(e)
	return e.row, true
}

// Delete removes the cached row for the given relation and PK.
func (c *Cache) Delete(relationID uint32, pk string) {
	k := key{RelationID: relationID, PK: pk}
	e, ok := c.items[k]
	if !ok {
		return
	}
	c.removeEntry(e)
}

// EvictRelation removes all cached entries for the given relation ID.
// Used on schema change or TRUNCATE.
func (c *Cache) EvictRelation(relationID uint32) {
	entries := c.relIndex[relationID]
	for _, e := range entries {
		c.unlink(e)
		delete(c.items, e.k)
		metrics.ToastCacheEvictions.Inc()
	}
	delete(c.relIndex, relationID)
	metrics.ToastCacheEntries.Set(float64(len(c.items)))
}

// EvictAll removes all entries from the cache.
func (c *Cache) EvictAll() {
	for k := range c.items {
		metrics.ToastCacheEvictions.Inc()
		delete(c.items, k)
	}
	c.relIndex = make(map[uint32][]*entry)
	c.head = nil
	c.tail = nil
	metrics.ToastCacheEntries.Set(0)
}

// Len returns the number of entries in the cache.
func (c *Cache) Len() int {
	return len(c.items)
}

// --- internal LRU mechanics ---

func (c *Cache) pushFront(e *entry) {
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *Cache) unlink(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (c *Cache) moveToFront(e *entry) {
	if c.head == e {
		return
	}
	c.unlink(e)
	c.pushFront(e)
}

func (c *Cache) evictLRU() {
	if c.tail == nil {
		return
	}
	e := c.tail
	c.removeEntry(e)
	metrics.ToastCacheEvictions.Inc()
}

func (c *Cache) removeEntry(e *entry) {
	c.unlink(e)
	delete(c.items, e.k)

	// Remove from relIndex.
	relEntries := c.relIndex[e.k.RelationID]
	for i, re := range relEntries {
		if re == e {
			c.relIndex[e.k.RelationID] = append(relEntries[:i], relEntries[i+1:]...)
			break
		}
	}
	if len(c.relIndex[e.k.RelationID]) == 0 {
		delete(c.relIndex, e.k.RelationID)
	}
	metrics.ToastCacheEntries.Set(float64(len(c.items)))
}
