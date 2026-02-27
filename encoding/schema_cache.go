package encoding

import (
	"sync"

	"github.com/hamba/avro/v2"
)

// SchemaCacheEntry holds a cached Avro schema and its optional registry ID.
type SchemaCacheEntry struct {
	Schema      string      // JSON Avro schema
	Parsed      avro.Schema // Pre-parsed Avro schema (avoids re-parsing on every encode)
	RegistryID  int         // Schema Registry ID; 0 if not registered
	ColumnsHash string      // Cached hash of columns used to generate this schema
}

// SchemaCache is a thread-safe cache for generated Avro schemas, keyed by
// "namespace.table:columnsHash".
type SchemaCache struct {
	mu      sync.RWMutex
	entries map[string]SchemaCacheEntry
}

// NewSchemaCache creates an empty schema cache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		entries: make(map[string]SchemaCacheEntry),
	}
}

// Get returns the cached entry and true, or the zero value and false.
func (c *SchemaCache) Get(key string) (SchemaCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	return entry, ok
}

// Put stores an entry in the cache.
func (c *SchemaCache) Put(key string, entry SchemaCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = entry
}
