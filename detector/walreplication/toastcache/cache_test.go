package toastcache

import (
	"testing"
)

func TestPutGet(t *testing.T) {
	c := New(10, nil)

	c.Put(1, "42", map[string]any{"id": "42", "name": "Alice"})
	row, ok := c.Get(1, "42")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if row["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", row["name"])
	}
	if c.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", c.Len())
	}
}

func TestGetMiss(t *testing.T) {
	c := New(10, nil)

	_, ok := c.Get(1, "99")
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestPutUpdate(t *testing.T) {
	c := New(10, nil)

	c.Put(1, "1", map[string]any{"id": "1", "v": "old"})
	c.Put(1, "1", map[string]any{"id": "1", "v": "new"})

	row, ok := c.Get(1, "1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if row["v"] != "new" {
		t.Fatalf("expected new, got %v", row["v"])
	}
	if c.Len() != 1 {
		t.Fatalf("expected 1 entry after update, got %d", c.Len())
	}
}

func TestLRUEviction(t *testing.T) {
	c := New(3, nil)

	c.Put(1, "a", map[string]any{"id": "a"})
	c.Put(1, "b", map[string]any{"id": "b"})
	c.Put(1, "c", map[string]any{"id": "c"})
	// Cache full: [c, b, a]

	// Adding a 4th should evict "a" (LRU).
	c.Put(1, "d", map[string]any{"id": "d"})

	if c.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", c.Len())
	}
	if _, ok := c.Get(1, "a"); ok {
		t.Fatal("expected 'a' to be evicted")
	}
	if _, ok := c.Get(1, "b"); !ok {
		t.Fatal("expected 'b' to still be present")
	}
}

func TestLRUAccessOrder(t *testing.T) {
	c := New(3, nil)

	c.Put(1, "a", map[string]any{"id": "a"})
	c.Put(1, "b", map[string]any{"id": "b"})
	c.Put(1, "c", map[string]any{"id": "c"})
	// Cache: [c, b, a]

	// Access "a" to move it to front.
	c.Get(1, "a")
	// Cache: [a, c, b]

	// Adding "d" should evict "b" (now LRU).
	c.Put(1, "d", map[string]any{"id": "d"})

	if _, ok := c.Get(1, "b"); ok {
		t.Fatal("expected 'b' to be evicted")
	}
	if _, ok := c.Get(1, "a"); !ok {
		t.Fatal("expected 'a' to still be present")
	}
}

func TestDelete(t *testing.T) {
	c := New(10, nil)

	c.Put(1, "1", map[string]any{"id": "1"})
	c.Delete(1, "1")

	if _, ok := c.Get(1, "1"); ok {
		t.Fatal("expected cache miss after delete")
	}
	if c.Len() != 0 {
		t.Fatalf("expected 0 entries, got %d", c.Len())
	}
}

func TestDeleteNonExistent(t *testing.T) {
	c := New(10, nil)
	c.Delete(1, "nope") // should not panic
}

func TestEvictRelation(t *testing.T) {
	c := New(10, nil)

	c.Put(1, "a", map[string]any{"id": "a"})
	c.Put(1, "b", map[string]any{"id": "b"})
	c.Put(2, "c", map[string]any{"id": "c"})

	c.EvictRelation(1)

	if c.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", c.Len())
	}
	if _, ok := c.Get(1, "a"); ok {
		t.Fatal("expected relation 1 entries to be evicted")
	}
	if _, ok := c.Get(2, "c"); !ok {
		t.Fatal("expected relation 2 entry to remain")
	}
}

func TestEvictRelationEmpty(t *testing.T) {
	c := New(10, nil)
	c.EvictRelation(999) // should not panic
}

func TestEvictAll(t *testing.T) {
	c := New(10, nil)

	c.Put(1, "a", map[string]any{"id": "a"})
	c.Put(2, "b", map[string]any{"id": "b"})

	c.EvictAll()

	if c.Len() != 0 {
		t.Fatalf("expected 0 entries, got %d", c.Len())
	}
}

func TestDifferentRelationsSamePK(t *testing.T) {
	c := New(10, nil)

	c.Put(1, "42", map[string]any{"table": "orders"})
	c.Put(2, "42", map[string]any{"table": "users"})

	row1, ok := c.Get(1, "42")
	if !ok || row1["table"] != "orders" {
		t.Fatalf("expected orders, got %v", row1)
	}
	row2, ok := c.Get(2, "42")
	if !ok || row2["table"] != "users" {
		t.Fatalf("expected users, got %v", row2)
	}
}
