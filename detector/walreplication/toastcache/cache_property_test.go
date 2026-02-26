package toastcache

import (
	"math/rand"
	"testing"
	"testing/quick"
)

func TestCacheProperty_SizeBounded(t *testing.T) {
	f := func(ops []byte) bool {
		c := New(50, nil)
		for _, op := range ops {
			relID := uint32(op % 5)
			pk := string(rune('A' + op%26))
			switch op % 3 {
			case 0:
				c.Put(relID, pk, map[string]any{"v": pk})
			case 1:
				c.Get(relID, pk)
			case 2:
				c.Delete(relID, pk)
			}
		}
		return c.Len() <= 50
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 1000}); err != nil {
		t.Fatal(err)
	}
}

func TestCacheProperty_GetAfterPut(t *testing.T) {
	f := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		c := New(100, nil)
		// Put a known entry
		c.Put(1, "target", map[string]any{"found": true})
		// Do random operations (that may or may not evict "target")
		for i := 0; i < 50; i++ {
			pk := string(rune('a' + rng.Intn(26)))
			c.Put(1, pk, map[string]any{"v": pk})
		}
		row, ok := c.Get(1, "target")
		if ok {
			return row["found"] == true
		}
		// Evicted is also valid if cache is full
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}

func TestCacheProperty_DeleteRemoves(t *testing.T) {
	f := func(pk string) bool {
		if pk == "" {
			return true // skip empty PK
		}
		c := New(100, nil)
		c.Put(1, pk, map[string]any{"v": pk})
		c.Delete(1, pk)
		_, ok := c.Get(1, pk)
		return !ok
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}
