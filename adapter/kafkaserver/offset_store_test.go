package kafkaserver

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

type mockCheckpointStore struct {
	mu   sync.Mutex
	data map[string]uint64
}

func newMockCheckpointStore() *mockCheckpointStore {
	return &mockCheckpointStore{data: make(map[string]uint64)}
}

func (m *mockCheckpointStore) Load(_ context.Context, key string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key], nil
}

func (m *mockCheckpointStore) Save(_ context.Context, key string, val uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = val
	return nil
}

func (m *mockCheckpointStore) Close() error { return nil }

func TestSlotName(t *testing.T) {
	got := slotName("mygroup", "mytopic", 0)
	want := "kafka:mygroup:mytopic:0"
	if got != want {
		t.Errorf("slotName: got %q, want %q", got, want)
	}

	got = slotName("g", "t", 7)
	want = "kafka:g:t:7"
	if got != want {
		t.Errorf("slotName: got %q, want %q", got, want)
	}
}

func TestOffsetStore_CommitAndFetch(t *testing.T) {
	store := newMockCheckpointStore()
	os := newOffsetStore(store, slog.Default())

	ctx := context.Background()

	if err := os.commitOffset(ctx, "group1", "topic1", 0, 42); err != nil {
		t.Fatalf("commitOffset: %v", err)
	}

	offset, err := os.fetchOffset(ctx, "group1", "topic1", 0)
	if err != nil {
		t.Fatalf("fetchOffset: %v", err)
	}
	if offset != 42 {
		t.Errorf("offset: got %d, want 42", offset)
	}
}

func TestOffsetStore_FetchNoCommit(t *testing.T) {
	store := newMockCheckpointStore()
	os := newOffsetStore(store, slog.Default())

	ctx := context.Background()

	offset, err := os.fetchOffset(ctx, "group1", "topic1", 0)
	if err != nil {
		t.Fatalf("fetchOffset: %v", err)
	}
	if offset != -1 {
		t.Errorf("offset: got %d, want -1 (no commit)", offset)
	}
}
