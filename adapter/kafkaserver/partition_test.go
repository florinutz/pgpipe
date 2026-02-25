package kafkaserver

import (
	"testing"
	"time"
)

func TestPartitionAppendAndRead(t *testing.T) {
	p := newPartition(100)

	// Append 5 records.
	for i := 0; i < 5; i++ {
		rec := record{
			Key:       []byte("key"),
			Value:     []byte(`{"i":` + string(rune('0'+i)) + `}`),
			Timestamp: int64(i * 1000),
		}
		offset := p.append(rec)
		if offset != int64(i) {
			t.Errorf("append %d: got offset %d, want %d", i, offset, i)
		}
	}

	// Verify sizes.
	if got := p.currentSize(); got != 5 {
		t.Errorf("currentSize: got %d, want 5", got)
	}
	if got := p.oldestOffset(); got != 0 {
		t.Errorf("oldestOffset: got %d, want 0", got)
	}
	if got := p.latestOffset(); got != 5 {
		t.Errorf("latestOffset: got %d, want 5", got)
	}

	// Read from offset 0.
	records, errCode := p.readFrom(0, 1024*1024)
	if errCode != errNone {
		t.Fatalf("readFrom(0): error code %d", errCode)
	}
	if len(records) != 5 {
		t.Errorf("readFrom(0): got %d records, want 5", len(records))
	}
	for i, rec := range records {
		if rec.Offset != int64(i) {
			t.Errorf("record[%d].Offset: got %d, want %d", i, rec.Offset, i)
		}
	}

	// Read from offset 3.
	records, errCode = p.readFrom(3, 1024*1024)
	if errCode != errNone {
		t.Fatalf("readFrom(3): error code %d", errCode)
	}
	if len(records) != 2 {
		t.Errorf("readFrom(3): got %d records, want 2", len(records))
	}
}

func TestPartitionRingBufferOverwrite(t *testing.T) {
	p := newPartition(5) // small buffer

	// Append 10 records — first 5 will be overwritten.
	for i := 0; i < 10; i++ {
		p.append(record{
			Key:   []byte("key"),
			Value: []byte(`{}`),
		})
	}

	if got := p.currentSize(); got != 5 {
		t.Errorf("currentSize: got %d, want 5", got)
	}
	if got := p.oldestOffset(); got != 5 {
		t.Errorf("oldestOffset: got %d, want 5", got)
	}
	if got := p.latestOffset(); got != 10 {
		t.Errorf("latestOffset: got %d, want 10", got)
	}

	// Reading from offset 0 should fail with OFFSET_OUT_OF_RANGE.
	_, errCode := p.readFrom(0, 1024*1024)
	if errCode != errOffsetOutOfRange {
		t.Errorf("readFrom(0): got error code %d, want %d (OFFSET_OUT_OF_RANGE)", errCode, errOffsetOutOfRange)
	}

	// Reading from offset 5 should work.
	records, errCode := p.readFrom(5, 1024*1024)
	if errCode != errNone {
		t.Fatalf("readFrom(5): error code %d", errCode)
	}
	if len(records) != 5 {
		t.Errorf("readFrom(5): got %d records, want 5", len(records))
	}
	if records[0].Offset != 5 {
		t.Errorf("first record offset: got %d, want 5", records[0].Offset)
	}
}

func TestPartitionReadBeyondLatest(t *testing.T) {
	p := newPartition(100)
	p.append(record{Key: []byte("k"), Value: []byte(`{}`)})

	// Reading at latestOffset should return empty (no data yet at that offset).
	records, errCode := p.readFrom(1, 1024*1024)
	if errCode != errNone {
		t.Fatalf("readFrom(latest): error code %d", errCode)
	}
	if len(records) != 0 {
		t.Errorf("readFrom(latest): got %d records, want 0", len(records))
	}
}

func TestPartitionWaiter(t *testing.T) {
	p := newPartition(100)

	ch := p.waiter()

	// Append should wake the waiter.
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.append(record{Key: []byte("k"), Value: []byte(`{}`)})
	}()

	select {
	case <-ch:
		// OK, waiter was notified.
	case <-time.After(1 * time.Second):
		t.Fatal("waiter was not notified within timeout")
	}
}

func TestPartitionEmpty(t *testing.T) {
	p := newPartition(100)

	if got := p.currentSize(); got != 0 {
		t.Errorf("currentSize: got %d, want 0", got)
	}
	if got := p.oldestOffset(); got != 0 {
		t.Errorf("oldestOffset: got %d, want 0", got)
	}
	if got := p.latestOffset(); got != 0 {
		t.Errorf("latestOffset: got %d, want 0", got)
	}

	records, errCode := p.readFrom(0, 1024*1024)
	if errCode != errNone {
		t.Fatalf("readFrom(0) on empty: error code %d", errCode)
	}
	if len(records) != 0 {
		t.Errorf("readFrom(0) on empty: got %d records, want 0", len(records))
	}
}
