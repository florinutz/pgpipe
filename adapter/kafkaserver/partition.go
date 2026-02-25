package kafkaserver

import (
	"sync"
)

// partition is a fixed-size ring buffer that stores records with monotonically
// increasing offsets. Consumers that fall behind the oldest offset get
// OFFSET_OUT_OF_RANGE (standard Kafka behavior).
type partition struct {
	mu         sync.Mutex
	ring       []record
	capacity   int
	nextOffset int64 // next offset to assign (monotonically increasing)
	size       int   // current number of valid records

	// Fetch waiters: goroutines blocked waiting for new data.
	waiters []chan struct{}
}

func newPartition(capacity int) *partition {
	if capacity <= 0 {
		capacity = 10000
	}
	return &partition{
		ring:     make([]record, capacity),
		capacity: capacity,
	}
}

// append adds a record to the ring buffer, assigning the next offset.
// Returns the assigned offset.
func (p *partition) append(rec record) int64 {
	p.mu.Lock()

	offset := p.nextOffset
	rec.Offset = offset

	idx := int(offset) % p.capacity
	p.ring[idx] = rec
	p.nextOffset++
	if p.size < p.capacity {
		p.size++
	}

	// Wake all Fetch waiters.
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()

	for _, ch := range waiters {
		close(ch)
	}

	return offset
}

// oldestOffset returns the oldest available offset, or 0 if empty.
func (p *partition) oldestOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.size == 0 {
		return 0
	}
	return p.nextOffset - int64(p.size)
}

// latestOffset returns the next offset to be assigned (one past the last).
func (p *partition) latestOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextOffset
}

// currentSize returns the number of records currently in the buffer.
func (p *partition) currentSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

// readFrom reads records starting at the given offset, up to maxBytes of
// encoded value data. Returns the records and an error code.
// If offset < oldestOffset: returns errOffsetOutOfRange.
// If offset >= nextOffset: returns empty slice (caller should wait).
func (p *partition) readFrom(offset int64, maxBytes int32) ([]record, int16) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.size == 0 && offset == 0 {
		return nil, errNone
	}

	oldest := p.nextOffset - int64(p.size)
	if offset < oldest {
		return nil, errOffsetOutOfRange
	}
	if offset >= p.nextOffset {
		return nil, errNone
	}

	var result []record
	var totalBytes int32
	for off := offset; off < p.nextOffset; off++ {
		idx := int(off) % p.capacity
		rec := p.ring[idx]
		recSize := int32(len(rec.Key) + len(rec.Value) + 64) // estimate
		if totalBytes+recSize > maxBytes && len(result) > 0 {
			break
		}
		result = append(result, rec)
		totalBytes += recSize
	}
	return result, errNone
}

// waiter returns a channel that is closed when new data is appended.
// The caller should call this under no lock; the partition will add
// the waiter atomically.
func (p *partition) waiter() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := make(chan struct{})
	p.waiters = append(p.waiters, ch)
	return ch
}
