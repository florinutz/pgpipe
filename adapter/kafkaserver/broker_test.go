package kafkaserver

import (
	"encoding/json"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func TestChannelToTopic(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"pgcdc:orders", "pgcdc.orders"},
		{"", ""},
		{"a:b:c", "a.b.c"},
		{"notopic", "notopic"},
		{"pgcdc:my:nested:channel", "pgcdc.my.nested.channel"},
	}
	for _, tt := range tests {
		got := channelToTopic(tt.input)
		if got != tt.want {
			t.Errorf("channelToTopic(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestBrokerIngest(t *testing.T) {
	brk := newBroker(4, 100, "id")

	ev := event.Event{
		ID:        "evt-1",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id":"key-1","name":"test"}`),
		CreatedAt: time.Now(),
		LSN:       42,
	}

	brk.ingest(ev)

	topicName := channelToTopic(ev.Channel)
	tp := brk.getTopic(topicName)
	if tp == nil {
		t.Fatal("topic not created after ingest")
	}

	// Find which partition got the record.
	key := extractKey(ev.Payload, "id", ev.ID)
	partIdx := partitionForKey(key, len(tp.partitions))

	p := tp.partitions[partIdx]
	if p.currentSize() != 1 {
		t.Fatalf("partition %d size: got %d, want 1", partIdx, p.currentSize())
	}

	records, errCode := p.readFrom(0, 1024*1024)
	if errCode != errNone {
		t.Fatalf("readFrom: error code %d", errCode)
	}
	if len(records) != 1 {
		t.Fatalf("records: got %d, want 1", len(records))
	}
	if string(records[0].Key) != "key-1" {
		t.Errorf("record key: got %q, want %q", string(records[0].Key), "key-1")
	}
	if records[0].LSN != 42 {
		t.Errorf("record LSN: got %d, want 42", records[0].LSN)
	}
}

func TestBrokerGetOrCreateTopic_Concurrent(t *testing.T) {
	brk := newBroker(4, 100, "id")

	var wg sync.WaitGroup
	results := make([]*topic, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = brk.getOrCreateTopic("concurrent.topic")
		}(i)
	}
	wg.Wait()

	// All results should point to the same topic.
	for i := 1; i < 100; i++ {
		if results[i] != results[0] {
			t.Fatalf("goroutine %d got a different topic pointer", i)
		}
	}

	// Only one topic should exist.
	names := brk.topicNames()
	if len(names) != 1 {
		t.Errorf("topic count: got %d, want 1", len(names))
	}
}

func TestBrokerTopicNames(t *testing.T) {
	brk := newBroker(4, 100, "id")
	brk.getOrCreateTopic("alpha")
	brk.getOrCreateTopic("beta")
	brk.getOrCreateTopic("gamma")

	names := brk.topicNames()
	sort.Strings(names)

	want := []string{"alpha", "beta", "gamma"}
	if len(names) != len(want) {
		t.Fatalf("topic names: got %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("topic[%d]: got %q, want %q", i, names[i], want[i])
		}
	}
}
