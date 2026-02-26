package transform

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/florinutz/pgcdc/event"
)

func makePayload(size int) json.RawMessage {
	padSize := size - 80
	if padSize < 0 {
		padSize = 0
	}
	m := map[string]any{
		"id":    "12345",
		"name":  "Alice",
		"email": "alice@example.com",
		"ssn":   "123-45-6789",
		"data":  strings.Repeat("x", padSize),
	}
	raw, _ := json.Marshal(m)
	return raw
}

func benchEvent(payload json.RawMessage) event.Event {
	return event.Event{
		ID:        "bench-id",
		Channel:   "bench",
		Operation: "INSERT",
		Payload:   payload,
	}
}

func BenchmarkDropColumns_100B(b *testing.B) {
	fn := DropColumns("ssn")
	ev := benchEvent(makePayload(100))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkDropColumns_1KB(b *testing.B) {
	fn := DropColumns("ssn")
	ev := benchEvent(makePayload(1024))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkDropColumns_10KB(b *testing.B) {
	fn := DropColumns("ssn")
	ev := benchEvent(makePayload(10240))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkRenameFields_100B(b *testing.B) {
	fn := RenameFields(map[string]string{"name": "full_name"})
	ev := benchEvent(makePayload(100))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkMask_Hash_100B(b *testing.B) {
	fn := Mask(MaskField{Field: "email", Mode: MaskHash})
	ev := benchEvent(makePayload(100))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkChain_Depth1(b *testing.B) {
	fn := Chain(DropColumns("ssn"))
	ev := benchEvent(makePayload(100))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkChain_Depth3(b *testing.B) {
	fn := Chain(
		DropColumns("ssn"),
		RenameFields(map[string]string{"name": "full_name"}),
		Mask(MaskField{Field: "email", Mode: MaskRedact}),
	)
	ev := benchEvent(makePayload(100))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkChain_Depth5(b *testing.B) {
	fn := Chain(
		DropColumns("ssn"),
		RenameFields(map[string]string{"name": "full_name"}),
		Mask(MaskField{Field: "email", Mode: MaskRedact}),
		FilterOperation("INSERT", "UPDATE"),
		DropColumns("data"),
	)
	ev := benchEvent(makePayload(100))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = fn(ev)
	}
}

func BenchmarkWithPayload_Unmarshal(b *testing.B) {
	ev := benchEvent(makePayload(1024))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = withPayload(ev, func(m map[string]any) {})
	}
}
