package transform

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/florinutz/pgcdc/event"
)

func FuzzDropColumns(f *testing.F) {
	f.Add([]byte(`{"name":"Alice","ssn":"123"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`"hello"`))
	f.Add([]byte(`null`))
	f.Add([]byte{})

	fn := DropColumns("ssn", "name")

	f.Fuzz(func(t *testing.T, payload []byte) {
		ev := event.Event{
			ID:      "fuzz-1",
			Payload: json.RawMessage(payload),
		}
		_, err := fn(ev)
		if err != nil && !errors.Is(err, ErrDropEvent) {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func FuzzRenameFields(f *testing.F) {
	f.Add([]byte(`{"old_name":"value"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))

	fn := RenameFields(map[string]string{"old_name": "new_name"})

	f.Fuzz(func(t *testing.T, payload []byte) {
		ev := event.Event{
			ID:      "fuzz-1",
			Payload: json.RawMessage(payload),
		}
		_, err := fn(ev)
		if err != nil && !errors.Is(err, ErrDropEvent) {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func FuzzMask(f *testing.F) {
	f.Add([]byte(`{"email":"test@test.com"}`))
	f.Add([]byte(`{"email":123}`))
	f.Add([]byte(`{}`))

	fn := Mask(MaskField{Field: "email", Mode: MaskHash})

	f.Fuzz(func(t *testing.T, payload []byte) {
		ev := event.Event{
			ID:      "fuzz-1",
			Payload: json.RawMessage(payload),
		}
		_, err := fn(ev)
		if err != nil && !errors.Is(err, ErrDropEvent) {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func FuzzChain(f *testing.F) {
	f.Add([]byte(`{"name":"Alice","ssn":"123","email":"a@b.com"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`"string"`))

	chain := Chain(
		DropColumns("ssn"),
		RenameFields(map[string]string{"name": "full_name"}),
		Mask(MaskField{Field: "email", Mode: MaskRedact}),
	)

	f.Fuzz(func(t *testing.T, payload []byte) {
		ev := event.Event{
			ID:        "fuzz-1",
			Operation: "INSERT",
			Payload:   json.RawMessage(payload),
		}
		_, err := chain(ev)
		if err != nil && !errors.Is(err, ErrDropEvent) {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
