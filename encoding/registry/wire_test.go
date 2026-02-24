package registry

import (
	"bytes"
	"testing"
)

func TestWireEncodeDecodRoundTrip(t *testing.T) {
	payload := []byte("hello avro")
	schemaID := 42

	encoded := WireEncode(schemaID, payload)

	// Header: 1 magic + 4 schema ID = 5 bytes.
	if len(encoded) != 5+len(payload) {
		t.Fatalf("len(encoded) = %d, want %d", len(encoded), 5+len(payload))
	}
	if encoded[0] != 0x00 {
		t.Errorf("magic byte = 0x%02x, want 0x00", encoded[0])
	}

	gotID, gotPayload, err := WireDecode(encoded)
	if err != nil {
		t.Fatalf("WireDecode: %v", err)
	}
	if gotID != schemaID {
		t.Errorf("schemaID = %d, want %d", gotID, schemaID)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("payload = %q, want %q", gotPayload, payload)
	}
}

func TestWireDecodeErrors(t *testing.T) {
	t.Run("too short", func(t *testing.T) {
		_, _, err := WireDecode([]byte{0x00, 0x01})
		if err == nil {
			t.Error("expected error for short data")
		}
	})

	t.Run("bad magic", func(t *testing.T) {
		_, _, err := WireDecode([]byte{0xFF, 0x00, 0x00, 0x00, 0x01})
		if err == nil {
			t.Error("expected error for bad magic byte")
		}
	})
}

func TestWireEncodeProtobuf(t *testing.T) {
	payload := []byte("proto data")
	schemaID := 7

	encoded := WireEncodeProtobuf(schemaID, payload)

	// Header: 1 magic + 4 schema ID + 1 message index = 6 bytes.
	if len(encoded) != 6+len(payload) {
		t.Fatalf("len(encoded) = %d, want %d", len(encoded), 6+len(payload))
	}
	if encoded[0] != 0x00 {
		t.Errorf("magic byte = 0x%02x, want 0x00", encoded[0])
	}
	if encoded[5] != 0x00 {
		t.Errorf("message index byte = 0x%02x, want 0x00", encoded[5])
	}

	// Payload should follow after the 6-byte header.
	if !bytes.Equal(encoded[6:], payload) {
		t.Errorf("payload mismatch")
	}
}
