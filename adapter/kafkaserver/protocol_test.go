package kafkaserver

import (
	"bytes"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	tests := []int64{0, 1, -1, 127, -128, 300, -300, 1<<31 - 1, -(1 << 31), 1<<62 - 1}
	for _, v := range tests {
		var buf [16]byte
		n := putVarint(buf[:], v)
		got, consumed, err := readVarint(buf[:n], 0)
		if err != nil {
			t.Fatalf("readVarint(%d): %v", v, err)
		}
		if got != v {
			t.Errorf("readVarint: got %d, want %d", got, v)
		}
		if consumed != n {
			t.Errorf("readVarint consumed %d bytes, putVarint wrote %d", consumed, n)
		}
	}
}

func TestVarintLen(t *testing.T) {
	tests := []int64{0, 1, -1, 127, -128, 300, -300, 1<<31 - 1}
	for _, v := range tests {
		var buf [16]byte
		n := putVarint(buf[:], v)
		got := varintLen(v)
		if got != n {
			t.Errorf("varintLen(%d) = %d, putVarint wrote %d", v, got, n)
		}
	}
}

func TestAppendVarint(t *testing.T) {
	var dst []byte
	dst = appendVarint(dst, 42)
	dst = appendVarint(dst, -1)

	// Read back.
	v1, n1, err := readVarint(dst, 0)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 42 {
		t.Errorf("first varint: got %d, want 42", v1)
	}

	v2, _, err := readVarint(dst, n1)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != -1 {
		t.Errorf("second varint: got %d, want -1", v2)
	}
}

func TestEncoderDecoder(t *testing.T) {
	enc := newEncoder(64)
	enc.putInt8(42)
	enc.putInt16(1000)
	enc.putInt32(123456)
	enc.putInt64(9876543210)
	enc.putString("hello")
	enc.putBytes([]byte{1, 2, 3})
	enc.putBytes(nil) // null bytes
	enc.putNullableString("")
	enc.putNullableString("world")

	d := newDecoder(enc.bytes())
	if v := d.readInt8(); v != 42 {
		t.Errorf("int8: got %d, want 42", v)
	}
	if v := d.readInt16(); v != 1000 {
		t.Errorf("int16: got %d, want 1000", v)
	}
	if v := d.readInt32(); v != 123456 {
		t.Errorf("int32: got %d, want 123456", v)
	}
	if v := d.readInt64(); v != 9876543210 {
		t.Errorf("int64: got %d, want 9876543210", v)
	}
	if v := d.readString(); v != "hello" {
		t.Errorf("string: got %q, want %q", v, "hello")
	}
	if v := d.readBytes(); !bytes.Equal(v, []byte{1, 2, 3}) {
		t.Errorf("bytes: got %v, want [1 2 3]", v)
	}
	if v := d.readBytes(); v != nil {
		t.Errorf("null bytes: got %v, want nil", v)
	}
	// Nullable empty string = null (int16(-1)).
	if v := d.readInt16(); v != -1 {
		t.Errorf("nullable empty: got %d, want -1", v)
	}
	if v := d.readString(); v != "world" {
		t.Errorf("nullable string: got %q, want %q", v, "world")
	}
	if d.err != nil {
		t.Fatalf("decoder error: %v", d.err)
	}
}

func TestRecordBatchRoundTrip(t *testing.T) {
	records := []record{
		{
			Offset:    0,
			Key:       []byte("key1"),
			Value:     []byte(`{"id":1}`),
			Timestamp: 1000000,
			Headers:   []recordHeader{{Key: "h1", Value: []byte("v1")}},
		},
		{
			Offset:    1,
			Key:       []byte("key2"),
			Value:     []byte(`{"id":2}`),
			Timestamp: 1000001,
			Headers:   nil,
		},
	}

	batch := encodeRecordBatch(records)
	if len(batch) == 0 {
		t.Fatal("encodeRecordBatch returned empty result")
	}

	// Verify magic byte is 2.
	// Batch layout: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1)
	if len(batch) < 17 {
		t.Fatalf("batch too short: %d bytes", len(batch))
	}
	magic := batch[16]
	if magic != 2 {
		t.Errorf("magic byte: got %d, want 2", magic)
	}
}

func TestRecordBatchEmpty(t *testing.T) {
	batch := encodeRecordBatch(nil)
	if batch != nil {
		t.Errorf("encodeRecordBatch(nil) should return nil, got %d bytes", len(batch))
	}
}

func TestReadWriteRequest(t *testing.T) {
	// Build a minimal Kafka request frame.
	enc := newEncoder(64)
	enc.putInt16(18)             // API key: ApiVersions
	enc.putInt16(0)              // API version
	enc.putInt32(42)             // Correlation ID
	enc.putString("test-client") // Client ID
	// No body

	// Wrap in length prefix.
	var frame bytes.Buffer
	length := int32(len(enc.bytes()))
	frameEnc := newEncoder(4 + len(enc.bytes()))
	frameEnc.putInt32(length)
	frame.Write(frameEnc.bytes())
	frame.Write(enc.bytes())

	hdr, body, err := readRequest(&frame)
	if err != nil {
		t.Fatalf("readRequest: %v", err)
	}
	if hdr.APIKey != 18 {
		t.Errorf("APIKey: got %d, want 18", hdr.APIKey)
	}
	if hdr.APIVersion != 0 {
		t.Errorf("APIVersion: got %d, want 0", hdr.APIVersion)
	}
	if hdr.CorrelationID != 42 {
		t.Errorf("CorrelationID: got %d, want 42", hdr.CorrelationID)
	}
	if hdr.ClientID != "test-client" {
		t.Errorf("ClientID: got %q, want %q", hdr.ClientID, "test-client")
	}
	_ = body
}

func TestReadUvarintBuf(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
		want uint64
		n    int
	}{
		{"zero", []byte{0}, 0, 1},
		{"one", []byte{1}, 1, 1},
		{"127", []byte{0x7f}, 127, 1},
		{"128", []byte{0x80, 0x01}, 128, 2},
		{"300", []byte{0xAC, 0x02}, 300, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, n := readUvarintBuf(tt.buf)
			if got != tt.want || n != tt.n {
				t.Errorf("readUvarintBuf(%v) = (%d, %d), want (%d, %d)", tt.buf, got, n, tt.want, tt.n)
			}
		})
	}
}

func TestReadRequestFlexibleHeader(t *testing.T) {
	// Build an ApiVersions v3 request with flexible header encoding.
	// Flexible header: apiKey(2) + version(2) + correlationID(4)
	//                + compact_nullable_string client_id + tagged_fields(varint 0)
	var frame bytes.Buffer

	payload := make([]byte, 0, 29)
	// apiKey = 18
	payload = append(payload, 0, 18)
	// version = 3
	payload = append(payload, 0, 3)
	// correlationID = 99
	payload = append(payload, 0, 0, 0, 99)
	// client_id as compact nullable string: "kgo" = 3 bytes, encoded as varint(4)
	payload = append(payload, 4) // varint: length+1
	payload = append(payload, 'k', 'g', 'o')
	// tagged fields: 0 (no tags)
	payload = append(payload, 0)
	// body: client_software_name compact string + version compact string + tags
	payload = append(payload, 9) // "franz-go" = 8 bytes, varint(9)
	payload = append(payload, "franz-go"...)
	payload = append(payload, 6) // "1.0.0" = 5 bytes, varint(6)
	payload = append(payload, "1.0.0"...)
	payload = append(payload, 0) // body tagged fields

	// Write length prefix.
	lenEnc := newEncoder(4)
	lenEnc.putInt32(int32(len(payload)))
	frame.Write(lenEnc.bytes())
	frame.Write(payload)

	hdr, _, err := readRequest(&frame)
	if err != nil {
		t.Fatalf("readRequest flexible: %v", err)
	}
	if hdr.APIKey != 18 {
		t.Errorf("APIKey: got %d, want 18", hdr.APIKey)
	}
	if hdr.APIVersion != 3 {
		t.Errorf("APIVersion: got %d, want 3", hdr.APIVersion)
	}
	if hdr.CorrelationID != 99 {
		t.Errorf("CorrelationID: got %d, want 99", hdr.CorrelationID)
	}
	if hdr.ClientID != "kgo" {
		t.Errorf("ClientID: got %q, want %q", hdr.ClientID, "kgo")
	}
}

func TestWriteResponse(t *testing.T) {
	body := []byte{0, 0, 0, 0} // 4 bytes

	var buf bytes.Buffer
	if err := writeResponse(&buf, 42, body); err != nil {
		t.Fatalf("writeResponse: %v", err)
	}

	// Response frame: length(4) + correlationID(4) + body(4) = 12 bytes.
	if buf.Len() != 12 {
		t.Errorf("response length: got %d, want 12", buf.Len())
	}
}
