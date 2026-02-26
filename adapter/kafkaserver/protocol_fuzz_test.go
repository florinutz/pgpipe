//go:build go1.18

package kafkaserver

import (
	"bytes"
	"testing"
)

func FuzzReadRequest(f *testing.F) {
	// Seeds.
	f.Add([]byte{0, 0, 0, 10, 0, 18, 0, 0, 0, 0, 0, 1, 0, 0})
	f.Add([]byte{0, 0, 0, 4, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = readRequest(bytes.NewReader(data))
	})
}

func FuzzVarintRoundTrip(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(-1))
	f.Add(int64(127))
	f.Add(int64(-128))
	f.Add(int64(1 << 62))
	f.Fuzz(func(t *testing.T, v int64) {
		var buf [16]byte
		n := putVarint(buf[:], v)
		got, _, err := readVarint(buf[:n], 0)
		if err != nil {
			t.Fatalf("readVarint error: %v", err)
		}
		if got != v {
			t.Fatalf("roundtrip: put %d, got %d", v, got)
		}
	})
}

func FuzzDecoder(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 3, 'a', 'b', 'c'})
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		d := newDecoder(data)
		// Exercise all decoder methods without panicking.
		d.readInt8()
		d.readInt16()
		d.readInt32()
		d.readInt64()
		d.readString()
		d.readBytes()
		d.readArrayLen()
	})
}

func FuzzRecordBatchEncode(f *testing.F) {
	f.Add([]byte("key"), []byte("value"), int64(1000), uint64(1))
	f.Add([]byte{}, []byte{}, int64(0), uint64(0))
	f.Add([]byte("k"), []byte(`{"id":"test"}`), int64(99999), uint64(42))
	f.Fuzz(func(t *testing.T, key, value []byte, ts int64, lsn uint64) {
		rec := record{
			Key:       key,
			Value:     value,
			Timestamp: ts,
			LSN:       lsn,
		}
		// Should not panic.
		encodeRecordBatch([]record{rec})
	})
}
