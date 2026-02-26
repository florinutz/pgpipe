package kafkaserver

import (
	"testing"
)

func BenchmarkEncodeRecordBatch_1Record(b *testing.B) {
	records := []record{
		{
			Offset:    0,
			Key:       []byte("key-1"),
			Value:     []byte(`{"id":"1","name":"test"}`),
			Timestamp: 1000,
			LSN:       1,
			Headers: []recordHeader{
				{Key: "pgcdc-channel", Value: []byte("pgcdc.orders")},
			},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeRecordBatch(records)
	}
}

func BenchmarkEncodeRecordBatch_100Records(b *testing.B) {
	records := make([]record, 100)
	for i := range records {
		records[i] = record{
			Offset:    int64(i),
			Key:       []byte("key-1"),
			Value:     []byte(`{"id":"1","name":"test"}`),
			Timestamp: int64(i * 1000),
			LSN:       uint64(i + 1),
			Headers: []recordHeader{
				{Key: "pgcdc-channel", Value: []byte("pgcdc.orders")},
			},
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeRecordBatch(records)
	}
}

func BenchmarkVarintEncode(b *testing.B) {
	var buf [16]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		putVarint(buf[:], int64(i))
	}
}

func BenchmarkVarintDecode(b *testing.B) {
	var buf [16]byte
	n := putVarint(buf[:], 123456789)
	encoded := buf[:n]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = readVarint(encoded, 0)
	}
}
