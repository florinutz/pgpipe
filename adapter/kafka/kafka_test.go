package kafka_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/kafka"
)

func TestKafkaAdapter_Name(t *testing.T) {
	a := kafka.New(nil, "", "", "", "", "", false, 0, 0, nil, nil, "")
	if got := a.Name(); got != "kafka" {
		t.Errorf("Name() = %q, want %q", got, "kafka")
	}
}
