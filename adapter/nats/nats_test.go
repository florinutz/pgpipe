package nats_test

import (
	"testing"

	adapternats "github.com/florinutz/pgcdc/adapter/nats"
)

func TestNATSAdapter_Name(t *testing.T) {
	a := adapternats.New("nats://localhost:4222", "", "", "", 0, 0, 0, nil, nil)
	if got := a.Name(); got != "nats" {
		t.Errorf("Name() = %q, want %q", got, "nats")
	}
}
