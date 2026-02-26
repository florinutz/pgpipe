package view

import (
	"math"
	"testing"
)

func TestCountAgg(t *testing.T) {
	a := NewAggregator(AggCount)
	// COUNT(*) semantics: caller passes non-nil sentinel for every row.
	a.Add(true)
	a.Add(true)
	a.Add(true)

	got := a.Result().(int64)
	if got != 3 {
		t.Errorf("Count = %d, want 3", got)
	}

	a.Reset()
	if a.Result().(int64) != 0 {
		t.Error("Count after reset should be 0")
	}
}

func TestCountFieldAgg(t *testing.T) {
	a := NewAggregator(AggCount)
	// COUNT(field) semantics: nil values are skipped (SQL NULL).
	a.Add(nil)
	a.Add("hello")
	a.Add(42)
	a.Add(nil)

	got := a.Result().(int64)
	if got != 2 {
		t.Errorf("Count(field) = %d, want 2", got)
	}
}

func TestSumAgg(t *testing.T) {
	a := NewAggregator(AggSum)

	a.Add(float64(10))
	a.Add(float64(20))
	a.Add(float64(30))

	got := a.Result().(float64)
	if got != 60 {
		t.Errorf("Sum = %f, want 60", got)
	}

	// Non-numeric values are skipped.
	ok := a.Add("not a number")
	if ok {
		t.Error("Sum should skip non-numeric values")
	}
	if a.Result().(float64) != 60 {
		t.Error("Sum should not change after non-numeric add")
	}

	a.Reset()
	if a.Result().(float64) != 0 {
		t.Error("Sum after reset should be 0")
	}
}

func TestAvgAgg(t *testing.T) {
	a := NewAggregator(AggAvg)

	a.Add(float64(10))
	a.Add(float64(20))
	a.Add(float64(30))

	got := a.Result().(float64)
	if got != 20 {
		t.Errorf("Avg = %f, want 20", got)
	}

	a.Reset()
	if a.Result().(float64) != 0 {
		t.Error("Avg after reset should be 0")
	}
}

func TestMinAgg(t *testing.T) {
	a := NewAggregator(AggMin)

	a.Add(float64(30))
	a.Add(float64(10))
	a.Add(float64(20))

	got := a.Result().(float64)
	if got != 10 {
		t.Errorf("Min = %f, want 10", got)
	}

	a.Reset()
	if a.Result() != nil {
		t.Error("Min after reset should be nil")
	}
}

func TestMaxAgg(t *testing.T) {
	a := NewAggregator(AggMax)

	a.Add(float64(10))
	a.Add(float64(30))
	a.Add(float64(20))

	got := a.Result().(float64)
	if got != 30 {
		t.Errorf("Max = %f, want 30", got)
	}

	a.Reset()
	if a.Result() != nil {
		t.Error("Max after reset should be nil")
	}
}

func TestSumIntegerValues(t *testing.T) {
	a := NewAggregator(AggSum)
	a.Add(int64(5))
	a.Add(int(10))
	a.Add(float64(2.5))

	got := a.Result().(float64)
	if math.Abs(got-17.5) > 0.001 {
		t.Errorf("Sum = %f, want 17.5", got)
	}
}

func TestAvgEmpty(t *testing.T) {
	a := NewAggregator(AggAvg)
	got := a.Result().(float64)
	if got != 0 {
		t.Errorf("Avg of empty = %f, want 0", got)
	}
}

func TestSumNilSkipped(t *testing.T) {
	a := NewAggregator(AggSum)
	ok := a.Add(nil)
	if ok {
		t.Error("Sum should skip nil values")
	}
}
