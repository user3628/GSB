package latency

import (
	"testing"

	"flowtrace/internal/model"
)

func ev(seq uint64) model.Event {
	return model.Event{Seq: seq, Subject: "subj", SrcField: "a.b", DstField: "c.d"}
}

func TestDeterministicAggregates(t *testing.T) {
	run := func() []Stat {
		s := NewSampler()
		for i := uint64(1); i <= 20; i++ {
			code := "OK"
			if i%3 == 0 {
				code = "E_FLOW_ESCAPED"
			}
			s.Observe("W1", code, ev(i))
		}
		return s.Stats()
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatal("bucket count differs")
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.Window != y.Window || x.Code != y.Code || x.Count != y.Count ||
			x.Avg != y.Avg || x.P95 != y.P95 || x.Max != y.Max {
			t.Fatalf("run divergence: %+v vs %+v", x, y)
		}
	}
	var okStat *Stat
	for i := range a {
		if a[i].Code == "OK" {
			okStat = &a[i]
		}
	}
	if okStat == nil || okStat.Count != 14 {
		t.Fatalf("OK bucket: %+v", okStat)
	}
	if okStat.Max < okStat.P95 || okStat.P95 < okStat.Avg && okStat.Max < okStat.Avg {
		t.Fatalf("inconsistent aggregates: %+v", okStat)
	}
}

func TestMonotonicClock(t *testing.T) {
	c := &MonoClock{}
	prev := c.Now()
	for i := 0; i < 100; i++ {
		c.Advance(uint64(i))
		if c.Now() < prev {
			t.Fatal("clock went backwards")
		}
		prev = c.Now()
	}
}
