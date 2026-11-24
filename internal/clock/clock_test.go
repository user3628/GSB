package clock

import "testing"

func TestScriptedDeterminism(t *testing.T) {
	a := NewScripted()
	b := NewScripted()
	for seq := int64(1); seq <= 10; seq++ {
		la := a.Elapsed(a.Mark(seq))
		lb := b.Elapsed(b.Mark(seq))
		if la != lb || la <= 0 || la >= 1_000_000 {
			t.Fatalf("non deterministic or out of sub-ms range seq=%d a=%v b=%v", seq, la, lb)
		}
	}
	if NewScripted().Now().Unix() != NewScripted().Now().Unix() {
		t.Fatal("scripted now must be deterministic")
	}
}

func TestRealClockMonotonic(t *testing.T) {
	c := NewReal()
	m := c.Mark(1)
	if c.Elapsed(m) < 0 {
		t.Fatal("real clock elapsed must be non-negative")
	}
}
