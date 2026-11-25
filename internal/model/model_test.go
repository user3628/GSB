package model

import "testing"

func TestRoundHalfAwayFromZero(t *testing.T) {
	cases := []struct {
		num, den, want int64
	}{
		{5, 2, 3}, // 2.5 -> 3 away from zero
		{4, 2, 2},
		{1, 3, 0},  // 0.333 -> 0
		{2, 3, 1},  // 0.667 -> 1
		{7, 10, 1}, // 0.7 -> 1
		{5, 10, 1}, // 0.5 -> 1 (half away)
	}
	for _, c := range cases {
		got, err := RoundHalfAwayFromZero(c.num, c.den)
		if err != nil || got != c.want {
			t.Fatalf("round(%d/%d)=%d want %d err=%v", c.num, c.den, got, c.want, err)
		}
	}
	if _, err := RoundHalfAwayFromZero(1, 0); err == nil {
		t.Fatal("division by zero must error")
	}
}

func TestEquivalentWithTolerance(t *testing.T) {
	// 10 per-mille tolerance on 1000 allows diff up to 10.
	if !EquivalentWithTolerance(1000, 1009, 10) {
		t.Fatal("within tolerance should be equivalent")
	}
	if EquivalentWithTolerance(1000, 989, 10) {
		t.Fatal("outside tolerance must differ")
	}
	if !EquivalentWithTolerance(100, 100, 0) {
		t.Fatal("identical values equivalent at zero tolerance")
	}
}

func TestTopologyHopsDeterministic(t *testing.T) {
	services := []Service{
		{ID: "a"}, {ID: "b", Upstreams: []string{"a"}},
		{ID: "c", Upstreams: []string{"b"}}, {ID: "d", Upstreams: []string{"a"}},
	}
	topo := NewTopology(services)
	hops := topo.HopsFrom("c")
	if hops["a"] != 2 || hops["b"] != 1 || hops["c"] != 0 {
		t.Fatalf("unexpected hops: %v", hops)
	}
	down := topo.DownstreamOf("a")
	if len(down) != 3 {
		t.Fatalf("want 3 downstream, got %v", down)
	}
}
