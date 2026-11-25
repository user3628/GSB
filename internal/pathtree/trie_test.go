package pathtree

import "testing"

func TestLongestPrefixAndHops(t *testing.T) {
	tr := New()
	for _, p := range []string{"service", "service.http", "service.http.tls"} {
		tr.Insert(p)
	}
	cases := []struct {
		key        string
		wantPrefix string
		wantHops   int
		matched    bool
	}{
		{"service.http.tls.enabled", "service.http.tls", 1, true},
		{"service.http.port", "service.http", 1, true},
		{"service.db.dsn", "service", 2, true},
		{"service.http", "service.http", 0, true},
		{"other.key", "", 0, false},
	}
	for _, tc := range cases {
		prefix, hops, ok := tr.Match(tc.key)
		if ok != tc.matched || prefix != tc.wantPrefix || hops != tc.wantHops {
			t.Fatalf("Match(%q)=(%q,%d,%v), want (%q,%d,%v)",
				tc.key, prefix, hops, ok, tc.wantPrefix, tc.wantHops, tc.matched)
		}
	}
}

func TestPrefixesOrderedLongestFirst(t *testing.T) {
	tr := New()
	tr.Insert("a.b")
	tr.Insert("a")
	got := tr.Prefixes("a.b.c")
	want := []string{"a.b", "a"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch: got %v want %v", got, want)
		}
	}
}
