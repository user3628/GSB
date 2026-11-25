package domain

import "testing"

func TestParseMemoryBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"512Mi", 512 * 1024 * 1024},
		{"40Gi", 40 * 1024 * 1024 * 1024},
		{"1Ki", 1024},
		{"1024", 1024},
		{"1Ti", 1024 * 1024 * 1024 * 1024},
	}
	for _, c := range cases {
		got, err := ParseMemoryBytes(c.in)
		if err != nil {
			t.Fatalf("ParseMemoryBytes(%q) err=%v", c.in, err)
		}
		if got != c.want {
			t.Errorf("ParseMemoryBytes(%q)=%d want %d", c.in, got, c.want)
		}
	}
}

func TestParseMemoryBytesErrors(t *testing.T) {
	for _, in := range []string{"", "-4Gi", "abc", "40Xi", "9999999999999Ti"} {
		if _, err := ParseMemoryBytes(in); err == nil {
			t.Errorf("expected error for %q", in)
		}
	}
}

func TestParseComputeMilli(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"250m", 250},
		{"1", 1000},
		{"0.25", 250},
		{"0.001", 1},
		{"900m", 900},
	}
	for _, c := range cases {
		got, err := ParseComputeMilli(c.in)
		if err != nil {
			t.Fatalf("ParseComputeMilli(%q) err=%v", c.in, err)
		}
		if got != c.want {
			t.Errorf("ParseComputeMilli(%q)=%d want %d", c.in, got, c.want)
		}
	}
}

func TestParseComputeMilliErrors(t *testing.T) {
	for _, in := range []string{"", "-5m", "1.2345", "50x"} {
		if _, err := ParseComputeMilli(in); err == nil {
			t.Errorf("expected error for %q", in)
		}
	}
}
