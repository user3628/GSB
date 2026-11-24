package pathnorm

import "testing"

func TestNormalizeWindowsSemantics(t *testing.T) {
	base := `C:\tmp\root\restricted-root`
	cases := []struct {
		in   string
		want string
		abs  bool
	}{
		{`../outside-root/secret.txt`, `C:\tmp\root\outside-root\secret.txt`, false},
		{`..\../outside-root/secret.txt`, `C:\tmp\outside-root\secret.txt`, false},
		{`app/../data/x`, `C:\tmp\root\restricted-root\data\x`, false},
		{`APP/BIN/X.EXE`, `C:\tmp\root\restricted-root\APP\BIN\X.EXE`, false},
		{`C:\tmp\..\x`, `C:\x`, true},
		{`\\srv\share\..\x`, `\\srv\share\x`, true},
		{`./plain.txt`, `C:\tmp\root\restricted-root\plain.txt`, false},
		{`..\..\x`, `C:\tmp\x`, false},
	}
	for _, tc := range cases {
		if got := IsAbsolute(tc.in); got != tc.abs {
			t.Errorf("IsAbsolute(%q) = %v, want %v", tc.in, got, tc.abs)
		}
		if got := Normalize(base, tc.in); got != tc.want {
			t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWithinCaseInsensitive(t *testing.T) {
	root := `C:\Root`
	inside := []string{`c:\root\app`, `C:\ROOT\app\..`, `C:\Root`}
	outside := []string{`C:\Root2`, `D:\Root\app`, `C:\other`}
	for _, p := range inside {
		if !Within(root, Normalize("", p)) {
			t.Errorf("expected %q within %q", p, root)
		}
	}
	for _, p := range outside {
		if Within(root, Normalize("", p)) {
			t.Errorf("expected %q outside %q", p, root)
		}
	}
}
