package cache

import (
	"net/http"
	"testing"
)

func defaultRules() KeyRules {
	return KeyRules{CollapseSlashes: true, MergeDotSegments: true, UppercasePercent: true, SortQuery: true}
}

func TestNormalizePathEquivalentShapes(t *testing.T) {
	rules := defaultRules()
	cases := map[string]string{
		"//a///b/./c/../d": "/a/b/d",
		"/%61/%2e%2e/b":    "/b",
		"/a/%2f/b":         "/a/%2F/b",
		"////":             "/",
		"/a/b/":            "/a/b/",
		"/a/%7Euser":       "/a/~user",
	}
	for in, want := range cases {
		got, err := NormalizePath(in, rules)
		if err != nil {
			t.Fatalf("NormalizePath(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("NormalizePath(%q)=%q want %q", in, got, want)
		}
	}
}

func TestNormalizePathErrors(t *testing.T) {
	for _, in := range []string{"relative", "/bad%2", "/bad%zz", "/a b"} {
		if _, err := NormalizePath(in, defaultRules()); err == nil {
			t.Errorf("NormalizePath(%q) should fail", in)
		}
	}
}

func TestNormalizeQueryAndIdentity(t *testing.T) {
	rules := defaultRules()
	q1, err := NormalizeQuery("b=2&a=hello+world&a=%41", rules)
	if err != nil {
		t.Fatal(err)
	}
	q2, err := NormalizeQuery("a=A&a=hello%20world&b=2", rules)
	if err != nil {
		t.Fatal(err)
	}
	if q1 != q2 {
		t.Fatalf("queries differ: %q vs %q", q1, q2)
	}
	h1 := http.Header{"Accept-Language": []string{" en, fr "}}
	h2 := http.Header{"Accept-Language": []string{"en", "fr"}}
	i1, err := BuildIdentity("//static/./app.js", "b=2&a=1", h1, []string{"Accept-Language"}, rules)
	if err != nil {
		t.Fatal(err)
	}
	i2, err := BuildIdentity("/static/app.js", "a=1&b=2", h2, []string{"accept-language"}, rules)
	if err != nil {
		t.Fatal(err)
	}
	if i1.Key != i2.Key {
		t.Fatalf("equivalent identities differ: %s vs %s", i1.DisplayKey, i2.DisplayKey)
	}
	h3 := http.Header{"Accept-Language": []string{"de"}}
	i3, _ := BuildIdentity("/static/app.js", "a=1&b=2", h3, []string{"Accept-Language"}, rules)
	if i3.Key == i1.Key {
		t.Fatal("different Vary value produced the same key")
	}
}

func TestPathRulesCanBeDisabled(t *testing.T) {
	rules := KeyRules{CollapseSlashes: false, MergeDotSegments: false, UppercasePercent: true, SortQuery: true}
	got, err := NormalizePath("//a//b", rules)
	if err != nil {
		t.Fatal(err)
	}
	if got != "//a//b" {
		t.Fatalf("got %q", got)
	}
	rules.MergeDotSegments = true
	got, err = NormalizePath("/a/./b/../c", rules)
	if err != nil {
		t.Fatal(err)
	}
	if got != "/a/c" {
		t.Fatalf("dot segments not merged: %q", got)
	}
}
