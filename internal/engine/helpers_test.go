package engine

import (
	"casengine/internal/token"
	"testing"
)

func tamperRaw(t *testing.T, raw string) string {
	t.Helper()
	out, err := token.Tamper(raw)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
