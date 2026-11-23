package apperr

import "testing"

func TestWithContextDoesNotAliasNil(t *testing.T) {
	e := New(CodeInvalidRequest, "bad", 400)
	WithContext(e, "field", "x")
	if e.Context["field"] != "x" {
		t.Fatalf("context not set: %#v", e.Context)
	}
}
