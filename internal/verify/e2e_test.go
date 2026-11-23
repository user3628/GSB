package verify

import (
	"net/http/httptest"
	"strings"
	"testing"

	"meshmigrate/internal/compat"
	"meshmigrate/internal/engine"
	"meshmigrate/internal/httpapi"
	"meshmigrate/internal/samples"
	"meshmigrate/internal/store"
)

func TestFixedSamplesEndToEnd(t *testing.T) {
	dir := t.TempDir()
	if _, err := samples.Generate(dir); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(t.TempDir() + "/mesh.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	matrix, err := compat.LoadMatrix()
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(st, matrix, engine.Options{})
	ts := httptest.NewServer(httpapi.New(eng, st, false).Handler())
	defer ts.Close()
	summary, err := Run(ts.URL, dir)
	if err != nil {
		t.Fatalf("fixed sample verification failed: %v\n%s", err, summary)
	}
	if !strings.Contains(summary, `"status": "pass"`) {
		t.Fatalf("unexpected verification summary: %s", summary)
	}
}
