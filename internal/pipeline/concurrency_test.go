package pipeline

import "testing"

func TestTwoServersOneWorkspaceLock(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	_, err = NewServer(dir)
	be := asBiz(err)
	if be.Code != CodeLocked {
		t.Fatalf("expected E_LOCKED for second process, got %v", err)
	}
}
