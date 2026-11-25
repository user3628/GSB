package appcli

import (
	"path/filepath"
	"testing"

	"headprune/internal/model"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := New(filepath.Join(t.TempDir(), "ws"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestServiceLifecycleAndRollback(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Generate(); err != nil {
		t.Fatal(err)
	}
	res, err := svc.RunAll()
	if err != nil {
		t.Fatal(err)
	}
	v1, err := svc.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RunAll(); err != nil {
		t.Fatal(err)
	}
	v2, err := svc.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != v1.Version+1 {
		t.Fatal("versions must advance monotonically")
	}
	rolled, err := svc.Rollback(v1.Version)
	if err != nil {
		t.Fatal(err)
	}
	d, _, err := svc.RestoreVersion(rolled.Version)
	if err != nil {
		t.Fatal(err)
	}
	if d.Fingerprint() != res.Decision.Fingerprint() {
		t.Fatal("rollback must restore the exact decision basis")
	}
	hist, err := svc.History()
	if err != nil || len(hist) != 3 {
		t.Fatalf("history len = %d, err=%v", len(hist), err)
	}
}

func TestGracefulStopResume(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Generate(); err != nil {
		t.Fatal(err)
	}
	if err := svc.Stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RunAll(); err == nil {
		t.Fatal("run must be refused while stopped")
	} else {
		be, ok := model.AsBizError(err)
		if !ok || be.Code != model.CodeConflict {
			t.Fatalf("expected E_CONFLICT, got %v", err)
		}
	}
	if err := svc.Resume(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RunAll(); err != nil {
		t.Fatalf("run must succeed after resume: %v", err)
	}
}

func TestBadPolicyRejected(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Generate(); err != nil {
		t.Fatal(err)
	}
	bad := svc.dataset.Policy
	bad.Tolerance = -1
	err := svc.SetPolicy(bad)
	if err == nil {
		t.Fatal("negative tolerance must be rejected")
	}
	be, ok := model.AsBizError(err)
	if !ok || be.Code != model.CodePolicyInvalid {
		t.Fatalf("expected E_POLICY_INVALID, got %v", err)
	}
}
