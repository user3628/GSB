package store

import "testing"

func TestPublishMonotonicAndReplacementDetection(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Lock(); err != nil {
		t.Fatal(err)
	}

	base := Checkpoint{MaterialID: "M1", Stage: "control", Seq: 1, ConfigVer: 1,
		RecipeFP: "r1", ArtifactFP: "a1", Payload: []byte("v1")}
	if out, err := s.Publish(base); err != nil || out != PublishAccepted {
		t.Fatalf("first publish: %v %v", out, err)
	}
	// Only one active row allowed.
	if rows, err := s.AllActive(); err != nil || len(rows) != 1 {
		t.Fatalf("active rows=%d err=%v", len(rows), err)
	}
	// Non-contiguous sequence is stale.
	gap := base
	gap.Seq = 3
	if out, _ := s.Publish(gap); out != PublishStale {
		t.Fatalf("gap publish=%v want stale", out)
	}
	// Same seq with different fingerprint is tampering (whole replacement).
	tampered := base
	tampered.ArtifactFP = "evil"
	if out, _ := s.Publish(tampered); out != PushbackTampered {
		t.Fatalf("tamper publish=%v want tampered", out)
	}
	// Same seq same content is a rollback/repeat.
	if out, _ := s.Publish(base); out != PushbackRollback {
		t.Fatalf("repeat publish=%v want rollback", out)
	}
	// Normal advance is accepted.
	next := base
	next.Seq = 2
	next.RecipeFP = "r2"
	next.ArtifactFP = "a2"
	next.Payload = []byte("v2")
	if out, err := s.Publish(next); err != nil || out != PublishAccepted {
		t.Fatalf("advance publish: %v %v", out, err)
	}
	// A lagging seq is rollback.
	if out, _ := s.Publish(base); out != PushbackRollback {
		t.Fatalf("lagging publish=%v want rollback", out)
	}
	active, err := s.Active("M1", "control")
	if err != nil || active.Seq != 2 {
		t.Fatalf("active should be seq 2: %+v err=%v", active, err)
	}
	// Cross-material first publish at seq != 1 is stale.
	other := base
	other.MaterialID = "M2"
	other.Seq = 2
	if out, _ := s.Publish(other); out != PublishStale {
		t.Fatalf("cross-material publish=%v want stale", out)
	}
}

func TestInvalidationTrail(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.LogInvalidation("M1", "noise", "frequency changed", "old", "new"); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Invalidations("M1")
	if err != nil || len(rows) != 1 || rows[0]["stage"] != "noise" {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
}
