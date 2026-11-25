package pipeline

import (
	"testing"
	"time"
)

func TestLatencyBudgetExceeded(t *testing.T) {
	cache := NewQueryCache()
	fp := SceneFingerprints{ContentFP: "c", CameraFP: "cam",
		PerViewImageFP: map[string]string{}, PerViewCameraFP: map[string]string{}}
	start := time.Now()
	_, err := cache.RunQuery("summary", "q-test", fp, "cfg", 5, func() (any, error) {
		time.Sleep(60 * time.Millisecond)
		return map[string]int{"x": 1}, nil
	})
	be := asBiz(err)
	if be.Code != CodeLatencyBudget {
		t.Fatalf("got %v", err)
	}
	if be.QueryID != "q-test" || be.ElapsedMs <= 0 {
		t.Fatalf("budget error missing fields: %+v", be)
	}
	if time.Since(start) > time.Second {
		t.Fatal("budget query blocked far beyond its budget")
	}
}

func TestCacheHitByteIdentical(t *testing.T) {
	cache := NewQueryCache()
	fp := SceneFingerprints{ContentFP: "c", CameraFP: "cam",
		PerViewImageFP: map[string]string{}, PerViewCameraFP: map[string]string{}}
	payload := map[string]any{"color_mean": 128.5, "count": 7}
	calls := 0
	compute := func() (any, error) {
		calls++
		return payload, nil
	}
	r1, err := cache.RunQuery("summary", "q1", fp, "cfg", 1000, compute)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cache.RunQuery("summary", "q2", fp, "cfg", 1000, compute)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.CacheHit || calls != 1 {
		t.Fatalf("expected single compute + cache hit, calls=%d hit=%v", calls, r2.CacheHit)
	}
	if string(r1.Payload) != string(r2.Payload) {
		t.Fatal("cached and recomputed payloads differ")
	}
}
