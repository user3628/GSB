package engine

import "testing"

func TestLRUCacheCapacityAndReplay(t *testing.T) {
	cache := newQueryCache(2)
	cache.put("a", []byte("payload-a"))
	payload, ok := cache.get("a")
	if !ok || string(payload) != "payload-a" {
		t.Fatal("cache miss for a")
	}
	cache.put("b", []byte("payload-b"))
	cache.put("c", []byte("payload-c"))
	if len(cache.items) != 2 {
		t.Fatalf("cache size = %d, want 2", len(cache.items))
	}
	if _, ok := cache.get("a"); ok {
		t.Fatal("least-recently-used entry a was not evicted")
	}
	stats := cache.stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Evictions != 1 {
		t.Fatalf("unexpected cache stats: %+v", stats)
	}
	zero := newQueryCache(0)
	zero.put("a", []byte("x"))
	if _, ok := zero.get("a"); ok || zero.stats().Misses != 1 {
		t.Fatal("zero-capacity cache must never hit")
	}
}
