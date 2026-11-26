package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
)

func testStore(t *testing.T, clock *time.Time) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dns.db")
	st, err := Open(path, Options{Clock: func() time.Time { return *clock }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func sampleSnapshot(version int64, address string, ttl uint32) ZoneSnapshot {
	s := ZoneSnapshot{
		Zone:          "example.test.",
		Version:       version,
		GeneratedAtMS: 1767225600000 + version*1000,
		RRsets: []RRset{
			{Owner: "example.test.", Type: dnsmsg.TypeSOA, TTL: 30, Records: []dnsmsg.RData{{SOA: &dnsmsg.SOA{
				MName: "ns1.example.test.", RName: "hostmaster.example.test.", Serial: uint32(version), Refresh: 3600, Retry: 600, Expire: 86400, Minimum: 5,
			}}}},
			{Owner: "example.test.", Type: dnsmsg.TypeNS, TTL: 60, Records: []dnsmsg.RData{{Target: "ns1.example.test."}}},
			{Owner: "www.example.test.", Type: dnsmsg.TypeA, TTL: ttl, Records: []dnsmsg.RData{{Address: address}}},
		},
	}
	s.Checksum = ComputeSnapshotChecksum(s)
	return s
}

func sampleDelta(base, version int64, address string) ZoneDelta {
	d := ZoneDelta{
		DeltaID:       "delta-" + string(rune('0'+version)),
		Zone:          "example.test.",
		BaseVersion:   base,
		Version:       version,
		GeneratedAtMS: 1767225600000 + version*1000,
		Changes: []RRsetChange{{
			Owner: "www.example.test.", Type: dnsmsg.TypeA, TTL: 30,
			Records: []dnsmsg.RData{{Address: address}},
		}},
	}
	d.Checksum = ComputeDeltaChecksum(d)
	return d
}

func positiveWrite(name string, ttl uint32, address string, queryID string) CacheWrite {
	return CacheWrite{
		QueryID: queryID, QName: name, QType: dnsmsg.TypeA, SourceType: dnsmsg.TypeA,
		Payload: CachePayload{RCode: dnsmsg.RCodeSuccess, Answers: []dnsmsg.RR{{
			Name: name, Type: dnsmsg.TypeA, Class: 1, TTL: ttl, Data: dnsmsg.RData{Address: address},
		}}},
	}
}

func TestPositiveCacheTTLMillisAndExpiredCleanup(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	st := testStore(t, &now)
	ctx := context.Background()
	if _, err := st.SubmitSnapshot(ctx, sampleSnapshot(1, "192.0.2.10", 10), "snap-1"); err != nil {
		t.Fatal(err)
	}
	res, err := st.WriteCache(ctx, positiveWrite("www.example.test.", 10, "192.0.2.10", "q-1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.TTLMillis != 10000 {
		t.Fatalf("ttl=%d", res.TTLMillis)
	}
	hit, err := st.Lookup(ctx, "WWW.example.test", dnsmsg.TypeA)
	if err != nil || !hit.Hit {
		t.Fatalf("hit=%v err=%v", hit, err)
	}
	if hit.Entry.RemainingMS != 10000 || hit.Entry.Payload.Answers[0].TTL != 10 {
		t.Fatalf("remaining=%d ttl=%d", hit.Entry.RemainingMS, hit.Entry.Payload.Answers[0].TTL)
	}
	now = now.Add(1200 * time.Millisecond)
	hit, err = st.Lookup(ctx, "www.example.test.", dnsmsg.TypeA)
	if err != nil || !hit.Hit {
		t.Fatalf("hit=%v err=%v", hit, err)
	}
	if hit.Entry.RemainingMS != 8800 {
		t.Fatalf("remaining=%d, want 8800", hit.Entry.RemainingMS)
	}
	if got := hit.Entry.Payload.Answers[0].TTL; got != 9 {
		t.Fatalf("wire ttl=%d, want 9", got)
	}
	now = now.Add(9 * time.Second)
	hit, err = st.Lookup(ctx, "www.example.test.", dnsmsg.TypeA)
	if err != nil || hit.Hit {
		t.Fatalf("expired entry hit=%v err=%v", hit, err)
	}
	stats, err := st.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != 0 {
		t.Fatalf("expired entry was not cleaned: %+v", stats)
	}
}

func TestNegativeCacheUsesSOAMinimumAndRejectsMissingSOA(t *testing.T) {
	now := time.UnixMilli(2_000_000)
	st := testStore(t, &now)
	ctx := context.Background()
	soa := dnsmsg.RR{Name: "example.test.", Type: dnsmsg.TypeSOA, Class: 1, TTL: 30, Data: dnsmsg.RData{SOA: &dnsmsg.SOA{
		MName: "ns1.example.test.", RName: "hostmaster.example.test.", Serial: 1, Refresh: 3600, Retry: 600, Expire: 86400, Minimum: 5,
	}}}
	w := CacheWrite{
		QueryID: "negative-1", QName: "absent.example.test.", QType: dnsmsg.TypeA,
		Payload: CachePayload{RCode: dnsmsg.RCodeNXDomain, Authorities: []dnsmsg.RR{soa}},
	}
	res, err := st.WriteCache(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	if res.TTLMillis != 5000 || res.Status != CacheNXDomain {
		t.Fatalf("result=%+v", res)
	}
	now = now.Add(3 * time.Second)
	hit, err := st.Lookup(ctx, "absent.example.test.", dnsmsg.TypeA)
	if err != nil || !hit.Hit || hit.Entry.Status != CacheNXDomain || hit.Entry.RemainingMS != 2000 {
		t.Fatalf("lookup=%+v err=%v", hit, err)
	}

	w2 := CacheWrite{QueryID: "negative-no-soa", QName: "other.example.test.", QType: dnsmsg.TypeA,
		Payload: CachePayload{RCode: dnsmsg.RCodeNXDomain}}
	_, err = st.WriteCache(ctx, w2)
	if errs.CodeOf(err) != errs.ENegativeTTLUndefined {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
}

func TestCacheIdempotencyAndConflictingRequest(t *testing.T) {
	now := time.UnixMilli(3_000_000)
	st := testStore(t, &now)
	ctx := context.Background()
	first, err := st.WriteCache(ctx, positiveWrite("www.example.test.", 60, "192.0.2.10", "same-query"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.WriteCache(ctx, positiveWrite("www.example.test.", 60, "192.0.2.10", "same-query"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.Idempotent || second.CacheID != first.CacheID {
		t.Fatalf("idempotent result=%+v first=%+v", second, first)
	}
	_, err = st.WriteCache(ctx, positiveWrite("www.example.test.", 60, "192.0.2.11", "same-query"))
	if errs.CodeOf(err) != errs.EConflictingRequest {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
}

func TestZoneDeltaInvalidatesCachesAndStaleBaseIsAtomic(t *testing.T) {
	now := time.UnixMilli(4_000_000)
	st := testStore(t, &now)
	ctx := context.Background()
	if _, err := st.SubmitSnapshot(ctx, sampleSnapshot(1, "192.0.2.10", 60), "snap-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteCache(ctx, positiveWrite("www.example.test.", 60, "192.0.2.10", "cache-1")); err != nil {
		t.Fatal(err)
	}
	if hit, _ := st.Lookup(ctx, "www.example.test.", dnsmsg.TypeA); !hit.Hit {
		t.Fatal("expected positive cache hit before delta")
	}
	epochBefore, err := st.ZoneEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delta := sampleDelta(1, 2, "192.0.2.11")
	result, err := st.SubmitDelta(ctx, delta, "batch-2")
	if err != nil {
		t.Fatal(err)
	}
	if result.InvalidatedCacheEntries != 1 || result.Version != 2 {
		t.Fatalf("result=%+v", result)
	}
	epochAfter, err := st.ZoneEpoch(ctx)
	if err != nil || epochAfter <= epochBefore {
		t.Fatalf("zone epoch did not advance: before=%d after=%d err=%v", epochBefore, epochAfter, err)
	}
	if hit, err := st.Lookup(ctx, "www.example.test.", dnsmsg.TypeA); err != nil || hit.Hit {
		t.Fatalf("cache survived delta: hit=%+v err=%v", hit, err)
	}
	state, err := st.ZoneState(ctx, "example.test.")
	if err != nil || state.Version != 2 {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	derived, err := st.DerivedRows(ctx, "example.test.")
	if err != nil || len(derived) == 0 {
		t.Fatalf("derived=%+v err=%v", derived, err)
	}
	for _, row := range derived {
		if row.Version != 2 {
			t.Fatalf("derived row kept old version: %+v", row)
		}
	}

	stale := sampleDelta(1, 3, "192.0.2.99")
	_, err = st.SubmitDelta(ctx, stale, "batch-stale")
	if errs.CodeOf(err) != errs.EStaleBase {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
	after, err := st.ZoneState(ctx, "example.test.")
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != 2 || after.Checksum != state.Checksum {
		t.Fatalf("stale delta changed state: %+v", after)
	}
	again, err := st.SubmitDelta(ctx, delta, "batch-2")
	if err != nil || !again.Idempotent || again.Version != 2 {
		t.Fatalf("idempotent delta=%+v err=%v", again, err)
	}
}

func TestConcurrentDeltaSameBaseOnlyOneWins(t *testing.T) {
	now := time.UnixMilli(5_000_000)
	st := testStore(t, &now)
	ctx := context.Background()
	if _, err := st.SubmitSnapshot(ctx, sampleSnapshot(1, "192.0.2.10", 60), "snap-1"); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		err error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i, addr := range []string{"192.0.2.21", "192.0.2.22"} {
		go func(i int, addr string) {
			<-start
			d := sampleDelta(1, 2, addr)
			d.DeltaID = "delta-concurrent-" + string(rune('a'+i))
			_, err := st.SubmitDelta(ctx, d, d.DeltaID)
			results <- outcome{err: err}
		}(i, addr)
	}
	close(start)
	var ok, stale int
	for i := 0; i < 2; i++ {
		r := <-results
		switch errs.CodeOf(r.err) {
		case "":
			ok++
		case errs.EStaleBase:
			stale++
		default:
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if ok != 1 || stale != 1 {
		t.Fatalf("ok=%d stale=%d", ok, stale)
	}
}

func TestRecoveryRollsBackCacheWithPendingIntent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dns.db")
	ctx := context.Background()
	st, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := st.db.ExecContext(ctx, `INSERT INTO intention_log(intent_id, kind, target, payload_hash, status, detail, created_ms, updated_ms)
VALUES('intent-test','cache_write','www.example.test.|1','hash','pending','',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(CachePayload{RCode: dnsmsg.RCodeSuccess, Answers: []dnsmsg.RR{{
		Name: "www.example.test.", Type: dnsmsg.TypeA, Class: 1, TTL: 60, Data: dnsmsg.RData{Address: "192.0.2.10"},
	}}})
	if _, err := st.db.ExecContext(ctx, `INSERT INTO cache_entries(cache_key,qname,qtype,status,source_type,zone,zone_version,written_ms,ttl_ms,payload_json,intent_id,created_ms)
VALUES('www.example.test.|1','www.example.test.',1,'positive',1,'',0,?,60000,?,'intent-test',?)`, now, string(payload), now); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.ListCache(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("pending cache row survived recovery: %+v", entries)
	}
	report := reopened.RecoveryReport()
	if report.PendingIntents != 1 || report.CacheEntriesRolledBack != 1 {
		t.Fatalf("report=%+v", report)
	}
	var status string
	if err := reopened.db.QueryRowContext(ctx, `SELECT status FROM intention_log WHERE intent_id='intent-test'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "aborted" {
		t.Fatalf("intent status=%s", status)
	}
}

func TestSIGKILLCacheWriteAndZoneBatchRecovery(t *testing.T) {
	ctx := context.Background()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("cache-write", func(t *testing.T) {
		db := filepath.Join(t.TempDir(), "crash-cache.db")
		cmd := exec.Command(exe, "-test.run=TestCrashHelperProcess", "-test.count=1")
		cmd.Env = append(os.Environ(), "DNSCACHE_CRASH_HELPER=cache", "DNSCACHE_CRASH_DB="+db)
		err := cmd.Run()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("helper did not die: %v", err)
		}
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || ws.Signal() != syscall.SIGKILL {
			t.Fatalf("helper exit=%v", err)
		}
		st, err := Open(db, Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		report := st.RecoveryReport()
		if report.PendingIntents != 1 {
			t.Fatalf("report=%+v", report)
		}
		// SIGKILL happened after the intent commit but before the cache
		// transaction, so there is no cache row to roll back; the report must
		// still account for the pending intent.
		if report.CacheEntriesRolledBack != 0 {
			t.Fatalf("unexpected rollback count: %+v", report)
		}
		entries, _ := st.ListCache(ctx, true)
		if len(entries) != 0 {
			t.Fatalf("interrupted cache row visible: %+v", entries)
		}
	})

	t.Run("zone-delta", func(t *testing.T) {
		db := filepath.Join(t.TempDir(), "crash-zone.db")
		cmd := exec.Command(exe, "-test.run=TestCrashHelperProcess", "-test.count=1")
		cmd.Env = append(os.Environ(), "DNSCACHE_CRASH_HELPER=zone", "DNSCACHE_CRASH_DB="+db)
		err := cmd.Run()
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("helper did not die: %v", err)
		}
		st, err := Open(db, Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		state, err := st.ZoneState(ctx, "example.test.")
		if err != nil {
			t.Fatal(err)
		}
		if state.Version != 1 {
			t.Fatalf("interrupted delta reached disk: %+v", state)
		}
		report := st.RecoveryReport()
		if report.PendingIntents != 1 || report.ZoneBatchesAborted != 1 {
			t.Fatalf("report=%+v", report)
		}
	})
}

func TestCrashHelperProcess(t *testing.T) {
	mode := os.Getenv("DNSCACHE_CRASH_HELPER")
	if mode == "" {
		t.Skip("helper only")
	}
	db := os.Getenv("DNSCACHE_CRASH_DB")
	ctx := context.Background()
	st, err := Open(db, Options{AfterIntent: func(kind, target string) {
		if kind == "delta" || (mode == "cache" && kind == "cache_write") {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "cache":
		_, err = st.WriteCache(ctx, positiveWrite("www.example.test.", 60, "192.0.2.10", "crash-query"))
	case "zone":
		if _, err = st.SubmitSnapshot(ctx, sampleSnapshot(1, "192.0.2.10", 60), "crash-snap"); err != nil {
			break
		}
		_, err = st.SubmitDelta(ctx, sampleDelta(1, 2, "192.0.2.11"), "crash-delta")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Fatal("helper did not crash")
}

func TestRecoveryReplaysDurableZoneBatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replay.db")
	ctx := context.Background()
	st, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	snap := sampleSnapshot(2, "192.0.2.11", 60)
	if _, err := st.SubmitSnapshot(ctx, snap, "durable-v2"); err != nil {
		t.Fatal(err)
	}
	// Simulate an interrupted batch whose zone facts are nevertheless durable.
	now := time.Now().UnixMilli()
	payload, _ := json.Marshal(snap)
	if _, err := st.db.ExecContext(ctx, `INSERT INTO intention_log(intent_id,kind,target,payload_hash,status,detail,created_ms,updated_ms)
VALUES('intent-replay','snapshot','pending-replay','x','pending','',?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO zone_batches(batch_id,zone,kind,base_version,target_version,checksum,status,code,message,result_json,payload_json,created_ms,updated_ms)
VALUES('pending-replay','example.test.','snapshot',0,2,?,'pending','','','',?,?,?)`, snap.Checksum, string(payload), now, now); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report := reopened.RecoveryReport()
	if report.ZoneBatchesReplayed != 1 {
		t.Fatalf("report=%+v", report)
	}
	var status string
	if err := reopened.db.QueryRowContext(ctx, `SELECT status FROM zone_batches WHERE batch_id='pending-replay'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "committed_replayed" {
		t.Fatalf("batch status=%s", status)
	}
}

func TestCNAMEOnlyChainIsNODATAAndUsesSOANegativeTTL(t *testing.T) {
	now := time.UnixMilli(6_000_000)
	st := testStore(t, &now)
	ctx := context.Background()
	soa := dnsmsg.RR{Name: "example.test.", Type: dnsmsg.TypeSOA, Class: 1, TTL: 30, Data: dnsmsg.RData{SOA: &dnsmsg.SOA{
		MName: "ns1.example.test.", RName: "hostmaster.example.test.", Serial: 9, Refresh: 3600, Retry: 600, Expire: 86400, Minimum: 5,
	}}}
	w := CacheWrite{QueryID: "cname-nodata", QName: "alias.example.test.", QType: dnsmsg.TypeA,
		Payload: CachePayload{RCode: dnsmsg.RCodeSuccess,
			Answers:     []dnsmsg.RR{{Name: "alias.example.test.", Type: dnsmsg.TypeCNAME, Class: 1, TTL: 60, Data: dnsmsg.RData{Target: "missing.example.test."}}},
			Authorities: []dnsmsg.RR{soa},
		}}
	res, err := st.WriteCache(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != CacheNODATA || res.TTLMillis != 5000 {
		t.Fatalf("result=%+v", res)
	}
	hit, err := st.Lookup(ctx, "alias.example.test.", dnsmsg.TypeA)
	if err != nil || !hit.Hit || hit.Entry.Status != CacheNODATA || hit.Entry.SourceType != dnsmsg.TypeSOA || len(hit.Entry.Payload.Answers) != 1 {
		t.Fatalf("lookup=%+v err=%v", hit, err)
	}
}
