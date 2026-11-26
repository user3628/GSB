package resolver

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/gsb/dnscache/internal/authserver"
	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
	"example.com/gsb/dnscache/internal/scheduler"
	"example.com/gsb/dnscache/internal/store"
)

type testStack struct {
	st       *store.Store
	auth     *authserver.Server
	res      *Resolver
	sched    *scheduler.Scheduler
	server   *Server
	metrics  *Metrics
	clock    *time.Time
	cancel   context.CancelFunc
	upstream string
}

func testSnapshot(version int64, address string) store.ZoneSnapshot {
	s := store.ZoneSnapshot{
		Zone:          "example.test.",
		Version:       version,
		GeneratedAtMS: 1767225600000 + version*1000,
		RRsets: []store.RRset{
			{Owner: "example.test.", Type: dnsmsg.TypeSOA, TTL: 30, Records: []dnsmsg.RData{{SOA: &dnsmsg.SOA{
				MName: "ns1.example.test.", RName: "hostmaster.example.test.", Serial: uint32(version), Refresh: 3600, Retry: 600, Expire: 86400, Minimum: 5,
			}}}},
			{Owner: "example.test.", Type: dnsmsg.TypeNS, TTL: 60, Records: []dnsmsg.RData{{Target: "ns1.example.test."}}},
			{Owner: "example.test.", Type: dnsmsg.TypeMX, TTL: 60, Records: []dnsmsg.RData{{Preference: 10, Target: "mail.example.test."}}},
			{Owner: "ns1.example.test.", Type: dnsmsg.TypeA, TTL: 60, Records: []dnsmsg.RData{{Address: "192.0.2.53"}}},
			{Owner: "www.example.test.", Type: dnsmsg.TypeA, TTL: 60, Records: []dnsmsg.RData{{Address: address}}},
			{Owner: "www.example.test.", Type: dnsmsg.TypeAAAA, TTL: 60, Records: []dnsmsg.RData{{Address: "2001:db8::10"}}},
			{Owner: "alias.example.test.", Type: dnsmsg.TypeCNAME, TTL: 60, Records: []dnsmsg.RData{{Target: "www.example.test."}}},
			{Owner: "mail.example.test.", Type: dnsmsg.TypeA, TTL: 60, Records: []dnsmsg.RData{{Address: "192.0.2.25"}}},
			{Owner: "txt.example.test.", Type: dnsmsg.TypeTXT, TTL: 60, Records: []dnsmsg.RData{{Texts: []string{"local deterministic sample"}}}},
		},
	}
	s.Checksum = store.ComputeSnapshotChecksum(s)
	return s
}

func newTestStack(t *testing.T, behavior authserver.Behavior, network string, delay time.Duration, withListener bool) *testStack {
	t.Helper()
	now := time.UnixMilli(10_000_000)
	st, err := store.Open(t.TempDir()+"/dns.db", store.Options{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	snap := testSnapshot(1, "192.0.2.10")
	if _, err := st.SubmitSnapshot(context.Background(), snap, "snap-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	auth, err := authserver.New(snap, authserver.Config{UDPAddr: "127.0.0.1:0", TCPAddr: "127.0.0.1:0", Behavior: behavior, Delay: delay}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Start(ctx); err != nil {
		t.Fatal(err)
	}
	upstream := auth.UDPAddr()
	if network == "tcp" {
		upstream = auth.TCPAddr()
	}
	sched := scheduler.New(scheduler.Config{
		GlobalLimit: 4, PerServerLimit: 2, QueueLimit: 32,
		RoundBudget: time.Second, DetachAfter: 500 * time.Millisecond,
	})
	metrics := NewMetrics()
	res := New(st, sched, Config{Upstreams: []string{upstream}, Network: network, UpstreamTimeout: 200 * time.Millisecond}, metrics, nil)
	stack := &testStack{st: st, auth: auth, res: res, sched: sched, metrics: metrics, clock: &now, cancel: cancel, upstream: upstream}
	if withListener {
		server := NewServer(res, ListenConfig{UDPAddr: "127.0.0.1:0", TCPAddr: "127.0.0.1:0", QueryTimeout: time.Second, TCPIdleTimeout: time.Second}, nil)
		if err := server.Start(ctx); err != nil {
			t.Fatal(err)
		}
		stack.server = server
	}
	t.Cleanup(func() {
		if stack.server != nil {
			_ = stack.server.Close()
		}
		cancel()
		_ = auth.Close()
		_ = st.Close()
	})
	return stack
}

func resolve(t *testing.T, r *Resolver, name string, typ dnsmsg.Type) *dnsmsg.Message {
	t.Helper()
	msg, err := r.Resolve(context.Background(), dnsmsg.NewQuery(uint16(time.Now().UnixNano()), name, typ))
	if err != nil {
		t.Fatalf("resolve %s/%s: %v", name, typ, err)
	}
	return msg
}

func TestResolverAllSupportedTypesAndCacheTTL(t *testing.T) {
	stack := newTestStack(t, authserver.Normal, "udp", 0, false)
	checks := []struct {
		name string
		typ  dnsmsg.Type
		want dnsmsg.Type
	}{
		{"www.example.test.", dnsmsg.TypeA, dnsmsg.TypeA},
		{"www.example.test.", dnsmsg.TypeAAAA, dnsmsg.TypeAAAA},
		{"alias.example.test.", dnsmsg.TypeCNAME, dnsmsg.TypeCNAME},
		{"example.test.", dnsmsg.TypeMX, dnsmsg.TypeMX},
		{"txt.example.test.", dnsmsg.TypeTXT, dnsmsg.TypeTXT},
		{"example.test.", dnsmsg.TypeNS, dnsmsg.TypeNS},
		{"example.test.", dnsmsg.TypeSOA, dnsmsg.TypeSOA},
	}
	for _, tc := range checks {
		resp := resolve(t, stack.res, tc.name, tc.typ)
		if !hasAnswerType(resp.Answers, tc.want) {
			t.Fatalf("%s/%s missing %s: %+v", tc.name, tc.typ, tc.want, resp.Answers)
		}
	}
	alias := resolve(t, stack.res, "alias.example.test.", dnsmsg.TypeA)
	if !hasAnswerType(alias.Answers, dnsmsg.TypeCNAME) || !hasAnswerType(alias.Answers, dnsmsg.TypeA) {
		t.Fatalf("CNAME chain was not assembled: %+v", alias.Answers)
	}

	before := stack.auth.QueryCount()
	first := resolve(t, stack.res, "www.example.test.", dnsmsg.TypeA)
	if got := first.Answers[0].TTL; got != 60 {
		t.Fatalf("initial TTL=%d", got)
	}
	*stack.clock = stack.clock.Add(1200 * time.Millisecond)
	second := resolve(t, stack.res, "www.example.test.", dnsmsg.TypeA)
	if got := second.Answers[0].TTL; got != 59 {
		t.Fatalf("cached TTL=%d, want 59", got)
	}
	if stack.auth.QueryCount() != before {
		t.Fatalf("cache hit contacted upstream: before=%d after=%d", before, stack.auth.QueryCount())
	}
	_ = stack.auth.Close()
	third := resolve(t, stack.res, "www.example.test.", dnsmsg.TypeA)
	if !hasAnswerType(third.Answers, dnsmsg.TypeA) {
		t.Fatal("cache hit after authority shutdown failed")
	}
}

func TestNegativeCacheNXDomainNODATAAndMissingSOA(t *testing.T) {
	stack := newTestStack(t, authserver.Normal, "udp", 0, false)
	nx := resolve(t, stack.res, "absent.example.test.", dnsmsg.TypeA)
	if nx.Header.RCode() != dnsmsg.RCodeNXDomain || len(nx.Authorities) == 0 || nx.Authorities[0].Type != dnsmsg.TypeSOA {
		t.Fatalf("NXDOMAIN response=%+v", nx)
	}
	hit, err := stack.st.Lookup(context.Background(), "absent.example.test.", dnsmsg.TypeA)
	if err != nil || !hit.Hit || hit.Entry.Status != store.CacheNXDomain {
		t.Fatalf("negative lookup=%+v err=%v", hit, err)
	}
	nodata := resolve(t, stack.res, "txt.example.test.", dnsmsg.TypeA)
	if nodata.Header.RCode() != dnsmsg.RCodeSuccess || len(nodata.Answers) != 0 || len(nodata.Authorities) == 0 {
		t.Fatalf("NODATA response=%+v", nodata)
	}
	hit, err = stack.st.Lookup(context.Background(), "txt.example.test.", dnsmsg.TypeA)
	if err != nil || !hit.Hit || hit.Entry.Status != store.CacheNODATA {
		t.Fatalf("nodata lookup=%+v err=%v", hit, err)
	}

	t.Run("missing-soa", func(t *testing.T) {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 4096)
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			q, err := dnsmsg.Parse(buf[:n])
			if err != nil {
				return
			}
			resp := dnsmsg.NewResponse(q, dnsmsg.RCodeNXDomain)
			b, _ := resp.Pack()
			_, _ = pc.WriteTo(b, addr)
		}()
		st, err := store.Open(t.TempDir()+"/no-soa.db", store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		sched := scheduler.New(scheduler.Config{GlobalLimit: 1, PerServerLimit: 1, QueueLimit: 2, RoundBudget: time.Second})
		r := New(st, sched, Config{Upstreams: []string{pc.LocalAddr().String()}, Network: "udp", UpstreamTimeout: 200 * time.Millisecond}, NewMetrics(), nil)
		_, err = r.Resolve(context.Background(), dnsmsg.NewQuery(1, "missing.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.ENegativeTTLUndefined {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
		<-done
	})
}

func TestConcurrentIdenticalQueriesUseOneUpstreamExchange(t *testing.T) {
	stack := newTestStack(t, authserver.Normal, "udp", 60*time.Millisecond, false)
	const clients = 10
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(uint16(time.Now().UnixNano()), "www.example.test.", dnsmsg.TypeA))
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := stack.auth.QueryCount(); got != 1 {
		t.Fatalf("upstream exchanges=%d, want 1", got)
	}
	if hit, _ := stack.st.Lookup(context.Background(), "www.example.test.", dnsmsg.TypeA); !hit.Hit {
		t.Fatal("coalesced result was not cached")
	}
}

func TestUpstreamTimeoutRefusedAndBadResponses(t *testing.T) {
	t.Run("udp-timeout", func(t *testing.T) {
		stack := newTestStack(t, authserver.Drop, "udp", 0, false)
		_, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.EUpstreamTimeout {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
		waitForStats(t, stack.sched, func(st scheduler.Stats) bool { return st.GlobalActive == 0 })
	})
	t.Run("tcp-timeout", func(t *testing.T) {
		stack := newTestStack(t, authserver.Drop, "tcp", 0, false)
		_, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.EUpstreamTimeout {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
	})
	t.Run("bad-id", func(t *testing.T) {
		stack := newTestStack(t, authserver.BadID, "udp", 0, false)
		_, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.EUpstreamBadResponse {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		stack := newTestStack(t, authserver.Malformed, "udp", 0, false)
		_, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.EUpstreamBadResponse {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
	})
	t.Run("refused-rcode", func(t *testing.T) {
		stack := newTestStack(t, authserver.Refused, "udp", 0, false)
		_, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.EUpstreamRefused {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
	})
	t.Run("connection-refused", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		st, err := store.Open(t.TempDir()+"/refused.db", store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		sched := scheduler.New(scheduler.Config{GlobalLimit: 1, PerServerLimit: 1, QueueLimit: 2, RoundBudget: 500 * time.Millisecond})
		r := New(st, sched, Config{Upstreams: []string{addr}, Network: "tcp", UpstreamTimeout: 100 * time.Millisecond}, NewMetrics(), nil)
		_, err = r.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		if errs.CodeOf(err) != errs.EUpstreamRefused {
			t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
		}
	})
}

func TestZoneCommitDuringUpstreamDiscardsStaleCache(t *testing.T) {
	stack := newTestStack(t, authserver.Normal, "udp", 150*time.Millisecond, false)
	respCh := make(chan *dnsmsg.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := stack.res.Resolve(context.Background(), dnsmsg.NewQuery(1, "www.example.test.", dnsmsg.TypeA))
		respCh <- resp
		errCh <- err
	}()
	waitForAuthQuery(t, stack.auth, 1)
	delta := store.ZoneDelta{
		DeltaID: "race-delta", Zone: "example.test.", BaseVersion: 1, Version: 2,
		GeneratedAtMS: 1767225602000,
		Changes: []store.RRsetChange{{
			Owner: "www.example.test.", Type: dnsmsg.TypeA, TTL: 30, Records: []dnsmsg.RData{{Address: "192.0.2.11"}},
		}},
	}
	delta.Checksum = store.ComputeDeltaChecksum(delta)
	if _, err := stack.st.SubmitDelta(context.Background(), delta, "race-batch"); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	resp := <-respCh
	if !hasAnswerType(resp.Answers, dnsmsg.TypeA) || resp.Answers[0].Data.Address != "192.0.2.10" {
		t.Fatalf("in-flight response changed: %+v", resp.Answers)
	}
	if hit, err := stack.st.Lookup(context.Background(), "www.example.test.", dnsmsg.TypeA); err != nil || hit.Hit {
		t.Fatalf("stale in-flight answer was cached: hit=%+v err=%v", hit, err)
	}
	if err := stack.auth.Update(testSnapshot(2, "192.0.2.11")); err != nil {
		t.Fatal(err)
	}
	second := resolve(t, stack.res, "www.example.test.", dnsmsg.TypeA)
	if second.Answers[0].Data.Address != "192.0.2.11" {
		t.Fatalf("new authority data not returned: %+v", second.Answers)
	}
	hit, err := stack.st.Lookup(context.Background(), "www.example.test.", dnsmsg.TypeA)
	if err != nil || !hit.Hit || hit.Entry.ZoneVersion != 2 {
		t.Fatalf("new cache entry=%+v err=%v", hit, err)
	}
}

func TestUDPAndTCPListenersAndMalformedResponses(t *testing.T) {
	stack := newTestStack(t, authserver.Normal, "udp", 0, true)
	for _, network := range []string{"udp", "tcp"} {
		addr := stack.server.UDPAddr()
		if network == "tcp" {
			addr = stack.server.TCPAddr()
		}
		resp := exchange(t, network, addr, dnsmsg.NewQuery(0x1234, "www.example.test.", dnsmsg.TypeA))
		if resp.Header.ID != 0x1234 || !hasAnswerType(resp.Answers, dnsmsg.TypeA) {
			t.Fatalf("%s response=%+v", network, resp)
		}
	}

	unsupported := exchange(t, "udp", stack.server.UDPAddr(), dnsmsg.NewQuery(0x4321, "www.example.test.", dnsmsg.Type(99)))
	if unsupported.Header.RCode() != dnsmsg.RCodeNotImp || edeCode(unsupported) != string(errs.EUnsupportedType) {
		t.Fatalf("unsupported rcode=%d ede=%q", unsupported.Header.RCode(), edeCode(unsupported))
	}

	bad := []byte{0, 9, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0xc0, 0x0c, 0, 1, 0, 1}
	resp := exchangeBytes(t, "udp", stack.server.UDPAddr(), bad)
	if resp.Header.RCode() != dnsmsg.RCodeFormErr {
		t.Fatalf("malformed rcode=%d", resp.Header.RCode())
	}
	if code := edeCode(resp); code != string(errs.EMessageMalformed) {
		t.Fatalf("EDE code=%q", code)
	}

	before := stack.metrics.TCPFrameBad.Load()
	conn, err := net.Dial("tcp", stack.server.TCPAddr())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte{0, 10, 1, 2, 3})
	_ = conn.Close()
	waitForMetric(t, &stack.metrics.TCPFrameBad, before+1)
}

func exchange(t *testing.T, network, addr string, q *dnsmsg.Message) *dnsmsg.Message {
	t.Helper()
	packet, err := q.Pack()
	if err != nil {
		t.Fatal(err)
	}
	return exchangeBytes(t, network, addr, packet)
}

func exchangeBytes(t *testing.T, network, addr string, packet []byte) *dnsmsg.Message {
	t.Helper()
	conn, err := net.DialTimeout(network, addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	var resp []byte
	if network == "tcp" {
		if _, err := conn.Write([]byte{byte(len(packet) >> 8), byte(len(packet))}); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
		resp, err = dnsmsg.ReadTCP(conn)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 65535)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		resp = buf[:n]
	}
	msg, err := dnsmsg.Parse(resp)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func edeCode(msg *dnsmsg.Message) string {
	for _, rr := range msg.Additionals {
		if rr.Type != dnsmsg.TypeOPT {
			continue
		}
		raw := rr.Data.Raw
		for pos := 0; pos+4 <= len(raw); {
			code := binary.BigEndian.Uint16(raw[pos : pos+2])
			l := int(binary.BigEndian.Uint16(raw[pos+2 : pos+4]))
			pos += 4
			if pos+l > len(raw) {
				return ""
			}
			text := string(raw[pos : pos+l])
			pos += l
			if code == 15 {
				if idx := strings.IndexByte(text, ':'); idx >= 0 {
					return text[:idx]
				}
				return text
			}
		}
	}
	return ""
}

func waitForStats(t *testing.T, s *scheduler.Scheduler, cond func(scheduler.Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond(s.Stats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("scheduler stats condition not met")
}

func waitForAuthQuery(t *testing.T, s *authserver.Server, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.QueryCount() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("authoritative server did not receive query")
}

func waitForMetric(t *testing.T, v *atomic.Uint64, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if v.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("metric=%d want>=%d", v.Load(), want)
}

func TestEachCNAMEHopGetsIndependentUpstreamTimeout(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		buf := make([]byte, 4096)
		for i := 0; i < 2; i++ {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			q, err := dnsmsg.Parse(buf[:n])
			if err != nil || len(q.Questions) != 1 {
				return
			}
			time.Sleep(70 * time.Millisecond)
			resp := dnsmsg.NewResponse(q, dnsmsg.RCodeSuccess)
			question := q.Questions[0]
			switch dnsmsg.CanonicalName(question.Name) {
			case "alias.example.test.":
				resp.Answers = []dnsmsg.RR{{Name: "alias.example.test.", Type: dnsmsg.TypeCNAME, Class: 1, TTL: 60, Data: dnsmsg.RData{Target: "www.example.test."}}}
			case "www.example.test.":
				resp.Answers = []dnsmsg.RR{{Name: "www.example.test.", Type: dnsmsg.TypeA, Class: 1, TTL: 60, Data: dnsmsg.RData{Address: "192.0.2.10"}}}
			default:
				resp.Header.Flags = (resp.Header.Flags &^ 0x000f) | uint16(dnsmsg.RCodeNXDomain)
			}
			b, _ := resp.Pack()
			_, _ = pc.WriteTo(b, addr)
		}
	}()
	st, err := store.Open(t.TempDir()+"/cname-hops.db", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sched := scheduler.New(scheduler.Config{GlobalLimit: 2, PerServerLimit: 2, QueueLimit: 4, RoundBudget: 500 * time.Millisecond})
	r := New(st, sched, Config{Upstreams: []string{pc.LocalAddr().String()}, Network: "udp", UpstreamTimeout: 120 * time.Millisecond}, NewMetrics(), nil)
	start := time.Now()
	resp, err := r.Resolve(context.Background(), dnsmsg.NewQuery(1, "alias.example.test.", dnsmsg.TypeA))
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 130*time.Millisecond {
		t.Fatalf("CNAME chain was too fast to exercise two hops: %s", time.Since(start))
	}
	if !hasAnswerType(resp.Answers, dnsmsg.TypeCNAME) || !hasAnswerType(resp.Answers, dnsmsg.TypeA) {
		t.Fatalf("combined answers=%+v", resp.Answers)
	}
	<-serverDone
}
