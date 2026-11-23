package smuggle

import (
	"fmt"
	"strings"
	"testing"
)

func adjudicateBothPaths(t *testing.T, proxy, backend string, maxReq int) (Adjudication, Adjudication) {
	t.Helper()
	p, _, _ := adjudicatePrecise([]byte(proxy), []byte(backend), maxReq)
	s, _, _ := adjudicateStreaming([]byte(proxy), []byte(backend), maxReq)
	return p, s
}

func TestAdjudicateConsistentDespiteHeaderOrder(t *testing.T) {
	proxy := "GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"
	backend := "GET / HTTP/1.1\r\ncontent-length: 5\r\nHost: x\r\n\r\nhello"
	p, s := adjudicateBothPaths(t, proxy, backend, 100)
	for _, adj := range []Adjudication{p, s} {
		if adj.Verdict != VerdictConsistent || adj.Code != CodeOK || adj.Offset != -1 {
			t.Fatalf("got %+v", adj)
		}
	}
}

func TestAdjudicateCountMismatch(t *testing.T) {
	proxy := "POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"
	backend := proxy + "GET /x HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
	p, s := adjudicateBothPaths(t, proxy, backend, 100)
	for _, adj := range []Adjudication{p, s} {
		if adj.Verdict != VerdictCountMismatch || adj.Code != CodeSmuggleAmbiguity {
			t.Fatalf("got %+v", adj)
		}
		if adj.Offset != len(proxy) {
			t.Fatalf("offset=%d want %d", adj.Offset, len(proxy))
		}
	}
}

func TestAdjudicateBoundaryMismatch(t *testing.T) {
	proxy := "POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"
	backend := "POST / HTTP/1.1\r\nContent-Length: 8\r\n\r\nhelloXYZ"
	p, s := adjudicateBothPaths(t, proxy, backend, 100)
	for _, adj := range []Adjudication{p, s} {
		if adj.Verdict != VerdictBoundaryMismatch || adj.Code != CodeSmuggleAmbiguity {
			t.Fatalf("got %+v", adj)
		}
		if adj.Offset != len("POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello") {
			t.Fatalf("offset=%d", adj.Offset)
		}
	}
}

func TestAdjudicateEncodingAmbiguity(t *testing.T) {
	raw := "POST / HTTP/1.1\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
	p, s := adjudicateBothPaths(t, raw, raw, 100)
	for _, adj := range []Adjudication{p, s} {
		if adj.Verdict != VerdictEncodingAmbiguity || adj.Code != CodeSmuggleAmbiguity {
			t.Fatalf("got %+v", adj)
		}
		if adj.Field != "content-length" || adj.Offset != 0 {
			t.Fatalf("offset=%d field=%s", adj.Offset, adj.Field)
		}
	}
}

// TestDualPathEquivalence 对确定性语料验证两条路径逐字节一致。
func TestDualPathEquivalence(t *testing.T) {
	var corpus [][2]string
	corpus = append(corpus,
		[2]string{"GET / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello", "GET / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"},
		[2]string{"POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 9\r\n\r\nhello", "POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello"},
		[2]string{"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n000A\r\n0123456789\r\n0\r\n\r\n", "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nA\r\n0123456789\r\n0\r\n\r\n"},
		[2]string{"POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n", "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"},
	)
	// 大规模流水线连接，覆盖流式路径。
	var big strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&big, "GET /r%d HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\nz", i)
	}
	corpus = append(corpus, [2]string{big.String(), big.String()})
	var bigShift strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&bigShift, "GET /r%d HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nzz", i)
	}
	corpus = append(corpus, [2]string{big.String(), bigShift.String()})

	for i, c := range corpus {
		p, s := adjudicateBothPaths(t, c[0], c[1], 100000)
		if p != s {
			t.Fatalf("case %d: precise=%+v stream=%+v", i, p, s)
		}
	}
}

func TestSelectPathDeterministic(t *testing.T) {
	one := "GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
	path, est := SelectPath([]byte(one), []byte(one), 2)
	if path != PathPrecise || est != 1 {
		t.Fatalf("got %s %d", path, est)
	}
	path, _ = SelectPath([]byte(one+one+one), []byte(one), 2)
	if path != PathStream {
		t.Fatalf("got %s", path)
	}
}
