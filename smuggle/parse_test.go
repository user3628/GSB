package smuggle

import "testing"

func parseOne(t *testing.T, raw string) View {
	t.Helper()
	return ParseView([]byte(raw), 100)
}

func TestParseNormalizesHeaders(t *testing.T) {
	v := parseOne(t, "GET / HTTP/1.1\r\ncontent-LENGTH:   5  \r\nHOST: x\r\nX-Fold:\r\n\tjoined\r\n\r\nhello")
	if v.Code != CodeOK || v.Requests != 1 {
		t.Fatalf("got code=%s requests=%d", v.Code, v.Requests)
	}
	b := v.Boundaries[0]
	if b.End != len("GET / HTTP/1.1\r\ncontent-LENGTH:   5  \r\nHOST: x\r\nX-Fold:\r\n\tjoined\r\n\r\nhello") {
		t.Fatalf("bad end %d", b.End)
	}
}

func TestParseDuplicateCLConsistent(t *testing.T) {
	v := parseOne(t, "GET / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5, 5\r\n\r\nhello")
	if v.Code != CodeOK || v.Requests != 1 {
		t.Fatalf("got code=%s requests=%d", v.Code, v.Requests)
	}
}

func TestParseDuplicateCLConflict(t *testing.T) {
	v := parseOne(t, "GET / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 9\r\n\r\nhello")
	if v.Code != CodeCLConflict || v.CodeField != "content-length" {
		t.Fatalf("got code=%s field=%s", v.Code, v.CodeField)
	}
}

func TestParseCLTEConflict(t *testing.T) {
	v := parseOne(t, "POST / HTTP/1.1\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n")
	if v.Code != CodeCLTEConflict {
		t.Fatalf("got code=%s", v.Code)
	}
	if v.Requests != 1 || v.Boundaries[0].End != len("POST / HTTP/1.1\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n") {
		t.Fatalf("boundary not computed with TE precedence: %+v", v)
	}
}

func TestParseChunkLeadingZeroUpperHex(t *testing.T) {
	raw := "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n000A\r\n0123456789\r\n0\r\n\r\n"
	v := parseOne(t, raw)
	if v.Code != CodeOK || v.Requests != 1 || v.Boundaries[0].End != len(raw) {
		t.Fatalf("got %+v", v)
	}
}

func TestParseObsFoldChunkBoundary(t *testing.T) {
	v := parseOne(t, "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n 5\r\nworld\r\n0\r\n\r\n")
	if v.Code != CodeChunkObsFold {
		t.Fatalf("got code=%s", v.Code)
	}
}

func TestParseUnknownTE(t *testing.T) {
	v := parseOne(t, "POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n")
	if v.Code != CodeTEUnknown {
		t.Fatalf("got code=%s", v.Code)
	}
}

func TestParseTENotFinal(t *testing.T) {
	v := parseOne(t, "POST / HTTP/1.1\r\nTransfer-Encoding: chunked, identity\r\n\r\n")
	if v.Code != CodeTEOrder {
		t.Fatalf("got code=%s", v.Code)
	}
}

func TestParseBodyTruncated(t *testing.T) {
	v := parseOne(t, "POST / HTTP/1.1\r\nContent-Length: 99\r\n\r\nshort")
	if v.Code != CodeBodyTruncated {
		t.Fatalf("got code=%s", v.Code)
	}
}

func TestParsePipelined(t *testing.T) {
	raw := "GET /a HTTP/1.1\r\nContent-Length: 2\r\n\r\nabGET /b HTTP/1.1\r\nContent-Length: 3\r\n\r\ncde"
	v := parseOne(t, raw)
	if v.Code != CodeOK || v.Requests != 2 {
		t.Fatalf("got %+v", v)
	}
	if v.Boundaries[1].Start != v.Boundaries[0].End {
		t.Fatalf("boundaries not contiguous: %+v", v.Boundaries)
	}
}

func TestParseRequestBudget(t *testing.T) {
	raw := "GET /a HTTP/1.1\r\nContent-Length: 0\r\n\r\nGET /b HTTP/1.1\r\nContent-Length: 0\r\n\r\n"
	v := ParseView([]byte(raw), 1)
	if v.Code != CodeRequestBudget || v.Requests != 1 {
		t.Fatalf("got code=%s requests=%d", v.Code, v.Requests)
	}
}
