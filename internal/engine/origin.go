package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
)

type OriginSource struct {
	Root     string
	TagsFile string
}

type tagMap map[string][]string

func (o *OriginSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if delay, _ := strconv.ParseInt(r.Header.Get("X-Lab-Delay-Ms"), 10, 64); delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	if status, _ := strconv.Atoi(r.Header.Get("X-Lab-Status")); status >= 100 && status <= 599 {
		http.Error(w, fmt.Sprintf("injected origin status %d", status), status)
		return
	}
	rel, err := safeOriginPath(r.URL.Path)
	if err != nil {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	full := filepath.Join(o.Root, filepath.FromSlash(rel))
	body, err := os.ReadFile(full)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "origin object not found", http.StatusNotFound)
			return
		}
		http.Error(w, "origin read failure", http.StatusInternalServerError)
		return
	}
	sum := sha256.Sum256(body)
	etag := `"sha256-` + hex.EncodeToString(sum[:]) + `"`
	contentType := mime.TypeByExtension(filepath.Ext(full))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	originalLength := len(body)
	switch strings.ToLower(r.Header.Get("X-Lab-Corrupt")) {
	case "truncate":
		n := parseIntDefault(r.Header.Get("X-Lab-Corrupt-Bytes"), 4)
		if n > len(body) {
			n = len(body)
		}
		body = body[:len(body)-n]
	case "grow":
		n := parseIntDefault(r.Header.Get("X-Lab-Corrupt-Bytes"), 4)
		body = append(append([]byte(nil), body...), []byte(strings.Repeat("X", n))...)
	}
	tags := o.tagsFor("/" + rel)
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Content-Version", strings.Trim(etag, `"`))
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Origin-Body-Length", strconv.Itoa(originalLength))
	w.Header().Set("X-Cache-Tags", strings.Join(tags, ","))
	w.Header().Set("X-Origin-Request-Id", r.Header.Get("X-Request-ID"))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func safeOriginPath(raw string) (string, error) {
	if strings.ContainsRune(raw, 0) {
		return "", errors.New("nul byte")
	}
	cleaned := path.Clean("/" + strings.TrimPrefix(raw, "/"))
	rel := strings.TrimPrefix(cleaned, "/")
	if rel == "" || rel == "." || strings.HasPrefix(rel, "../") || rel == ".." {
		return "", errors.New("empty or escaping path")
	}
	if filepath.IsAbs(rel) {
		return "", errors.New("absolute path")
	}
	return rel, nil
}

func parseIntDefault(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func (o *OriginSource) tagsFor(p string) []string {
	if o.TagsFile == "" {
		return nil
	}
	raw, err := os.ReadFile(o.TagsFile)
	if err != nil {
		return nil
	}
	var tags tagMap
	if err := json.Unmarshal(raw, &tags); err != nil {
		return nil
	}
	return tags[p]
}

type OriginResult struct {
	Body              []byte
	ETag              string
	ContentType       string
	Tags              []string
	Status            int
	DurationMs        int64
	FetchedAtMs       int64
	OriginID          string
	RequestBodyLength int64
}

type OriginClient struct {
	BaseURL    string
	RefusedURL string
	HTTP       *http.Client
	Clock      model.Clock
}

func NewOriginClient(baseURL, refusedURL string, clock model.Clock) *OriginClient {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       10 * time.Second,
		ResponseHeaderTimeout: 0,
	}
	return &OriginClient{
		BaseURL:    strings.TrimSuffix(baseURL, "/"),
		RefusedURL: strings.TrimSuffix(refusedURL, "/"),
		HTTP:       &http.Client{Transport: transport},
		Clock:      clock,
	}
}

func (c *OriginClient) Close() {
	if c == nil || c.HTTP == nil {
		return
	}
	if tr, ok := c.HTTP.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
}

func (c *OriginClient) Fetch(ctx context.Context, identity cache.Identity, headers http.Header, fault FaultSpec, timeout time.Duration) (*OriginResult, error) {
	start := c.Clock.Now()
	base := c.BaseURL
	if fault.Kind == "origin_refused" {
		base = c.RefusedURL
	}
	u, err := url.Parse(base + identity.NormPath)
	if err != nil {
		return nil, model.NewBiz(model.CodeKeyInvalid, "cannot construct origin URL")
	}
	u.RawQuery = identity.NormQuery
	reqCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, model.NewBiz(model.CodeInternal, err.Error())
	}
	for name, values := range headers {
		if strings.EqualFold(name, "Host") || strings.EqualFold(name, "Content-Length") {
			continue
		}
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	if fault.Kind == "origin_timeout" || fault.Kind == "origin_delay" {
		delay := fault.DelayMs
		if delay == 0 {
			delay = 250
		}
		req.Header.Set("X-Lab-Delay-Ms", strconv.FormatInt(delay, 10))
	}
	if fault.Kind == "origin_status" {
		req.Header.Set("X-Lab-Status", strconv.Itoa(fault.Status))
	}
	if fault.Kind == "body_corrupt" {
		req.Header.Set("X-Lab-Corrupt", fault.Corrupt)
		req.Header.Set("X-Lab-Corrupt-Bytes", strconv.Itoa(fault.CorruptBytes))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if isTimeoutErr(err) || errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			return nil, model.WithDetails(model.NewBiz(model.CodeOriginTimeout, "origin request exceeded its real local timeout"), map[string]any{
				"timeout_ms": timeout.Milliseconds(),
				"origin_url": u.Redacted(),
			})
		}
		if isRefusedErr(err) {
			return nil, model.WithDetails(model.NewBiz(model.CodeOriginRefused, "local origin refused the TCP connection"), map[string]any{
				"origin_url": u.Redacted(),
			})
		}
		if errors.Is(reqCtx.Err(), context.Canceled) {
			return nil, reqCtx.Err()
		}
		return nil, model.WithDetails(model.NewBiz(model.CodeOriginRefused, "origin transport failure"), map[string]any{
			"error": err.Error(),
		})
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		if isTimeoutErr(err) || errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			return nil, model.NewBiz(model.CodeOriginTimeout, "origin response body read timed out")
		}
		return nil, model.WithDetails(model.NewBiz(model.CodeBodyCorrupt, "failed reading origin body"), map[string]any{"error": err.Error()})
	}
	if resp.StatusCode != http.StatusOK {
		return nil, model.WithDetails(model.NewBiz(model.CodeOriginStatus, "origin returned a non-200 status"), map[string]any{
			"status": resp.StatusCode,
			"body":   truncateString(string(body), 256),
		})
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	if etag == "" {
		return nil, model.NewBiz(model.CodeBodyCorrupt, "origin response is missing ETag/content version")
	}
	expected := digestFromETag(etag)
	if expected == "" {
		return nil, model.WithDetails(model.NewBiz(model.CodeBodyCorrupt, "origin ETag is not a sha256 content version"), map[string]any{"etag": etag})
	}
	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	if !strings.EqualFold(expected, actual) {
		return nil, model.WithDetails(model.NewBiz(model.CodeBodyCorrupt, "origin body digest does not match ETag"), map[string]any{
			"etag":            etag,
			"expected_sha256": expected,
			"actual_sha256":   actual,
			"body_length":     len(body),
			"header_length":   resp.Header.Get("X-Origin-Body-Length"),
		})
	}
	tags := splitTags(resp.Header.Get("X-Cache-Tags"))
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	end := c.Clock.Now()
	return &OriginResult{
		Body: body, ETag: etag, ContentType: ct, Tags: tags, Status: resp.StatusCode,
		DurationMs: end.Sub(start).Milliseconds(), FetchedAtMs: end.UnixMilli(), OriginID: "origin-main",
		RequestBodyLength: int64(len(body)),
	}, nil
}

func digestFromETag(etag string) string {
	e := strings.TrimSpace(etag)
	e = strings.TrimPrefix(e, "W/")
	e = strings.Trim(e, `"`)
	e = strings.TrimPrefix(e, "sha256-")
	if len(e) != 64 {
		return ""
	}
	for i := range e {
		c := e[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return ""
		}
	}
	return strings.ToLower(e)
}

func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "timeout")
}

func isRefusedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") || strings.Contains(msg, "actively refused") || strings.Contains(msg, "no connection could be made")
}

func splitTags(s string) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range strings.Split(s, ",") {
		t = strings.TrimSpace(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
