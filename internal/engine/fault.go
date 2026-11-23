package engine

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"cdn-edge-consistency/internal/model"
)

type FaultSpec struct {
	Kind         string `json:"kind,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	DelayMs      int64  `json:"delay_ms,omitempty"`
	Status       int    `json:"status,omitempty"`
	Corrupt      string `json:"corrupt,omitempty"`
	CorruptBytes int    `json:"corrupt_bytes,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
}

func (f FaultSpec) Applies(nodeID string) bool {
	return f.Kind != "" && (f.NodeID == "" || f.NodeID == nodeID)
}

var controlKeys = map[string]bool{
	"__fault": true, "__fault_node": true, "__delay_ms": true, "__status": true,
	"__corrupt": true, "__corrupt_bytes": true, "__request_id": true,
}

func ExtractControls(r *http.Request, nodeID string) (FaultSpec, string, error) {
	var f FaultSpec
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return f, "", model.WithDetails(model.NewBiz(model.CodeKeyInvalid, "malformed query string"), map[string]any{"query": r.URL.RawQuery})
	}
	f.Kind = firstNonEmpty(values.Get("__fault"), r.Header.Get("X-Lab-Fault"))
	f.NodeID = firstNonEmpty(values.Get("__fault_node"), r.Header.Get("X-Lab-Fault-Node"))
	f.RequestID = firstNonEmpty(values.Get("__request_id"), r.Header.Get("X-Request-ID"), r.Header.Get("Idempotency-Key"))
	if v := firstNonEmpty(values.Get("__delay_ms"), r.Header.Get("X-Lab-Delay-Ms")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 || n > 60000 {
			return f, "", model.NewBiz(model.CodeBadRequest, "__delay_ms must be an integer in [0,60000]")
		}
		f.DelayMs = n
	}
	if v := firstNonEmpty(values.Get("__status"), r.Header.Get("X-Lab-Status")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 100 || n > 599 {
			return f, "", model.NewBiz(model.CodeBadRequest, "__status must be an HTTP status in [100,599]")
		}
		f.Status = n
	}
	f.Corrupt = strings.ToLower(firstNonEmpty(values.Get("__corrupt"), r.Header.Get("X-Lab-Corrupt")))
	if f.Corrupt != "" && f.Corrupt != "truncate" && f.Corrupt != "grow" {
		return f, "", model.NewBiz(model.CodeBadRequest, "__corrupt must be truncate or grow")
	}
	if v := firstNonEmpty(values.Get("__corrupt_bytes"), r.Header.Get("X-Lab-Corrupt-Bytes")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 1048576 {
			return f, "", model.NewBiz(model.CodeBadRequest, "__corrupt_bytes must be an integer in [0,1048576]")
		}
		f.CorruptBytes = n
	}
	switch f.Kind {
	case "", "origin_timeout", "timeout", "origin_delay", "delay", "origin_status", "status", "body_corrupt", "corrupt", "origin_refused", "refused", "purge_interrupt":
	default:
		return f, "", model.WithDetails(model.NewBiz(model.CodeBadRequest, "unknown fault kind"), map[string]any{"fault": f.Kind})
	}
	f.Kind = normalizeFaultKind(f.Kind)
	if f.Kind == "origin_timeout" && f.DelayMs == 0 {
		f.DelayMs = 2000
	}
	if f.Kind == "origin_status" && f.Status == 0 {
		f.Status = 503
	}
	if f.Kind == "body_corrupt" {
		if f.Corrupt == "" {
			f.Corrupt = "truncate"
		}
		if f.CorruptBytes == 0 {
			f.CorruptBytes = 4
		}
	}
	clean := stripControlQuery(r.URL.RawQuery)
	if f.RequestID == "" {
		f.RequestID = requestIDFromQuery(r.URL.RawQuery)
	}
	return f, clean, nil
}

func normalizeFaultKind(k string) string {
	switch k {
	case "timeout":
		return "origin_timeout"
	case "delay":
		return "origin_delay"
	case "status":
		return "origin_status"
	case "corrupt":
		return "body_corrupt"
	case "refused":
		return "origin_refused"
	default:
		return k
	}
}

func stripControlQuery(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "&")
	out := parts[:0]
	for _, p := range parts {
		if p == "" {
			continue
		}
		k, _, _ := strings.Cut(p, "=")
		if dk, err := url.QueryUnescape(k); err == nil {
			k = dk
		}
		if controlKeys[k] || strings.HasPrefix(k, "__") {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, "&")
}

func requestIDFromQuery(raw string) string {
	values, _ := url.ParseQuery(raw)
	return values.Get("__request_id")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
