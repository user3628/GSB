package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/pipeline"
)

type Stats struct {
	Received      int    `json:"received"`
	Completed     int    `json:"completed"`
	Canceled      int    `json:"canceled"`
	DuplicateAcks int    `json:"duplicate_acks"`
	LastSeen      string `json:"last_seen"`
}

type Handler struct {
	doc   *pipeline.Document
	mu    sync.Mutex
	stats map[string]*Stats
}

func New(doc *pipeline.Document) *Handler {
	return &Handler{doc: doc, stats: map[string]*Stats{}}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "sample_id": h.doc.SampleID, "sample_digest": h.doc.Digest})
	})
	mux.HandleFunc("GET /stats", h.statsHandler)
	mux.HandleFunc("POST /push", h.pushHandler)
	return mux
}

func (h *Handler) statsHandler(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	copyStats := make(map[string]Stats, len(h.stats))
	for key, value := range h.stats {
		copyStats[key] = *value
	}
	writeJSON(w, http.StatusOK, map[string]any{"sample_id": h.doc.SampleID, "by_push": copyStats})
}

func (h *Handler) pushHandler(w http.ResponseWriter, r *http.Request) {
	behavior := r.URL.Query().Get("behavior")
	if behavior == "" {
		behavior = "success"
	}
	var envelope core.PushEnvelope
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "read failed"})
		return
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.PushID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid push envelope"})
		return
	}
	h.recordReceived(envelope.PushID)

	if behavior == "reject" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "rejected", "message": "injected endpoint rejection"})
		return
	}
	if behavior == "malformed" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
		return
	}
	if behavior == "delay" {
		delay := 1500 * time.Millisecond
		if raw := r.URL.Query().Get("delay_ms"); raw != "" {
			ms, parseErr := strconv.Atoi(raw)
			if parseErr == nil && ms > 0 {
				delay = time.Duration(ms) * time.Millisecond
			}
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			h.recordCanceled(envelope.PushID)
			return
		}
	}
	if behavior == "success" {
		if rawVersion := r.URL.Query().Get("delay_version"); rawVersion != "" {
			version, parseErr := strconv.ParseInt(rawVersion, 10, 64)
			if parseErr == nil && version == envelope.Version {
				delay := 300 * time.Millisecond
				if raw := r.URL.Query().Get("delay_ms"); raw != "" {
					if ms, parseErr := strconv.Atoi(raw); parseErr == nil && ms > 0 {
						delay = time.Duration(ms) * time.Millisecond
					}
				}
				select {
				case <-time.After(delay):
				case <-r.Context().Done():
					h.recordCanceled(envelope.PushID)
					return
				}
			}
		}
	}

	summary, err := pipeline.Compute(envelope.ClientID, envelope.Version, envelope.Config, h.doc)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
		return
	}
	ackVersion := envelope.Version
	if behavior == "wrong_version" || behavior == "version_mismatch" {
		ackVersion++
	}
	ackID := deterministicAckID(envelope.PushID, summary.ResultDigest)
	ack := core.Ack{
		AckID:         ackID,
		PushID:        envelope.PushID,
		ClientID:      envelope.ClientID,
		Version:       ackVersion,
		ContentDigest: envelope.ContentDigest,
		ResultDigest:  summary.ResultDigest,
		Result:        summary.AsMap(),
		IngressCount:  1,
		Source:        "client-sim",
		ReceivedAt:    core.Now(),
	}
	acks := []core.Ack{ack}
	if behavior == "duplicate" {
		acks = append(acks, ack)
		h.mu.Lock()
		h.stats[envelope.PushID].DuplicateAcks++
		h.mu.Unlock()
	}
	h.recordCompleted(envelope.PushID)
	writeJSON(w, http.StatusOK, core.ClientResponse{
		Status: "ok",
		Acks:   acks,
		Result: summary.AsMap(),
	})
}

func deterministicAckID(pushID, resultDigest string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{"ack-sim.v1", pushID, resultDigest}, "\n")))
	return "ack-" + hex.EncodeToString(sum[:12])
}

func (h *Handler) recordReceived(pushID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.stats[pushID]
	if s == nil {
		s = &Stats{}
		h.stats[pushID] = s
	}
	s.Received++
	s.LastSeen = core.Now()
}

func (h *Handler) recordCompleted(pushID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.stats[pushID]; s != nil {
		s.Completed++
	}
}

func (h *Handler) recordCanceled(pushID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.stats[pushID]; s != nil {
		s.Canceled++
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (h *Handler) String() string {
	return fmt.Sprintf("client-sim sample=%s digest=%s", h.doc.SampleID, h.doc.Digest)
}
