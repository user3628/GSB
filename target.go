package convergence

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// LocalTarget is the deterministic loopback write/read endpoint used by samples and tests.
type LocalTarget struct {
	mu        sync.Mutex
	records   map[string]map[string]targetRecord
	delays    map[string]time.Duration
	mismatch  map[string]bool
	requestID int
	putCount  atomic.Int64
}

type targetRecord struct {
	ActualFields    FieldSet `json:"actual_fields"`
	ResourceVersion string   `json:"resource_version"`
	UpdatedAt       string   `json:"updated_at"`
}

func NewLocalTarget() *LocalTarget {
	return &LocalTarget{
		records:  map[string]map[string]targetRecord{},
		delays:   map[string]time.Duration{},
		mismatch: map[string]bool{},
	}
}

func (t *LocalTarget) SeedSnapshot(snapshot *Snapshot) {
	if snapshot == nil {
		return
	}
	for _, object := range snapshot.Objects {
		t.Seed(object.ClusterID, object.ID, object.Actual)
	}
}

func (t *LocalTarget) Seed(clusterID, objectID string, fields FieldSet) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.records[clusterID] == nil {
		t.records[clusterID] = map[string]targetRecord{}
	}
	t.records[clusterID][objectID] = targetRecord{ActualFields: fields, ResourceVersion: "seed-1", UpdatedAt: "deterministic"}
}

func (t *LocalTarget) SetDelay(objectID string, delay time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delays[objectID] = delay
}

func (t *LocalTarget) SetMismatch(objectID string, enabled bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mismatch[objectID] = enabled
}

func (t *LocalTarget) PutCount() int64 { return t.putCount.Load() }

func (t *LocalTarget) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/clusters/{cluster}/objects/{id}", t.handlePut)
	mux.HandleFunc("GET /api/v1/clusters/{cluster}/objects/{id}", t.handleGet)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func (t *LocalTarget) handlePut(w http.ResponseWriter, r *http.Request) {
	t.putCount.Add(1)
	clusterID := r.PathValue("cluster")
	objectID := r.PathValue("id")

	t.mu.Lock()
	delay := t.delays[objectID]
	mismatch := t.mismatch[objectID]
	t.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}

	var request writeRequestBody
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": CodeSpecInvalid, "error": err.Error()})
		return
	}

	fields := request.ExpectedFields
	if mismatch {
		if fields == nil {
			fields = FieldSet{}
		}
		fields["readback_marker"] = Field{Value: "injected-mismatch", Source: "fault-injection"}
	}

	t.mu.Lock()
	if t.records[clusterID] == nil {
		t.records[clusterID] = map[string]targetRecord{}
	}
	t.requestID++
	version := request.ResourceVersion
	if version == "" {
		version = "write-" + itoa(t.requestID)
	}
	t.records[clusterID][objectID] = targetRecord{ActualFields: fields, ResourceVersion: version, UpdatedAt: "loopback"}
	t.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "object_id": objectID, "resource_version": version})
}

func (t *LocalTarget) handleGet(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster")
	objectID := r.PathValue("id")
	t.mu.Lock()
	record, ok := t.records[clusterID][objectID]
	t.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"code": CodeNotFound, "message": "object not found on target"})
		return
	}
	writeJSON(w, http.StatusOK, readbackResponseBody{
		ObjectID:     objectID,
		ClusterID:    clusterID,
		ActualFields: record.ActualFields,
		ResourceVer:  record.ResourceVersion,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
