// Package httpx exposes the engine over a loopback-only HTTP API for local
// demos and the verification tool. It never binds a non-loopback address.
package httpx

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"ticketpipeline/internal/engine"
	"ticketpipeline/internal/gen"
	"ticketpipeline/internal/model"
)

// Server wraps an engine with a loopback HTTP mux.
type Server struct {
	eng     *engine.Engine
	dataDir string
	outDir  string
	httpSrv *http.Server
}

// NewServer builds the server without starting it.
func NewServer(eng *engine.Engine, dataDir, outDir string) *Server {
	s := &Server{eng: eng, dataDir: dataDir, outDir: outDir}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/import", s.handleImport)
	mux.HandleFunc("/run", s.handleRun)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/report", s.handleReport)
	mux.HandleFunc("/change/policy", s.handlePolicyChange)
	mux.HandleFunc("/change/shard", s.handleShardChange)
	mux.HandleFunc("/stop", s.handleStop)
	mux.HandleFunc("/recover", s.handleRecover)
	s.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Start binds a loopback listener. If preferredPort is 0 or busy, an
// ephemeral loopback port is selected and reported through addr.
func (s *Server) Start(preferredPort int) (string, error) {
	addr := "127.0.0.1:" + itoa(preferredPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil && preferredPort != 0 {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return "", err
	}
	go func() { _ = s.httpSrv.Serve(ln) }()
	return ln.Addr().String(), nil
}

// Shutdown gracefully stops the HTTP listener.
func (s *Server) Shutdown(ctx context.Context) error { return s.httpSrv.Shutdown(ctx) }

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleImport(w http.ResponseWriter, _ *http.Request) {
	shards := gen.GenerateShards()
	for _, sh := range shards {
		if err := gen.WriteShard(s.dataDir, sh); err != nil {
			writeErr(w, err)
			return
		}
	}
	policy := gen.GeneratePolicy()
	if err := gen.WritePolicy(s.dataDir, policy); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"shards":         len(shards),
		"tickets":        countTickets(shards),
		"policy_version": policy.Version,
	})
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		CrashAt string `json:"crash_at"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	if req.CrashAt != "" {
		s.eng.SetCrashAt(req.CrashAt)
	}
	snap, err := s.eng.Run(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runSummary(snap))
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	status, err := engine.Status(s.eng.Store())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleReport(w http.ResponseWriter, _ *http.Request) {
	snap, err := s.eng.Run(context.Background())
	if err != nil {
		writeErr(w, err)
		return
	}
	if snap.Report == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "report not yet generated"})
		return
	}
	writeJSON(w, http.StatusOK, snap.Report)
}

func (s *Server) handlePolicyChange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var patch engine.PolicyPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeErr(w, err)
		return
	}
	p, err := engine.BumpPolicy(s.dataDir, patch)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy_version": p.Version})
}

func (s *Server) handleShardChange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		ShardID       string            `json:"shard_id"`
		Tickets       []model.RawTicket `json:"tickets"`
		ForceMismatch bool              `json:"force_mismatch"`
		PolicyVersion int               `json:"policy_version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, err)
		return
	}
	if err := engine.ReplaceShard(s.dataDir, req.ShardID, req.Tickets,
		req.ForceMismatch, req.PolicyVersion); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"shard_id": req.ShardID})
}

func (s *Server) handleStop(w http.ResponseWriter, _ *http.Request) {
	s.eng.RequestStop()
	writeJSON(w, http.StatusOK, map[string]string{"status": "stop_requested"})
}

func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	snap, err := s.eng.Run(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runSummary(snap))
}

func runSummary(snap *engine.Snapshot) map[string]any {
	out := map[string]any{
		"policy_version": snap.Policy.Version,
		"total_tickets":  len(snap.Tickets),
		"errors": map[string]int{
			"normalize": len(snap.NormErr),
			"intent":    len(snap.IntentErr),
			"priority":  len(snap.PrioErr),
		},
	}
	if snap.Report != nil {
		out["report_digest"] = snap.Report.Digest
		out["intent_accuracy"] = snap.Report.IntentAccuracy
		out["verdict_counts"] = snap.Report.VerdictCounts
	}
	return out
}

func countTickets(shards []model.Shard) int {
	n := 0
	for _, sh := range shards {
		n += len(sh.Tickets)
	}
	return n
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	body := map[string]string{"error": err.Error()}
	if be, ok := model.AsBizError(err); ok {
		body["code"] = be.Code
		body["stage"] = be.Stage
		body["ticket_id"] = be.TicketID
		code = http.StatusUnprocessableEntity
	}
	writeJSON(w, code, body)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
