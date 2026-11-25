package edgeconfig

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func NewHTTPHandler(service *Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("GET /v1/version", func(w http.ResponseWriter, r *http.Request) {
		version, err := service.CurrentVersion(r.Context())
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"current_version": version})
	})
	mux.HandleFunc("POST /v1/topology", func(w http.ResponseWriter, r *http.Request) {
		var topology Topology
		if !decodeRequest(w, r, &topology) {
			return
		}
		result, err := service.SaveTopology(r.Context(), topology)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /v1/snapshots", func(w http.ResponseWriter, r *http.Request) {
		var snapshot Snapshot
		if !decodeRequest(w, r, &snapshot) {
			return
		}
		result, err := service.SubmitSnapshot(r.Context(), snapshot)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /v1/increments", func(w http.ResponseWriter, r *http.Request) {
		var increment Increment
		if !decodeRequest(w, r, &increment) {
			return
		}
		result, err := service.SubmitIncrement(r.Context(), increment)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /v1/affected/recompute", func(w http.ResponseWriter, r *http.Request) {
		var request AffectedRequest
		if !decodeRequest(w, r, &request) {
			return
		}
		result, err := service.RecomputeAffected(r.Context(), request)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("GET /v1/affected/{version}", func(w http.ResponseWriter, r *http.Request) {
		version, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
		if err != nil {
			WriteJSONError(w, NewAPIError(http.StatusBadRequest, CodeValidation, "invalid version", nil))
			return
		}
		result, err := service.GetAffectedReport(r.Context(), version)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /v1/reports", func(w http.ResponseWriter, r *http.Request) {
		var report Report
		if !decodeRequest(w, r, &report) {
			return
		}
		result, err := service.SubmitReport(r.Context(), report)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /v1/probe-rounds", func(w http.ResponseWriter, r *http.Request) {
		var request ProbeRequest
		if !decodeRequest(w, r, &request) {
			return
		}
		result, err := service.RunProbeRound(r.Context(), request)
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("GET /v1/convergence", func(w http.ResponseWriter, r *http.Request) {
		views, err := service.ListConvergence(r.Context())
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"nodes": views})
	})
	mux.HandleFunc("GET /v1/consistency", func(w http.ResponseWriter, r *http.Request) {
		consistency, err := service.GetConsistency(r.Context(), r.URL.Query().Get("scope"))
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, consistency)
	})
	mux.HandleFunc("GET /v1/consistency/scopes", func(w http.ResponseWriter, r *http.Request) {
		scopes, err := service.ListConsistencyScopes(r.Context())
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"scopes": scopes})
	})
	mux.HandleFunc("GET /v1/intents", func(w http.ResponseWriter, r *http.Request) {
		intents, err := service.ListIntents(r.Context(), r.URL.Query().Get("status"))
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"intents": intents})
	})
	mux.HandleFunc("POST /v1/recovery", func(w http.ResponseWriter, r *http.Request) {
		report, err := service.Recover(r.Context())
		if err != nil {
			WriteJSONError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
	})
	return mux
}

func decodeRequest(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
	if err := decoder.Decode(target); err != nil {
		WriteJSONError(w, NewAPIError(http.StatusBadRequest, CodeInvalidJSON, "request body is not valid JSON", err.Error()))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
