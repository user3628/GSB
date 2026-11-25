package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"driftengine/internal/engine"
	"driftengine/internal/errs"
	"driftengine/internal/loopendpoint"
	"driftengine/internal/sample"
	"driftengine/internal/spec"
)

func initialEndpointStates() map[string]loopendpoint.ObjectState {
	var states map[string]loopendpoint.ObjectState
	if err := spec.DecodeJSON(sample.MustAsset("endpoint-state.json"), &states); err != nil {
		panic(err)
	}
	return states
}

func checkLoopbackConvergence(ctx context.Context, opts Options) (any, error) {
	dbPath := filepath.Join(opts.WorkDir, "converge.db")
	st, err := openStore(dbPath)
	if err != nil {
		return nil, err
	}
	defer st.Close()

	endpoint, err := loopendpoint.New("127.0.0.1:0", initialEndpointStates())
	if err != nil {
		return nil, err
	}
	endpoint.Start()
	defer endpoint.Close()

	eng := engine.New(st, endpoint.URL())
	if err := eng.Bootstrap(ctx); err != nil {
		return nil, err
	}
	snapshot, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	if _, err := eng.SubmitSnapshot(ctx, snapshot); err != nil {
		return nil, err
	}
	roundCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	result, err := eng.RunRound(roundCtx, engine.RoundOptions{Timeout: 15 * time.Second})
	if err != nil {
		return roundFailureDetail(eng, endpoint.URL(), result), err
	}
	view := eng.View()
	if view.Summary.VerifiedConverged != len(snapshot.Objects) {
		return roundFailureDetail(eng, endpoint.URL(), result), fmt.Errorf("only %d/%d objects converged", view.Summary.VerifiedConverged, len(snapshot.Objects))
	}
	state, ok := endpoint.Get("obj-value-offset")
	if !ok {
		return nil, fmt.Errorf("obj-value-offset missing from endpoint")
	}
	return map[string]any{
		"objects":            len(snapshot.Objects),
		"verified_converged": view.Summary.VerifiedConverged,
		"round_duration_ms":  result.DurationMS,
		"url":                endpoint.URL(),
		"value_readback":     state.Fields["ratio"],
	}, nil
}

func roundFailureDetail(eng *engine.Engine, url string, result *engine.RoundResult) map[string]any {
	detail := map[string]any{"url": url}
	if result != nil {
		detail["round_duration_ms"] = result.DurationMS
		detail["failed_objects"] = result.FailedObjects
	}
	if view := eng.View(); view != nil {
		failed := map[string]any{}
		for id, state := range view.ObjectStates {
			if state.Phase != engine.PhaseConverged && state.Phase != engine.PhaseObservedInSync {
				failed[id] = map[string]any{"phase": state.Phase, "code": state.LastErrorCode, "differences": len(state.Differences)}
			}
		}
		detail["states"] = failed
		detail["summary"] = view.Summary
	}
	return detail
}

func checkConcurrentIncrements(ctx context.Context, opts Options) (any, error) {
	st, err := openStore(filepath.Join(opts.WorkDir, "increment.db"))
	if err != nil {
		return nil, err
	}
	defer st.Close()
	eng := engine.New(st, "http://127.0.0.1:0")
	snapshot, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	if _, err := eng.SubmitSnapshot(ctx, snapshot); err != nil {
		return nil, err
	}
	incrementData, err := sample.Asset("increment-base-v1.json")
	if err != nil {
		return nil, err
	}
	type outcome struct {
		OK      bool   `json:"ok"`
		Version int64  `json:"version,omitempty"`
		Code    string `json:"code,omitempty"`
	}
	outcomes := make([]outcome, 2)
	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			view, submitErr := eng.SubmitIncrementData(ctx, incrementData)
			if submitErr == nil {
				outcomes[i] = outcome{OK: true, Version: view.Version}
			} else {
				outcomes[i] = outcome{OK: false, Code: errs.Code(submitErr)}
			}
			done <- struct{}{}
		}()
	}
	<-done
	<-done
	successes, stale := 0, 0
	for _, item := range outcomes {
		if item.OK {
			successes++
		}
		if item.Code == errs.StaleBase {
			stale++
		}
	}
	if successes != 1 || stale != 1 || eng.CurrentVersion() != 2 {
		return map[string]any{"outcomes": outcomes, "current_version": eng.CurrentVersion()},
			fmt.Errorf("expected one success and one E_STALE_BASE")
	}
	return map[string]any{"current_version": 2, "outcomes": outcomes}, nil
}

func checkInvalidSpec(ctx context.Context, opts Options) (any, error) {
	st, err := openStore(filepath.Join(opts.WorkDir, "invalid.db"))
	if err != nil {
		return nil, err
	}
	defer st.Close()
	eng := engine.New(st, "http://127.0.0.1:0")
	snapshot, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	if _, err := eng.SubmitSnapshot(ctx, snapshot); err != nil {
		return nil, err
	}
	bad, err := sample.StandardSnapshot()
	if err != nil {
		return nil, err
	}
	bad.Version = 2
	bad.Digest = ""
	bad.Objects = append(bad.Objects, spec.Object{
		ID: "bad-missing-ref", Category: "Bad", Namespace: "default",
		Desired: spec.Fields{"x": 1}, Actual: spec.Fields{"x": 1},
		Dependencies: []string{"does-not-exist"}, ResourceBudget: spec.Budget{Units: 1},
	})
	data, err := json.Marshal(bad)
	if err != nil {
		return nil, err
	}
	_, submitErr := eng.SubmitSnapshotData(ctx, data)
	if code := errs.Code(submitErr); code != errs.SpecInvalid {
		return map[string]any{"error": fmt.Sprint(submitErr)}, fmt.Errorf("expected E_SPEC_INVALID, got %s", code)
	}
	view := eng.View()
	if eng.CurrentVersion() != 1 || view == nil || len(view.Snapshot.Objects) != 24 {
		return map[string]any{"current_version": eng.CurrentVersion()}, fmt.Errorf("committed state changed after invalid spec")
	}
	return map[string]any{"current_version": 1, "error_code": errs.SpecInvalid, "objects": 24}, nil
}

var _ = filepath.Join
var _ = time.Second
