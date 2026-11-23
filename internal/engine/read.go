package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"meshmigrate/internal/model"
	"meshmigrate/internal/store"
)

type SnapshotView struct {
	SnapshotVersion         uint64                `json:"snapshotVersion"`
	SnapshotID              string                `json:"snapshotId"`
	GeneratedAtMs           int64                 `json:"generatedAtMs"`
	ImportedAtMs            int64                 `json:"importedAtMs"`
	SourceFormat            string                `json:"sourceFormat"`
	FormatVersion           int                   `json:"formatVersion"`
	SourceDigest            string                `json:"sourceDigest"`
	ContentDigest           string                `json:"contentDigest"`
	PolicyCount             int                   `json:"policyCount"`
	CumulativeRoundingError int64                 `json:"cumulativeRoundingErrorUs"`
	DefaultsApplied         []model.DefaultDetail `json:"defaultsApplied,omitempty"`
}

type PolicyListResponse struct {
	SnapshotView
	ActiveVersion uint64         `json:"activeVersion"`
	Policies      []model.Policy `json:"policies"`
}

func snapshotView(m store.SnapshotMeta) SnapshotView {
	return SnapshotView{
		SnapshotVersion: m.SnapshotVersion, SnapshotID: m.SnapshotID, GeneratedAtMs: m.GeneratedAtMs,
		ImportedAtMs: m.ImportedAtMs, SourceFormat: m.SourceFormat, FormatVersion: m.FormatVersion,
		SourceDigest: m.SourceDigest, ContentDigest: m.ContentDigest, PolicyCount: m.PolicyCount,
		CumulativeRoundingError: m.CumulativeRoundingError, DefaultsApplied: m.DefaultDetails,
	}
}

func (e *Engine) Policies(ctx context.Context, version uint64, policyID string) ([]byte, *APIError) {
	var data *store.SnapshotData
	var activeVersion uint64
	var err error
	if version == 0 {
		data, activeVersion, err = e.store.GetActiveSnapshot(ctx)
		if err == nil && (data == nil || activeVersion == 0) {
			ae := newError(http.StatusNotFound, CodeNotFound, "no active completed snapshot")
			return ae.Envelope(), ae
		}
	} else {
		data, err = e.store.GetSnapshot(ctx, version)
		activeVersion, _ = e.store.ActiveVersion(ctx)
	}
	if errors.Is(err, store.ErrNotFound) {
		return e.retiredError(ctx, version)
	}
	if err != nil {
		ae := newError(http.StatusInternalServerError, CodeInternal, "read policy snapshot: "+err.Error())
		return ae.Envelope(), ae
	}
	if policyID != "" {
		for _, p := range data.Policies {
			if p.PolicyID == policyID {
				b, _ := json.Marshal(struct {
					SnapshotVersion uint64       `json:"snapshotVersion"`
					ActiveVersion   uint64       `json:"activeVersion"`
					Policy          model.Policy `json:"policy"`
				}{data.Meta.SnapshotVersion, activeVersion, p})
				return b, nil
			}
		}
		ae := newError(http.StatusNotFound, CodeNotFound, "policy not found in snapshot "+itoa(data.Meta.SnapshotVersion))
		ae.Details = map[string]any{"snapshotVersion": data.Meta.SnapshotVersion, "policyId": policyID}
		return ae.Envelope(), ae
	}
	resp := PolicyListResponse{SnapshotView: snapshotView(data.Meta), ActiveVersion: activeVersion, Policies: data.Policies}
	b, err := json.Marshal(resp)
	if err != nil {
		return internalBody(err)
	}
	return b, nil
}

type DerivedResponse struct {
	SnapshotVersion uint64                `json:"snapshotVersion"`
	RebuiltAtMs     int64                 `json:"rebuiltAtMs"`
	Affected        []string              `json:"affectedServices"`
	RouteIndex      []store.RouteRow      `json:"routeIndex"`
	RateWindows     []store.RateWindowRow `json:"rateWindows"`
	CircuitStates   []store.CircuitRow    `json:"circuitStates"`
	Summaries       []store.SummaryRow    `json:"summaries"`
}

func (e *Engine) Derived(ctx context.Context, version uint64) ([]byte, *APIError) {
	d, err := e.store.GetDerived(ctx, version)
	if errors.Is(err, store.ErrNotFound) {
		return e.retiredError(ctx, version)
	}
	if err != nil {
		ae := newError(http.StatusInternalServerError, CodeInternal, "read derived structures: "+err.Error())
		return ae.Envelope(), ae
	}
	resp := DerivedResponse{
		SnapshotVersion: d.SnapshotVersion, RebuiltAtMs: d.RebuiltAtMs, Affected: d.Affected,
		RouteIndex: d.Routes, RateWindows: d.RateWindows, CircuitStates: d.Circuits, Summaries: d.Summaries,
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return internalBody(err)
	}
	return b, nil
}

func (e *Engine) VersionList(ctx context.Context) ([]byte, *APIError) {
	versions, err := e.store.ListVersions(ctx)
	if err != nil {
		ae := newError(http.StatusInternalServerError, CodeInternal, "list versions: "+err.Error())
		return ae.Envelope(), ae
	}
	active, err := e.store.ActiveVersion(ctx)
	if err != nil {
		ae := newError(http.StatusInternalServerError, CodeInternal, "read active version: "+err.Error())
		return ae.Envelope(), ae
	}
	views := make([]SnapshotView, 0, len(versions))
	for _, v := range versions {
		views = append(views, snapshotView(v))
	}
	b, err := json.Marshal(struct {
		ActiveVersion uint64         `json:"activeVersion"`
		Versions      []SnapshotView `json:"versions"`
	}{active, views})
	if err != nil {
		return internalBody(err)
	}
	return b, nil
}

func (e *Engine) Report(ctx context.Context, reportID string) ([]byte, *APIError) {
	body, items, err := e.store.GetImportReport(ctx, reportID)
	if errors.Is(err, store.ErrNotFound) {
		ae := newError(http.StatusNotFound, CodeNotFound, "import report not found")
		ae.Details = map[string]any{"reportId": reportID}
		return ae.Envelope(), ae
	}
	if err != nil {
		ae := newError(http.StatusInternalServerError, CodeInternal, "read import report: "+err.Error())
		return ae.Envelope(), ae
	}
	// The stored report is the exact committed response. If an older report
	// omitted items, the normalized items table remains independently auditable.
	var probe map[string]any
	if json.Unmarshal(body, &probe) == nil {
		if _, exists := probe["items"]; !exists {
			probe["items"] = items
			if b, err := json.Marshal(probe); err == nil {
				return b, nil
			}
		}
	}
	return body, nil
}

func (e *Engine) RecoveryReport(ctx context.Context) ([]byte, *APIError) {
	report, err := e.store.LatestRecoveryReport(ctx)
	if errors.Is(err, store.ErrNotFound) {
		ae := newError(http.StatusNotFound, CodeNotFound, "no recovery report has been written")
		return ae.Envelope(), ae
	}
	if err != nil {
		ae := newError(http.StatusInternalServerError, CodeInternal, "read recovery report: "+err.Error())
		return ae.Envelope(), ae
	}
	b, err := json.Marshal(report)
	if err != nil {
		return internalBody(err)
	}
	return b, nil
}

func (e *Engine) Recover(ctx context.Context) (*store.RecoveryResult, error) {
	return e.store.RecoverPending(ctx)
}

func (e *Engine) retiredError(ctx context.Context, requested uint64) ([]byte, *APIError) {
	versions, _ := e.store.ListVersions(ctx)
	available := make([]uint64, 0, len(versions))
	for _, v := range versions {
		available = append(available, v.SnapshotVersion)
	}
	active, _ := e.store.ActiveVersion(ctx)
	ae := newError(http.StatusGone, CodeVersionRetired, "requested policy snapshot is not available; version binding cannot be satisfied")
	ae.Details = map[string]any{"requestedVersion": requested, "activeVersion": active, "availableVersions": available}
	return ae.Envelope(), ae
}

func internalBody(err error) ([]byte, *APIError) {
	ae := newError(http.StatusInternalServerError, CodeInternal, err.Error())
	return ae.Envelope(), ae
}

func itoa(v uint64) string {
	return fmtUint(v)
}
