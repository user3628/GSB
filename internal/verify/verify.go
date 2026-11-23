package verify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"meshmigrate/internal/model"
	"meshmigrate/internal/samples"
)

type Verifier struct {
	base     string
	dir      string
	client   *http.Client
	checks   int
	failures []string
}

type NegotiationView struct {
	Decision          string                `json:"decision"`
	Format            string                `json:"format"`
	FormatVersion     int                   `json:"formatVersion"`
	SnapshotVersion   uint64                `json:"snapshotVersion"`
	PolicyCount       int                   `json:"policyCount"`
	ErrorCode         string                `json:"errorCode"`
	UnknownFields     []string              `json:"unknownFields"`
	ContentDigest     string                `json:"contentDigest"`
	DefaultsApplied   []model.DefaultDetail `json:"defaultsApplied"`
	RoundingRecords   []RoundingView        `json:"roundingRecords"`
	CumulativeErrorUs int64                 `json:"cumulativeRoundingErrorUs"`
}

type RoundingView struct {
	PolicyID             string `json:"policyId"`
	SourceTimestampUs    int64  `json:"sourceTimestampUs"`
	ConvertedTimestampMs int64  `json:"convertedTimestampMs"`
	RemainderUs          int64  `json:"remainderUs"`
	RoundingBit          int    `json:"roundingBit"`
	ErrorUs              int64  `json:"errorUs"`
	CumulativeErrorUs    int64  `json:"cumulativeErrorUs"`
}

type ImportResponse struct {
	RequestID        string                `json:"requestId"`
	ReportID         string                `json:"reportId"`
	Status           string                `json:"status"`
	SnapshotVersion  uint64                `json:"snapshotVersion"`
	ActiveVersion    uint64                `json:"activeVersion"`
	Decision         string                `json:"decision"`
	AffectedServices []string              `json:"affectedServices"`
	DefaultsApplied  []model.DefaultDetail `json:"defaultsApplied"`
	Items            []ReportItem          `json:"items"`
}

type Report struct {
	ReportID        string                `json:"reportId"`
	SnapshotVersion uint64                `json:"snapshotVersion"`
	ActiveVersion   uint64                `json:"activeVersion"`
	Status          string                `json:"status"`
	Items           []ReportItem          `json:"items"`
	DefaultsApplied []model.DefaultDetail `json:"defaultsApplied"`
}

type ReportItem struct {
	PolicyID string   `json:"policyId"`
	Outcome  string   `json:"outcome"`
	Basis    []string `json:"basis"`
}

type ErrorEnvelope struct {
	Error struct {
		Code          string   `json:"code"`
		Message       string   `json:"message"`
		ByteOffset    *int64   `json:"byteOffset"`
		UnknownFields []string `json:"unknownFields"`
		ReportID      string   `json:"reportId"`
	} `json:"error"`
}

type PolicyList struct {
	SnapshotVersion uint64         `json:"snapshotVersion"`
	SnapshotID      string         `json:"snapshotId"`
	ActiveVersion   uint64         `json:"activeVersion"`
	Policies        []model.Policy `json:"policies"`
}

type DerivedResponse struct {
	SnapshotVersion uint64   `json:"snapshotVersion"`
	Affected        []string `json:"affectedServices"`
	RouteIndex      []struct {
		PolicyID string `json:"policyId"`
	} `json:"routeIndex"`
	RateWindows []struct {
		PolicyIDs []string `json:"policyIds"`
	} `json:"rateWindows"`
	CircuitStates []struct {
		PolicyIDs []string `json:"policyIds"`
	} `json:"circuitStates"`
	Summaries []struct {
		PolicyIDs []string `json:"policyIds"`
	} `json:"summaries"`
}

type VersionList struct {
	ActiveVersion uint64 `json:"activeVersion"`
	Versions      []struct {
		SnapshotVersion uint64 `json:"snapshotVersion"`
	} `json:"versions"`
}

func Run(baseURL, sampleDir string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme != "http" {
		return "", fmt.Errorf("base URL must be a local http URL")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return "", fmt.Errorf("base URL %q is not a loopback endpoint", baseURL)
	}
	v := &Verifier{
		base:   strings.TrimRight(baseURL, "/"),
		dir:    sampleDir,
		client: &http.Client{Timeout: 20 * time.Second},
	}
	manifest, err := samples.LoadManifest(sampleDir)
	if err != nil {
		return "", err
	}
	v.run(manifest)
	summary := map[string]any{
		"checks":   v.checks,
		"failures": v.failures,
		"status":   "pass",
	}
	if len(v.failures) > 0 {
		summary["status"] = "fail"
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	if len(v.failures) > 0 {
		return string(b), fmt.Errorf("%d verification checks failed", len(v.failures))
	}
	return string(b), nil
}

func (v *Verifier) run(m *samples.Manifest) {
	status, body, err := v.request(http.MethodGet, "/healthz", nil, nil)
	v.check(err == nil && status == 200, "health endpoint", fmt.Sprintf("status=%d err=%v body=%s", status, err, preview(body)))
	if err != nil {
		return
	}
	versions := v.getVersions()
	v.check(versions.ActiveVersion == 0 && len(versions.Versions) == 0, "fresh sample database", fmt.Sprintf("active=%d versions=%d", versions.ActiveVersion, len(versions.Versions)))
	if len(versions.Versions) != 0 {
		v.fail("verification requires a fresh database; re-run with a new -db path")
		return
	}

	legacyBytes := v.readFile(m.Files.LegacyBinary)
	currentBytes := v.readFile(m.Files.CurrentJSONL)
	unsupportedBytes := v.readFile(m.Files.UnsupportedJSONL)
	duplicateBytes := v.readFile(m.Files.DuplicateJSONL)

	status, neg1a, err := v.request(http.MethodPost, "/v1/negotiate?format=auto", legacyBytes, nil)
	v.check(err == nil && status == 200, "legacy negotiation HTTP", fmt.Sprintf("status=%d err=%v", status, err))
	status, neg1b, _ := v.request(http.MethodPost, "/v1/negotiate?format=auto", legacyBytes, nil)
	v.check(bytes.Equal(neg1a, neg1b), "repeated negotiation is byte-identical", "")
	var n1 NegotiationView
	v.decode(neg1a, &n1, "legacy negotiation body")
	v.check(n1.Decision == m.Expectations.Legacy.Decision, "legacy decision downgrade_accept", n1.Decision)
	v.check(n1.Format == "binary" && n1.FormatVersion == 1, "legacy detected as binary v1", fmt.Sprintf("%s v%d", n1.Format, n1.FormatVersion))
	v.check(n1.PolicyCount == m.Expectations.Legacy.PolicyCount, "legacy policy count", fmt.Sprintf("%d", n1.PolicyCount))
	v.check(len(n1.DefaultsApplied) > 0, "legacy default details present", fmt.Sprintf("%d defaults", len(n1.DefaultsApplied)))
	v.check(n1.ContentDigest != "", "legacy normalized content digest present", n1.ContentDigest)

	status, neg2Bytes, err := v.request(http.MethodPost, "/v1/negotiate?format=auto&truncation=floor", currentBytes, nil)
	v.check(err == nil && status == 200, "current negotiation HTTP", fmt.Sprintf("status=%d err=%v", status, err))
	var n2 NegotiationView
	v.decode(neg2Bytes, &n2, "current negotiation body")
	v.check(n2.Decision == m.Expectations.Current.Decision, "current decision accept", n2.Decision)
	v.check(n2.Format == "jsonl" && n2.FormatVersion == 2, "current detected as JSONL v2", fmt.Sprintf("%s v%d", n2.Format, n2.FormatVersion))
	v.check(roundingFor(n2.RoundingRecords, "checkout-rate").RemainderUs == 456, "microsecond rounding remainder 456", fmt.Sprintf("%+v", roundingFor(n2.RoundingRecords, "checkout-rate")))
	v.check(n2.CumulativeErrorUs != 0, "cumulative rounding error recorded", fmt.Sprintf("%d", n2.CumulativeErrorUs))

	status, neg3Bytes, err := v.request(http.MethodPost, "/v1/negotiate?format=auto", unsupportedBytes, nil)
	v.check(err == nil && status == 200, "unsupported negotiation HTTP", fmt.Sprintf("status=%d err=%v", status, err))
	var n3 NegotiationView
	v.decode(neg3Bytes, &n3, "unsupported negotiation body")
	v.check(n3.Decision == m.Expectations.Unsupported.Decision, "unsupported decision reject", n3.Decision)
	v.check(containsAll(n3.UnknownFields, m.Expectations.Unsupported.UnknownFields), "unsupported unknown required field names", strings.Join(n3.UnknownFields, ","))

	for _, c := range []struct {
		file     string
		fragment string
		label    string
	}{
		{m.Files.CorruptCRC, "CRC32 mismatch", "corrupt CRC offset"},
		{m.Files.CorruptLength, "length prefix", "corrupt length offset"},
		{m.Files.CorruptMagic, "bad magic", "corrupt record magic offset"},
	} {
		status, body, err := v.request(http.MethodPost, "/v1/negotiate?format=auto", v.readFile(c.file), nil)
		var e ErrorEnvelope
		_ = json.Unmarshal(body, &e)
		v.check(err == nil && status == 400 && e.Error.Code == "E_FORMAT_CORRUPT" && e.Error.ByteOffset != nil && *e.Error.ByteOffset > 0 && strings.Contains(e.Error.Message, c.fragment),
			c.label, fmt.Sprintf("status=%d code=%s offset=%v msg=%s", status, e.Error.Code, derefOffset(e.Error.ByteOffset), e.Error.Message))
	}

	status, import1Bytes, err := v.postImport("verify-v1", legacyBytes, "auto")
	v.check(err == nil && status == 201, "legacy import committed", fmt.Sprintf("status=%d err=%v body=%s", status, err, preview(import1Bytes)))
	var imp1 ImportResponse
	v.decode(import1Bytes, &imp1, "legacy import response")
	v.check(imp1.SnapshotVersion == 1 && imp1.ActiveVersion == 1 && imp1.Status == "committed", "legacy active version 1", fmt.Sprintf("snapshot=%d active=%d status=%s", imp1.SnapshotVersion, imp1.ActiveVersion, imp1.Status))
	report1 := v.getReport(imp1.ReportID)
	v.check(report1.Status == "committed" && len(report1.Items) == 4, "legacy import report committed with 4 items", fmt.Sprintf("status=%s items=%d", report1.Status, len(report1.Items)))
	v.check(len(report1.DefaultsApplied) > 0, "legacy report contains downgrade detail", fmt.Sprintf("%d", len(report1.DefaultsApplied)))

	p1 := v.getPolicies(0)
	v.check(p1.SnapshotVersion == 1 && p1.ActiveVersion == 1 && len(p1.Policies) == 4, "policy query returns complete version 1", fmt.Sprintf("snapshot=%d policies=%d", p1.SnapshotVersion, len(p1.Policies)))
	checkout1, ok := findPolicy(p1.Policies, "checkout-rate")
	v.check(ok && checkout1.RateLimitBps == 1000000 && checkout1.CircuitWindowMs == 30000 && checkout1.RateWindowMs == 60000 && checkout1.WindowAlignment == "up" && checkout1.TimeoutMs == 1500,
		"legacy defaults normalized", fmt.Sprintf("%+v", checkout1))

	status, import2Bytes, err := v.postImport("verify-v2", currentBytes, "auto")
	v.check(err == nil && status == 201, "current import committed", fmt.Sprintf("status=%d err=%v body=%s", status, err, preview(import2Bytes)))
	var imp2 ImportResponse
	v.decode(import2Bytes, &imp2, "current import response")
	v.check(imp2.SnapshotVersion == 2 && imp2.ActiveVersion == 2, "active version advanced to 2", fmt.Sprintf("snapshot=%d active=%d", imp2.SnapshotVersion, imp2.ActiveVersion))
	v.check(containsAll(imp2.AffectedServices, []string{"cart", "catalog", "checkout"}), "cascade affected services reported", strings.Join(imp2.AffectedServices, ","))
	report2 := v.getReport(imp2.ReportID)
	itemMap := reportItems(report2.Items)
	for id, outcome := range m.Expectations.Current.Items {
		v.check(itemMap[id] == outcome, "conflict outcome "+id, fmt.Sprintf("got=%s want=%s", itemMap[id], outcome))
	}

	p2 := v.getPolicies(0)
	v.check(p2.SnapshotVersion == 2 && p2.ActiveVersion == 2, "active policy query bound to version 2", fmt.Sprintf("snapshot=%d active=%d", p2.SnapshotVersion, p2.ActiveVersion))
	checkout2, _ := findPolicy(p2.Policies, "checkout-rate")
	v.check(checkout2.RateLimitBps == 250000 && checkout2.CircuitWindowMs == 45000 && checkout2.TimeoutMs == 1750, "JSON units normalized to bps/ms", fmt.Sprintf("bps=%d circuit=%d timeout=%d", checkout2.RateLimitBps, checkout2.CircuitWindowMs, checkout2.TimeoutMs))
	v.check(checkout2.Rounding.RemainderUs == 456 && checkout2.Rounding.RoundingBit == 1 && checkout2.Rounding.ErrorUs == -456, "per-record rounding audit", fmt.Sprintf("%+v", checkout2.Rounding))
	retained, ok := findPolicy(p2.Policies, "legacy-report")
	v.check(ok && retained.PolicyRevision == 3 && retained.Match.PathPrefix == "/reports", "lower revision conflict skipped and old policy retained", fmt.Sprintf("rev=%d path=%s", retained.PolicyRevision, retained.Match.PathPrefix))
	_, removed := findPolicy(p2.Policies, "remove-me")
	v.check(!removed, "absent snapshot policy removed", "")

	oldP1 := v.getPolicies(1)
	v.check(oldP1.SnapshotVersion == 1 && oldP1.ActiveVersion == 2, "explicit old version remains bound and readable", fmt.Sprintf("snapshot=%d active=%d", oldP1.SnapshotVersion, oldP1.ActiveVersion))
	oldCheckout, _ := findPolicy(oldP1.Policies, "checkout-rate")
	v.check(oldCheckout.RateLimitBps == 1000000, "old version does not mix new policy values", fmt.Sprintf("bps=%d", oldCheckout.RateLimitBps))

	d2 := v.getDerived(2)
	v.check(d2.SnapshotVersion == 2 && len(d2.RouteIndex) == 4 && len(d2.CircuitStates) == 4 && len(d2.Summaries) == 4, "derived structures rebuilt for version 2", fmt.Sprintf("routes=%d circuits=%d summaries=%d", len(d2.RouteIndex), len(d2.CircuitStates), len(d2.Summaries)))
	v.check(containsAll(d2.Affected, []string{"cart", "catalog", "checkout"}), "derived affected service set", strings.Join(d2.Affected, ","))
	d1 := v.getDerived(1)
	v.check(d1.SnapshotVersion == 1 && len(d1.RouteIndex) == 4, "old derived version remains independently readable", fmt.Sprintf("routes=%d", len(d1.RouteIndex)))

	status, nonMonotonicBody, err := v.postImport("verify-v1-again", legacyBytes, "auto")
	var ne ErrorEnvelope
	_ = json.Unmarshal(nonMonotonicBody, &ne)
	v.check(err == nil && status == 409 && ne.Error.Code == "E_VERSION_NOT_MONOTONIC", "non-monotonic snapshot version rejected", fmt.Sprintf("status=%d code=%s", status, ne.Error.Code))
	v.check(v.getVersions().ActiveVersion == 2, "non-monotonic rejection leaves active version 2", "")

	status, unsupportedImport, err := v.postImport("verify-v3-unsupported", unsupportedBytes, "auto")
	var ue ErrorEnvelope
	_ = json.Unmarshal(unsupportedImport, &ue)
	v.check(err == nil && status == 422 && ue.Error.Code == "E_FORMAT_UNSUPPORTED" && containsAll(ue.Error.UnknownFields, m.Expectations.Unsupported.UnknownFields),
		"unsupported import rejected with unknown field name", fmt.Sprintf("status=%d code=%s fields=%v", status, ue.Error.Code, ue.Error.UnknownFields))
	v.check(v.getVersions().ActiveVersion == 2, "unsupported import leaves active version 2", "")

	status, duplicateImport, err := v.postImport("verify-v4-duplicate", duplicateBytes, "auto")
	var de ErrorEnvelope
	_ = json.Unmarshal(duplicateImport, &de)
	v.check(err == nil && status == 409 && de.Error.Code == m.Expectations.DuplicateBatch.ErrorCode && de.Error.ReportID != "",
		"duplicate batch rejected atomically", fmt.Sprintf("status=%d code=%s report=%s", status, de.Error.Code, de.Error.ReportID))
	rejectReport := v.getReport(de.Error.ReportID)
	v.check(rejectReport.Status == "rejected" && hasRejectItem(rejectReport.Items), "reject report lists per-policy outcomes", fmt.Sprintf("status=%s items=%d", rejectReport.Status, len(rejectReport.Items)))
	v.check(v.getVersions().ActiveVersion == 2, "rejected batch leaves active version 2", "")
	versionsFinal := v.getVersions()
	v.check(len(versionsFinal.Versions) == 2, "rejected batches did not persist snapshot versions 3/4", fmt.Sprintf("versions=%d", len(versionsFinal.Versions)))

	status, replayBody, err := v.postImport("verify-v2", currentBytes, "auto")
	v.check(err == nil && status == 201 && bytes.Equal(replayBody, import2Bytes), "same request id reuses exact stored response", fmt.Sprintf("status=%d identical=%v", status, bytes.Equal(replayBody, import2Bytes)))
	status, conflictBody, err := v.postImport("verify-v2", legacyBytes, "auto")
	var ce ErrorEnvelope
	_ = json.Unmarshal(conflictBody, &ce)
	v.check(err == nil && status == 409 && ce.Error.Code == "E_CONFLICTING_REQUEST", "same request id different content is conflicting", fmt.Sprintf("status=%d code=%s", status, ce.Error.Code))

	status, retiredBody, err := v.request(http.MethodGet, "/v1/policies?version=999", nil, nil)
	var retired ErrorEnvelope
	_ = json.Unmarshal(retiredBody, &retired)
	v.check(err == nil && status == 410 && retired.Error.Code == "E_VERSION_RETIRED", "unknown/retired version returns E_VERSION_RETIRED", fmt.Sprintf("status=%d code=%s", status, retired.Error.Code))
}

func (v *Verifier) postImport(requestID string, body []byte, format string) (int, []byte, error) {
	headers := map[string]string{"X-Request-ID": requestID, "X-Format": format}
	return v.request(http.MethodPost, "/v1/import?truncation=floor", body, headers)
}

func (v *Verifier) getVersions() VersionList {
	status, body, err := v.request(http.MethodGet, "/v1/versions", nil, nil)
	v.check(err == nil && status == 200, "versions endpoint", fmt.Sprintf("status=%d err=%v", status, err))
	var out VersionList
	_ = json.Unmarshal(body, &out)
	return out
}

func (v *Verifier) getPolicies(version uint64) PolicyList {
	path := "/v1/policies"
	if version > 0 {
		path += fmt.Sprintf("?version=%d", version)
	}
	status, body, err := v.request(http.MethodGet, path, nil, nil)
	v.check(err == nil && status == 200, "policies endpoint", fmt.Sprintf("version=%d status=%d err=%v", version, status, err))
	var out PolicyList
	if err := json.Unmarshal(body, &out); err != nil {
		v.fail("decode policy list: " + err.Error())
	}
	return out
}

func (v *Verifier) getDerived(version uint64) DerivedResponse {
	status, body, err := v.request(http.MethodGet, fmt.Sprintf("/v1/derived?version=%d", version), nil, nil)
	v.check(err == nil && status == 200, "derived endpoint", fmt.Sprintf("version=%d status=%d err=%v", version, status, err))
	var out DerivedResponse
	if err := json.Unmarshal(body, &out); err != nil {
		v.fail("decode derived response: " + err.Error())
	}
	return out
}

func (v *Verifier) getReport(reportID string) Report {
	status, body, err := v.request(http.MethodGet, "/v1/imports/"+url.PathEscape(reportID), nil, nil)
	v.check(err == nil && status == 200, "import report endpoint", fmt.Sprintf("report=%s status=%d err=%v", reportID, status, err))
	var out Report
	if err := json.Unmarshal(body, &out); err != nil {
		v.fail("decode import report: " + err.Error())
	}
	return out
}

func (v *Verifier) request(method, path string, body []byte, headers map[string]string) (int, []byte, error) {
	req, err := http.NewRequest(method, v.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	for k, val := range headers {
		req.Header.Set(k, val)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	return resp.StatusCode, data, err
}

func (v *Verifier) readFile(name string) []byte {
	b, err := os.ReadFile(filepath.Join(v.dir, name))
	if err != nil {
		v.fail("read sample " + name + ": " + err.Error())
		return nil
	}
	return b
}

func (v *Verifier) decode(body []byte, out any, label string) {
	if err := json.Unmarshal(body, out); err != nil {
		v.fail(label + ": " + err.Error())
	}
}

func (v *Verifier) check(ok bool, label, detail string) {
	v.checks++
	if ok {
		fmt.Printf("PASS %s\n", label)
		return
	}
	msg := "FAIL " + label
	if detail != "" {
		msg += " :: " + detail
	}
	fmt.Println(msg)
	v.failures = append(v.failures, msg)
}

func (v *Verifier) fail(msg string) {
	v.failures = append(v.failures, msg)
	fmt.Println("FAIL " + msg)
}

func findPolicy(policies []model.Policy, id string) (model.Policy, bool) {
	for _, p := range policies {
		if p.PolicyID == id {
			return p, true
		}
	}
	return model.Policy{}, false
}

func reportItems(items []ReportItem) map[string]string {
	out := make(map[string]string, len(items))
	for _, it := range items {
		out[it.PolicyID] = it.Outcome
	}
	return out
}

func hasRejectItem(items []ReportItem) bool {
	for _, it := range items {
		if it.Outcome == "reject" {
			return true
		}
	}
	return false
}

func containsAll(have, want []string) bool {
	set := make(map[string]struct{}, len(have))
	for _, v := range have {
		set[v] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; !ok {
			return false
		}
	}
	return true
}

func roundingFor(records []RoundingView, policyID string) RoundingView {
	for _, r := range records {
		if r.PolicyID == policyID {
			return r
		}
	}
	return RoundingView{}
}

func derefOffset(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func preview(b []byte) string {
	if len(b) > 300 {
		b = b[:300]
	}
	return string(b)
}
