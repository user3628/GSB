package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"vpnbench/internal/config"
	"vpnbench/internal/engine"
	"vpnbench/internal/model"
	"vpnbench/internal/monotonic"
	"vpnbench/internal/policy"
	"vpnbench/internal/report"
	"vpnbench/internal/requests"
	"vpnbench/internal/scheduler"
	"vpnbench/internal/store"
)

type RunSpec struct {
	PolicyPath       string
	Sessions         []config.SessionSpec
	ConcurrencyLimit int
	LatencyClock     string
	DefaultBudget    int
}

type RunResult struct {
	Report              report.Report
	Policy              model.PolicySet
	PolicySnapshot      store.Snapshot
	SettlementSnapshots []store.Snapshot
	Outcomes            []engine.Outcome
}

type persistedSession struct {
	Schema      string                 `json:"schema"`
	Clock       report.ClockInfo       `json:"clock"`
	Concurrency scheduler.Metrics      `json:"concurrency"`
	Outcome     engine.Outcome         `json:"outcome"`
	ExtraCodes  []model.CodeOccurrence `json:"extra_codes,omitempty"`
}

type policySnapshotContent struct {
	Schema   string         `json:"schema"`
	Digest   string         `json:"digest"`
	Policies []model.Policy `json:"policies"`
}

type preparedSession struct {
	Spec     config.SessionSpec
	Requests []model.Request
}

func Run(ctx context.Context, spec RunSpec, dbPath string) (RunResult, error) {
	if len(spec.Sessions) == 0 {
		return RunResult{}, model.NewError(model.CodeInternal, "at least one session is required", 0)
	}
	if spec.ConcurrencyLimit < 1 {
		spec.ConcurrencyLimit = 1
	}
	if spec.LatencyClock == "" {
		spec.LatencyClock = "fixed"
	}
	if spec.DefaultBudget == 0 {
		spec.DefaultBudget = 10000
	}
	for i := range spec.Sessions {
		if spec.Sessions[i].SessionID == "" {
			return RunResult{}, model.NewError(model.CodeInternal, "session_id must not be empty", 0)
		}
		if spec.Sessions[i].RequestBudget == 0 {
			spec.Sessions[i].RequestBudget = spec.DefaultBudget
		}
	}

	prepared, err := prepareSessions(spec.Sessions)
	if err != nil {
		return RunResult{}, err
	}
	sort.SliceStable(prepared, func(i, j int) bool {
		return prepared[i].Spec.SessionID < prepared[j].Spec.SessionID
	})

	database, err := store.Open(dbPath)
	if err != nil {
		return RunResult{}, err
	}
	defer database.Close()

	loadedPolicy, policySnapshot, policyChainBefore, err := loadPolicy(ctx, database, spec.PolicyPath)
	if err != nil {
		return RunResult{}, err
	}
	var extraCodes []model.CodeOccurrence
	if policyChainBefore != nil && len(policyChainBefore.MissingVersions) > 0 {
		extraCodes = append(extraCodes, model.CodeOccurrence{
			Code:    model.CodeSnapshotVersion,
			Offset:  0,
			Message: fmt.Sprintf("policy snapshot chain gap; using fallback version %d", policySnapshot.Version),
		})
	}
	if policySnapshot.ChangeType == "WHOLE_REPLACEMENT" {
		extraCodes = append(extraCodes, model.CodeOccurrence{
			Code:    model.CodePolicyReplaced,
			Offset:  0,
			Message: fmt.Sprintf("policy snapshot %d wholly replaced snapshot %d", policySnapshot.Version, policySnapshot.BeforeSummary.Version),
		})
	}

	clockFactory := monotonic.Factory(spec.LatencyClock)
	clockSample := clockFactory()
	clockInfo := report.ClockInfo{Mode: clockSample.Mode(), Marker: clockSample.Marker()}
	outcomes, metrics, conflictCodes, err := runSessions(ctx, prepared, loadedPolicy, spec.ConcurrencyLimit, clockFactory)
	if err != nil {
		return RunResult{}, err
	}
	extraCodes = append(extraCodes, conflictCodes...)

	settled := make([]report.SettledSession, 0, len(outcomes))
	snapshots := make([]store.Snapshot, 0, len(outcomes))
	for _, outcome := range outcomes {
		persisted := persistedSession{
			Schema:      "vpnbench.settled-session/v1",
			Clock:       clockInfo,
			Concurrency: metrics,
			Outcome:     outcome,
			ExtraCodes:  append([]model.CodeOccurrence(nil), extraCodes...),
		}
		encoded, err := json.Marshal(persisted)
		if err != nil {
			return RunResult{}, model.NewError(model.CodeInternal, "encode settled session: "+err.Error(), 0)
		}
		results := make([]store.WindowResult, 0, len(outcome.Requests))
		for _, request := range outcome.Requests {
			resultJSON, err := json.Marshal(request)
			if err != nil {
				return RunResult{}, model.NewError(model.CodeInternal, "encode request result: "+err.Error(), 0)
			}
			results = append(results, store.WindowResult{
				StreamID:   request.StreamID,
				RequestSeq: request.RequestSeq,
				Code:       request.Settlement.Code,
				ResultJSON: resultJSON,
			})
		}
		snapshot, _, err := database.SaveSettlementSnapshot(ctx, store.SettlementInput{
			SessionID:     outcome.SessionID,
			WindowID:      outcome.WindowID,
			Status:        outcome.Status,
			PolicyVersion: outcome.PolicyVersion,
			OutcomeJSON:   encoded,
			Results:       results,
			Corrections:   outcome.Corrections,
			Ledger:        outcome.ErrorLedger,
		})
		if err != nil {
			return RunResult{}, err
		}
		snapshots = append(snapshots, snapshot)
		settled = append(settled, report.SettledSession{Outcome: outcome, SettlementVersion: snapshot.Version})
	}

	policyChain, err := database.Chain(ctx, "policy")
	if err != nil {
		return RunResult{}, err
	}
	settlementChain, err := database.Chain(ctx, "settlement")
	if err != nil {
		return RunResult{}, err
	}
	built := report.Build(clockInfo, metrics, policyChain, settlementChain, settled, extraCodes)
	return RunResult{
		Report:              built,
		Policy:              loadedPolicy,
		PolicySnapshot:      policySnapshot,
		SettlementSnapshots: snapshots,
		Outcomes:            outcomes,
	}, nil
}

func prepareSessions(specs []config.SessionSpec) ([]preparedSession, error) {
	prepared := make([]preparedSession, 0, len(specs))
	for _, spec := range specs {
		data, err := os.ReadFile(spec.RequestsFile)
		if err != nil {
			return nil, model.NewError(model.CodeRequestInvalid, "read request file: "+err.Error(), 0).
				Locate(spec.SessionID, "", 0)
		}
		parsed, err := requests.Parse(data)
		if err != nil {
			var coded *model.CodedError
			if ok := asCoded(err, &coded); ok {
				coded.SessionID = spec.SessionID
			}
			return nil, err
		}
		prepared = append(prepared, preparedSession{Spec: spec, Requests: parsed})
	}
	return prepared, nil
}

func loadPolicy(ctx context.Context, database *store.Store, path string) (model.PolicySet, store.Snapshot, *store.Chain, error) {
	existing, chainErr := database.LatestContiguous(ctx, "policy")
	if chainErr != nil && existing.Fallback != nil {
		fallback := existing.Fallback
		loaded, err := policyFromSnapshot(*fallback)
		if err != nil {
			return model.PolicySet{}, store.Snapshot{}, &existing, err
		}
		return loaded, *fallback, &existing, nil
	}
	if chainErr != nil && len(existing.Snapshots) > 0 {
		return model.PolicySet{}, store.Snapshot{}, &existing, chainErr
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return model.PolicySet{}, store.Snapshot{}, nil, model.NewError(model.CodePolicyInvalid, "read policy file: "+err.Error(), 0)
	}
	loaded, err := policy.Parse(data)
	if err != nil {
		return model.PolicySet{}, store.Snapshot{}, nil, err
	}
	content, err := json.Marshal(policySnapshotContent{
		Schema:   "vpnbench.policy-snapshot/v1",
		Digest:   loaded.Digest,
		Policies: loaded.Policies,
	})
	if err != nil {
		return model.PolicySet{}, store.Snapshot{}, nil, model.NewError(model.CodeInternal, "encode policy snapshot: "+err.Error(), 0)
	}
	identities := make([]string, 0, len(loaded.Policies))
	for _, policyEntry := range loaded.Policies {
		identities = append(identities, policyEntry.TunnelID)
	}
	snapshot, err := database.SavePolicySnapshot(ctx, store.SnapshotInput{
		Kind:          "policy",
		Content:       content,
		IdentityKeys:  identities,
		SummaryDigest: loaded.Digest,
	})
	if err != nil {
		return model.PolicySet{}, store.Snapshot{}, nil, err
	}
	loaded.Version = snapshot.Version
	return loaded, snapshot, nil, nil
}

func policyFromSnapshot(snapshot store.Snapshot) (model.PolicySet, error) {
	var content policySnapshotContent
	if err := json.Unmarshal(snapshot.Content, &content); err != nil {
		return model.PolicySet{}, model.NewError(model.CodeSnapshotVersion, "fallback policy snapshot cannot be decoded", 0)
	}
	loaded, err := policy.Parse(mustJSON(content))
	if err != nil {
		return model.PolicySet{}, err
	}
	loaded.Version = snapshot.Version
	return loaded, nil
}

func runSessions(ctx context.Context, prepared []preparedSession, policySet model.PolicySet, limit int, clockFactory func() monotonic.Clock) ([]engine.Outcome, scheduler.Metrics, []model.CodeOccurrence, error) {
	manager := scheduler.New(limit)
	type result struct {
		outcome engine.Outcome
		err     error
	}
	results := make(chan result, len(prepared))
	var waitGroup sync.WaitGroup
	uniqueSessions := make(map[string]struct{})
	for _, session := range prepared {
		uniqueSessions[session.Spec.SessionID] = struct{}{}
	}
	expectedActive := len(uniqueSessions)
	if expectedActive > limit {
		expectedActive = limit
	}
	expectedQueued := len(uniqueSessions) - expectedActive

	gate := make(chan struct{})
	gateClosed := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() {
		gateOnce.Do(func() {
			close(gate)
			close(gateClosed)
		})
	}
	launch := make(chan struct{})
	for _, preparedSession := range prepared {
		preparedSession := preparedSession
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-launch
			firstSeq := int64(0)
			if len(preparedSession.Requests) > 0 {
				firstSeq = preparedSession.Requests[0].RequestSeq
			}
			release, err := manager.Acquire(ctx, preparedSession.Spec.SessionID, firstSeq)
			if err != nil {
				results <- result{err: err}
				return
			}
			// Hold the first wave at a barrier until the configured active and
			// queued depths have become observable. This makes concurrency
			// metrics stable without serializing session execution.
			metrics := manager.Metrics()
			if metrics.PeakInFlight >= expectedActive && metrics.Queued >= expectedQueued {
				openGate()
			}
			<-gate
			outcome, runErr := engine.Run(engine.Config{
				SessionID:     preparedSession.Spec.SessionID,
				Policy:        policySet,
				Requests:      preparedSession.Requests,
				RequestBudget: preparedSession.Spec.RequestBudget,
				Clock:         clockFactory(),
			})
			release()
			results <- result{outcome: outcome, err: runErr}
		}()
	}
	close(launch)

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
waitForGate:
	for {
		metrics := manager.Metrics()
		if metrics.PeakInFlight >= expectedActive && metrics.Queued >= expectedQueued && expectedActive > 0 {
			openGate()
			break
		}
		select {
		case <-gateClosed:
			break waitForGate
		case <-ctx.Done():
			openGate()
			return nil, scheduler.Metrics{}, nil, model.NewError(model.CodeInternal, "session scheduling cancelled", 0)
		case <-deadline.C:
			openGate()
			return nil, scheduler.Metrics{}, nil, model.NewError(model.CodeInternal,
				fmt.Sprintf("timed out waiting for concurrency observation: expected_active=%d expected_queued=%d metrics=%+v", expectedActive, expectedQueued, metrics), 0)
		case <-ticker.C:
		}
	}
	waitGroup.Wait()
	close(results)

	var outcomes []engine.Outcome
	for item := range results {
		if item.err != nil {
			if model.CodeOf(item.err) == model.CodeSessionConflict {
				continue
			}
			return nil, scheduler.Metrics{}, nil, item.err
		}
		outcomes = append(outcomes, item.outcome)
	}
	sort.SliceStable(outcomes, func(i, j int) bool {
		return outcomes[i].SessionID < outcomes[j].SessionID
	})
	metrics := manager.Metrics()
	var conflictCodes []model.CodeOccurrence
	for _, conflict := range metrics.Conflicts {
		conflictCodes = append(conflictCodes, model.CodeOccurrence{
			Code:       conflict.Code,
			SessionID:  conflict.SessionID,
			RequestSeq: conflict.RequestSeq,
			Offset:     0,
			Message:    "concurrent duplicate session rejected; original session state preserved",
		})
	}
	return outcomes, metrics, conflictCodes, nil
}

func LoadRunConfig(path string) (RunSpec, error) {
	cfg, err := config.ReadRun(path)
	if err != nil {
		return RunSpec{}, err
	}
	return RunSpec{
		PolicyPath:       cfg.PolicyFile,
		Sessions:         cfg.Sessions,
		ConcurrencyLimit: cfg.ConcurrencyLimit,
		LatencyClock:     cfg.LatencyClock,
		DefaultBudget:    cfg.RequestBudget,
	}, nil
}

func asCoded(err error, target **model.CodedError) bool {
	if value, ok := err.(*model.CodedError); ok {
		*target = value
		return true
	}
	return false
}

func mustJSON(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}
