package engine

import (
	"fmt"

	"casengine/internal/codes"
)

// ChainReport is the engine-facing result of verifying a tenant's chain.
type ChainReport struct {
	TenantID    string `json:"tenantId"`
	Contiguous  bool   `json:"contiguous"`
	HeadVersion int64  `json:"headVersion"`
	LastContig  int64  `json:"lastContiguousVersion"`
	GapAt       int64  `json:"gapAtVersion,omitempty"`
	HeadMatches bool   `json:"headDigestMatchesLive"`
	Recovered   bool   `json:"recovered"`
}

// VerifyTenantChain checks version continuity and content digest. On any
// integrity problem the tenant is faulted (taken out of service) until Rollback
// is invoked.
func (e *Engine) VerifyTenantChain(tenantID string) (ChainReport, error) {
	st, err := e.store.VerifyChain(tenantID)
	if err != nil {
		return ChainReport{}, err
	}
	rep := ChainReport{
		TenantID: tenantID, Contiguous: st.Contiguous,
		HeadVersion: st.HeadVersion, LastContig: st.LastContig,
		GapAt: st.GapAt, HeadMatches: st.HeadMatches,
	}
	if !st.Contiguous || !st.HeadMatches {
		e.MarkTenantFault(tenantID, codes.New(codes.SnapshotVersion,
			fmt.Sprintf("snapshot chain invalid for %s", tenantID)))
	}
	return rep, nil
}

// RollbackTenant truncates to the last contiguous version and appends a fresh
// snapshot of the live state, then restores service to the tenant.
func (e *Engine) RollbackTenant(tenantID string) (ChainReport, error) {
	st, err := e.store.VerifyChain(tenantID)
	if err != nil {
		return ChainReport{}, err
	}
	_, hadFault := e.tenantFault.Load(tenantID)
	if st.Contiguous && st.HeadMatches && !hadFault {
		e.ClearTenantFault(tenantID)
		// A fault may still be registered from an earlier observation even
		// though the durable chain is already healthy; report it as recovered.
		return ChainReport{TenantID: tenantID, Contiguous: true,
			HeadVersion: st.HeadVersion, LastContig: st.LastContig,
			HeadMatches: true, Recovered: hadFault}, nil
	}
	truncateTo := st.LastContig
	if st.Contiguous && truncateTo > 0 && (hadFault || !st.HeadMatches) {
		// The chain has no gap but the head snapshot does not match live state
		// (e.g. a non-committed transaction was discarded on restart). Drop
		// the stale head before rebuilding a truthful snapshot.
		truncateTo--
	}
	if _, err := e.store.Rollback(tenantID, truncateTo, e.clk.EventNS(800001)); err != nil {
		return ChainReport{}, err
	}
	e.ClearTenantFault(tenantID)
	// Re-read the chain without re-triggering a fault mark: rollback is the
	// explicit recovery action, so a healthy result must clear the fault.
	st2, err := e.store.VerifyChain(tenantID)
	if err != nil {
		return ChainReport{}, err
	}
	after := ChainReport{
		TenantID: tenantID, Contiguous: st2.Contiguous,
		HeadVersion: st2.HeadVersion, LastContig: st2.LastContig,
		GapAt: st2.GapAt, HeadMatches: st2.HeadMatches, Recovered: true,
	}
	after.Recovered = true
	if !after.Contiguous || !after.HeadMatches {
		return ChainReport{}, fmt.Errorf("rollback did not restore a healthy chain for %s", tenantID)
	}
	return after, nil
}

// EnsureAllChains runs at startup after a (possibly forced) shutdown. Tenants
// with a healthy chain continue; broken chains are rolled back automatically.
func (e *Engine) EnsureAllChains(tenantIDs []string) ([]ChainReport, error) {
	var reps []ChainReport
	for _, id := range tenantIDs {
		rep, err := e.VerifyTenantChain(id)
		if err != nil {
			return nil, err
		}
		if !rep.Contiguous || !rep.HeadMatches {
			rep, err = e.RollbackTenant(id)
			if err != nil {
				return nil, err
			}
		}
		reps = append(reps, rep)
	}
	return reps, nil
}
