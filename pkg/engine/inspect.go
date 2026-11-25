package engine

import (
	"context"
	"fmt"

	"local/quantops/pkg/dataset"
	"local/quantops/pkg/qerr"
	"local/quantops/pkg/store"
)

// ClassifyCheckpoint compares committed state with the current manifest and
// returns normal, replacement, or rollback classification. Replacement
// invalidates the earliest affected stage and all downstream stages; rollback
// is rejected with E_STALE_CHECKPOINT.
func ClassifyCheckpoint(ctx context.Context, ds *dataset.Loaded, st *store.Store, shardID string) (*store.ReconcileResult, error) {
	sh, ok := ds.Shard(shardID)
	if !ok {
		return nil, qerr.New(qerr.CodeInvalidInput, shardID, "", "", "unknown shard", nil)
	}
	rt, err := prepareRuntime(ds, sh)
	if err != nil {
		return nil, fmt.Errorf("prepare shard %s: %w", shardID, err)
	}
	return st.Reconcile(ctx, store.ReconcileRequest{
		ShardID: sh.ID, ShardOrdinal: sh.Ordinal,
		BaseInputFingerprint: rt.contentFP, Expected: expectedStageConfigs(rt),
	})
}
