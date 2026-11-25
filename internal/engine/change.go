package engine

import (
	"ticketpipeline/internal/gen"
	"ticketpipeline/internal/model"
)

// BumpPolicy applies one validated change to the current policy and writes a
// new snapshot whose Version is strictly monotonic. Only the fields named in
// patch are changed; the returned policy is what the next engine run reads.
type PolicyPatch struct {
	IntentWeights  map[string]float64
	PriorityThresh []float64
	Smoothing      *float64
	HashDim        *int
	SampleRatio    *float64
	Injection      *model.Injection
}

// BumpPolicy writes policy version N+1 with the patch applied.
func BumpPolicy(dataDir string, patch PolicyPatch) (model.Policy, error) {
	current, err := LoadPolicy(dataDir)
	if err != nil {
		return model.Policy{}, err
	}
	next := current
	next.Version = current.Version + 1
	if patch.IntentWeights != nil {
		next.IntentWeights = cloneWeights(patch.IntentWeights)
	}
	if patch.PriorityThresh != nil {
		next.PriorityThresh = append([]float64(nil), patch.PriorityThresh...)
	}
	if patch.Smoothing != nil {
		next.Smoothing = *patch.Smoothing
	}
	if patch.HashDim != nil {
		next.HashDim = *patch.HashDim
	}
	if patch.SampleRatio != nil {
		next.SampleRatio = *patch.SampleRatio
	}
	if patch.Injection != nil {
		next.Injection = *patch.Injection
	}
	if err := gen.WritePolicy(dataDir, next); err != nil {
		return model.Policy{}, err
	}
	return next, nil
}

// ReplaceShard rewrites one shard file and its sidecar with the supplied
// tickets. DeclaredRows is set to the actual count unless forceMismatch is
// set, in which case one extra row is declared to exercise E_SHARD_MISMATCH.
func ReplaceShard(dataDir, shardID string, tickets []model.RawTicket, forceMismatch bool, policyVersion int) error {
	declared := len(tickets)
	if forceMismatch {
		declared = len(tickets) + 1
	}
	shard := model.Shard{
		Meta:    model.ShardMeta{ShardID: shardID, DeclaredRows: declared, PolicyVersion: policyVersion},
		Tickets: tickets,
	}
	return gen.WriteShard(dataDir, shard)
}

func cloneWeights(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
