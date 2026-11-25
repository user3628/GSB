package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"searchlab/internal/apperr"
	"searchlab/internal/checkpoint"
	"searchlab/internal/compress"
	"searchlab/internal/config"
	"searchlab/internal/index"
	"searchlab/internal/stage"
)

type Runtime struct {
	Config               config.Config
	ShardDigest          string
	ConfigVersion        string
	IndexVersion         string
	PipelineID           string
	Stats                StatsArtifact
	Postings             compress.Artifact
	Diagnostics          DiagnosticsArtifact
	StatsByTerm          map[string]*index.TermStat
	PostingsByTerm       map[string]*compress.CompressedTerm
	StatPostingByTermDoc map[string]map[int64]index.QuantPosting
	Records              []checkpoint.Record
}

func LoadRuntime(ctx context.Context, store *checkpoint.Store) (*Runtime, error) {
	payload, err := store.GetMeta(ctx, "config_json")
	if err != nil {
		return nil, err
	}
	var cfg config.Config
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return nil, apperr.E(apperr.CodeInput, "metadata config JSON is invalid", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, apperr.E(apperr.CodeInput, "metadata config is invalid", err)
	}
	loaded := make(map[string]*checkpoint.Loaded)
	for _, stageName := range stage.Order {
		item, err := store.LoadLatest(ctx, stageName)
		if err != nil {
			return nil, err
		}
		loaded[stageName] = item
	}
	pipelineID := loaded[stage.Tokenize].Record.PipelineID
	previousSeq := int64(0)
	for _, stageName := range stage.Order {
		record := loaded[stageName].Record
		if record.PipelineID != pipelineID {
			return nil, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "active checkpoints do not belong to one pipeline", Stage: stageName}
		}
		if previousSeq != 0 && record.Seq != previousSeq+1 {
			return nil, &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "active checkpoints are not sequence-contiguous", Stage: stageName, ExpectedSeq: previousSeq + 1, ActualSeq: record.Seq}
		}
		if record.ConfigVersion != cfg.Digest() {
			return nil, &apperr.Error{Code: apperr.CodePrereqMissing, Message: "checkpoint config version differs from stored config", Stage: stageName}
		}
		previousSeq = record.Seq
	}
	var stats StatsArtifact
	if err := json.Unmarshal(loaded[stage.Stats].Payload, &stats); err != nil {
		return nil, apperr.E(apperr.CodeInput, "decode active stats artifact", err)
	}
	var postings compress.Artifact
	if err := json.Unmarshal(loaded[stage.Postings].Payload, &postings); err != nil {
		return nil, apperr.E(apperr.CodeInput, "decode active postings artifact", err)
	}
	var diagnostics DiagnosticsArtifact
	if err := json.Unmarshal(loaded[stage.Diagnostics].Payload, &diagnostics); err != nil {
		return nil, apperr.E(apperr.CodeInput, "decode active diagnostics artifact", err)
	}
	if err := verifyRuntimeArtifacts(stats, postings); err != nil {
		return nil, err
	}
	indexVersion, err := store.IndexVersion(ctx)
	if err != nil {
		return nil, err
	}
	shardPayload, err := store.GetMeta(ctx, "shard_digest")
	if err != nil {
		return nil, err
	}
	runtime := &Runtime{
		Config:               cfg,
		ShardDigest:          string(shardPayload),
		ConfigVersion:        cfg.Digest(),
		IndexVersion:         indexVersion,
		PipelineID:           pipelineID,
		Stats:                stats,
		Postings:             postings,
		Diagnostics:          diagnostics,
		StatsByTerm:          make(map[string]*index.TermStat, len(stats.Terms)),
		PostingsByTerm:       make(map[string]*compress.CompressedTerm, len(postings.Terms)),
		StatPostingByTermDoc: make(map[string]map[int64]index.QuantPosting, len(stats.Terms)),
		Records: []checkpoint.Record{
			loaded[stage.Tokenize].Record,
			loaded[stage.Stats].Record,
			loaded[stage.Postings].Record,
			loaded[stage.Diagnostics].Record,
		},
	}
	for i := range runtime.Stats.Terms {
		stat := &runtime.Stats.Terms[i]
		runtime.StatsByTerm[stat.Term] = stat
		byDoc := make(map[int64]index.QuantPosting, len(stat.Postings))
		for _, posting := range stat.Postings {
			byDoc[posting.DocID] = posting
		}
		runtime.StatPostingByTermDoc[stat.Term] = byDoc
	}
	for i := range runtime.Postings.Terms {
		runtime.PostingsByTerm[runtime.Postings.Terms[i].Term] = &runtime.Postings.Terms[i]
	}
	return runtime, nil
}

func verifyRuntimeArtifacts(stats StatsArtifact, postings compress.Artifact) error {
	if len(stats.Terms) != len(postings.Terms) {
		return apperr.E(apperr.CodeDecodeMismatch, "stats and postings artifacts have different term counts", nil)
	}
	for i := range stats.Terms {
		stat := &stats.Terms[i]
		term := &postings.Terms[i]
		if stat.TermID != term.TermID || stat.Term != term.Term || stat.DF != term.DF {
			return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "stats and postings term identity differ", Term: stat.Term}
		}
		byDoc := make(map[int64]int64, len(stat.Postings))
		for _, posting := range stat.Postings {
			byDoc[posting.DocID] = posting.Quantized
		}
		seen := 0
		previous := int64(-1)
		for _, block := range term.Blocks {
			decoded, err := compress.DecodeBlock(block)
			if err != nil {
				return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "active compressed block does not decode", Term: stat.Term, FirstDiffBlock: block.Ordinal + 1, Err: err}
			}
			for _, posting := range decoded {
				if posting.DocID <= previous {
					return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "active postings are not strictly ordered", Term: stat.Term, FirstDiffBlock: block.Ordinal + 1}
				}
				expected, ok := byDoc[posting.DocID]
				if !ok || expected != posting.Quant {
					return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "decoded doc_id or weight differs from uncompressed stats", Term: stat.Term, DocumentID: posting.DocID, FirstDiffBlock: block.Ordinal + 1}
				}
				previous = posting.DocID
				seen++
			}
		}
		if seen != stat.DF {
			return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: fmt.Sprintf("decoded postings %d differ from df %d", seen, stat.DF), Term: stat.Term}
		}
	}
	return nil
}
