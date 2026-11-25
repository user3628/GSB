package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"searchlab/internal/apperr"
	"searchlab/internal/checkpoint"
	"searchlab/internal/compress"
	"searchlab/internal/config"
	"searchlab/internal/model"
	"searchlab/internal/stage"
	"searchlab/internal/tokenize"
)

type StageReport struct {
	Stage          string `json:"stage"`
	Seq            int64  `json:"seq"`
	InputDigest    string `json:"input_digest"`
	ArtifactDigest string `json:"artifact_digest"`
	Reused         bool   `json:"reused"`
}

type BuildReport struct {
	PipelineID    string        `json:"pipeline_id"`
	ConfigVersion string        `json:"config_version"`
	ConfigDigest  string        `json:"config_digest"`
	ShardDigest   string        `json:"shard_digest"`
	IndexVersion  string        `json:"index_version"`
	Stages        []StageReport `json:"stages"`
}

func Build(ctx context.Context, cfg config.Config, corpus model.Corpus, store *checkpoint.Store, force bool) (*BuildReport, error) {
	pipelineID := digestParts("pipeline/v1", cfg.Digest(), corpus.ShardDigest)
	report := &BuildReport{
		PipelineID:    pipelineID,
		ConfigVersion: cfg.Version,
		ConfigDigest:  cfg.Digest(),
		ShardDigest:   corpus.ShardDigest,
		Stages:        make([]StageReport, 0, len(stage.Order)),
	}
	var tokensPayload []byte
	var tokensArtifact *tokenize.Artifact
	var statsPayload []byte
	var statsArtifact *StatsArtifact
	var postingsPayload []byte
	var postingsArtifact *compress.Artifact
	for _, stageName := range stage.Order {
		var inputDigest string
		switch stageName {
		case stage.Tokenize:
			inputDigest = digestParts("tokenize/v1", corpus.ShardDigest, cfg.TokenizerDigest())
		case stage.Stats:
			inputDigest = digestParts("stats/v1", digest(tokensPayload), cfg.StatsDigest())
		case stage.Postings:
			inputDigest = digestParts("postings/v1", digest(statsPayload), cfg.PostingsDigest())
		case stage.Diagnostics:
			inputDigest = digestParts("diagnostics/v1", digest(postingsPayload), cfg.DiagnosticsDigest())
		}
		var payload []byte
		reused := false
		if !force {
			active, err := store.Active(ctx, stageName, corpus.ShardDigest)
			if err != nil {
				return nil, err
			}
			if active != nil && active.Record.InputDigest == inputDigest {
				payload = active.Payload
				reused = true
			}
		}
		if !reused {
			var err error
			switch stageName {
			case stage.Tokenize:
				payload, tokensArtifact, err = tokenize.New(cfg).Build(corpus)
			case stage.Stats:
				if tokensArtifact == nil {
					return nil, apperr.E(apperr.CodePrereqMissing, "token artifact is unavailable", nil)
				}
				payload, statsArtifact, err = BuildStats(cfg, *tokensArtifact)
			case stage.Postings:
				if statsArtifact == nil {
					return nil, apperr.E(apperr.CodePrereqMissing, "stats artifact is unavailable", nil)
				}
				payload, postingsArtifact, err = compress.Build(cfg, *statsArtifact)
			case stage.Diagnostics:
				if statsArtifact == nil || postingsArtifact == nil {
					return nil, apperr.E(apperr.CodePrereqMissing, "diagnostics prerequisites are unavailable", nil)
				}
				var diagnosticsArtifact *DiagnosticsArtifact
				payload, diagnosticsArtifact, err = BuildDiagnostics(cfg, *statsArtifact, *postingsArtifact)
				_ = diagnosticsArtifact
			}
			if err != nil {
				return nil, err
			}
		}
		switch stageName {
		case stage.Tokenize:
			tokensPayload = payload
			var decoded tokenize.Artifact
			if err := json.Unmarshal(payload, &decoded); err != nil {
				return nil, &apperr.Error{Code: apperr.CodeInput, Message: "decode token artifact", Stage: stageName, Err: err}
			}
			tokensArtifact = &decoded
		case stage.Stats:
			statsPayload = payload
			var decoded StatsArtifact
			if err := json.Unmarshal(payload, &decoded); err != nil {
				return nil, &apperr.Error{Code: apperr.CodeInput, Message: "decode stats artifact", Stage: stageName, Err: err}
			}
			statsArtifact = &decoded
		case stage.Postings:
			postingsPayload = payload
			var decoded compress.Artifact
			if err := json.Unmarshal(payload, &decoded); err != nil {
				return nil, &apperr.Error{Code: apperr.CodeInput, Message: "decode postings artifact", Stage: stageName, Err: err}
			}
			postingsArtifact = &decoded
			if err := compress.VerifyArtifact(decoded); err != nil {
				return nil, err
			}
		case stage.Diagnostics:
			// The payload is already deterministic; keep the last artifact digest as the chain input for clarity.
		}
		artifactDigest := digest(payload)
		seq, err := store.NextSeq(ctx)
		if err != nil {
			return nil, err
		}
		record, err := store.Publish(ctx, checkpoint.PublishRequest{
			Seq:            seq,
			Stage:          stageName,
			ShardDigest:    corpus.ShardDigest,
			InputDigest:    inputDigest,
			ConfigVersion:  cfg.Digest(),
			ArtifactDigest: artifactDigest,
			PipelineID:     pipelineID,
			Payload:        payload,
		})
		if err != nil {
			return nil, err
		}
		report.Stages = append(report.Stages, StageReport{Stage: stageName, Seq: record.Seq, InputDigest: inputDigest, ArtifactDigest: artifactDigest, Reused: reused})
	}
	configPayload := cfg.CanonicalJSON()
	if err := store.PutMeta(ctx, "config_json", configPayload); err != nil {
		return nil, err
	}
	if err := store.PutMeta(ctx, "config_version", []byte(cfg.Version)); err != nil {
		return nil, err
	}
	if err := store.PutMeta(ctx, "config_digest", []byte(cfg.Digest())); err != nil {
		return nil, err
	}
	if err := store.PutMeta(ctx, "shard_digest", []byte(corpus.ShardDigest)); err != nil {
		return nil, err
	}
	if err := store.PutMeta(ctx, "pipeline_id", []byte(pipelineID)); err != nil {
		return nil, err
	}
	version, err := store.IndexVersion(ctx)
	if err != nil {
		return nil, err
	}
	report.IndexVersion = version
	return report, nil
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func digestParts(prefix string, parts ...string) string {
	payload, _ := json.Marshal(struct {
		Prefix string   `json:"prefix"`
		Parts  []string `json:"parts"`
	}{Prefix: prefix, Parts: parts})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func FormatReport(report *BuildReport) string {
	payload, _ := json.MarshalIndent(report, "", "  ")
	return string(payload)
}

func ValidateStageInputs(report *BuildReport) error {
	if len(report.Stages) != len(stage.Order) {
		return fmt.Errorf("incomplete build report")
	}
	for i, item := range report.Stages {
		if item.Stage != stage.Order[i] {
			return fmt.Errorf("stage report order mismatch at %d", i)
		}
	}
	return nil
}
