package engine

import (
	"encoding/json"
	"fmt"

	"local/quantops/pkg/canon"
	"local/quantops/pkg/quant"
	"local/quantops/pkg/scheduler"
	"local/quantops/pkg/store"
)

const SummarySchemaVersion = "quantops.summary.v1"

type StageSummary struct {
	Stage            string `json:"stage"`
	StageSeq         int    `json:"stage_seq"`
	StageDigest      string `json:"stage_digest"`
	ConfigVersion    string `json:"config_version"`
	InputFingerprint string `json:"input_fingerprint"`
}

type LayerOutputDigest struct {
	Index  int    `json:"index"`
	Name   string `json:"name"`
	Shape  []int  `json:"shape"`
	Digest string `json:"digest"`
}

type ClampSummary struct {
	Layer  int                `json:"layer"`
	Source string             `json:"source"`
	Count  int                `json:"count"`
	Events []quant.ClampEvent `json:"events,omitempty"`
}

type ShardSummary struct {
	ShardID            string              `json:"shard_id"`
	ShardOrdinal       int                 `json:"shard_ordinal"`
	Status             string              `json:"status"`
	OutputShape        []int               `json:"output_shape"`
	OutputDigest       string              `json:"output_digest"`
	LayerOutputs       []LayerOutputDigest `json:"layer_outputs"`
	Stages             []StageSummary      `json:"stages"`
	VersionFingerprint string              `json:"version_fingerprint"`
	Clamps             []ClampSummary      `json:"clamps"`
	ClampCount         int                 `json:"clamp_count"`
	Diagnostics        DiagnosticReport    `json:"diagnostics"`
}

type Summary struct {
	SchemaVersion       string               `json:"schema_version"`
	ManifestFingerprint string               `json:"manifest_fingerprint"`
	Status              string               `json:"status"`
	Limits              scheduler.Limits     `json:"limits"`
	Scheduler           scheduler.Plan       `json:"scheduler"`
	Shards              []ShardSummary       `json:"shards"`
	Untreated           []scheduler.Rejected `json:"untreated"`
	AggregateDigest     string               `json:"aggregate_digest"`
}

func buildShardSummary(rt *shardRuntime, states []store.State, progress *store.Progress, arts *artifactSet) ShardSummary {
	summary := ShardSummary{
		ShardID: rt.spec.ID, ShardOrdinal: rt.spec.Ordinal,
		VersionFingerprint: progress.VersionFingerprint,
		Diagnostics:        arts.verify.Report,
	}
	for _, st := range states {
		summary.Stages = append(summary.Stages, StageSummary{
			Stage: string(st.Stage), StageSeq: st.StageSeq, StageDigest: st.StageDigest,
			ConfigVersion: st.ConfigVersion, InputFingerprint: st.InputFingerprint,
		})
	}
	for _, layer := range arts.deq.Layers {
		digest, _ := canon.Hash(struct {
			Shape  []int     `json:"shape"`
			Values []float64 `json:"values"`
		}{Shape: layer.Shape, Values: layer.Values})
		summary.LayerOutputs = append(summary.LayerOutputs, LayerOutputDigest{
			Index: layer.Index, Name: layer.Name, Shape: append([]int(nil), layer.Shape...), Digest: digest,
		})
	}
	if len(arts.deq.Layers) > 0 {
		final := arts.deq.Layers[len(arts.deq.Layers)-1]
		summary.OutputShape = append([]int(nil), final.Shape...)
		summary.OutputDigest, _ = canon.Hash(struct {
			Shape  []int     `json:"shape"`
			Values []float64 `json:"values"`
		}{Shape: final.Shape, Values: final.Values})
	}
	for _, layer := range arts.fwd.Layers {
		if len(layer.InputClamps) > 0 {
			summary.Clamps = append(summary.Clamps, ClampSummary{
				Layer: layer.Index, Source: "activation", Count: len(layer.InputClamps),
				Events: append([]quant.ClampEvent(nil), layer.InputClamps...),
			})
		}
		if len(layer.WeightClamps) > 0 {
			summary.Clamps = append(summary.Clamps, ClampSummary{
				Layer: layer.Index, Source: "weight", Count: len(layer.WeightClamps),
				Events: append([]quant.ClampEvent(nil), layer.WeightClamps...),
			})
		}
		summary.ClampCount += layer.InputClampCount + layer.WeightClampCount
	}
	if arts.verify.Report.Passed {
		summary.Status = "passed"
	} else {
		summary.Status = "numeric_mismatch"
	}
	return summary
}

func summaryAggregate(s Summary) (string, error) {
	view := struct {
		SchemaVersion       string               `json:"schema_version"`
		ManifestFingerprint string               `json:"manifest_fingerprint"`
		Status              string               `json:"status"`
		Limits              scheduler.Limits     `json:"limits"`
		Scheduler           scheduler.Plan       `json:"scheduler"`
		Shards              []ShardSummary       `json:"shards"`
		Untreated           []scheduler.Rejected `json:"untreated"`
	}{
		SchemaVersion: s.SchemaVersion, ManifestFingerprint: s.ManifestFingerprint,
		Status: s.Status, Limits: s.Limits, Scheduler: s.Scheduler,
		Shards: s.Shards, Untreated: s.Untreated,
	}
	return canon.Hash(view)
}

func marshalSummary(s Summary) ([]byte, error) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal summary: %w", err)
	}
	return append(b, '\n'), nil
}
