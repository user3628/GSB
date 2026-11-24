// Package samplegen creates deterministic local samples used by the acceptance
// procedure. It does not generate timestamps or random identifiers.
package samplegen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"replicationlag/internal/domain"
	"replicationlag/internal/format"
)

const (
	SourceOne = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	SourceTwo = "6f9d1a7e-8f5b-4cf4-a4a7-3bdfbc3f7d21"
)

func Generate(directory string) error {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	observations, err := canonicalObservations()
	if err != nil {
		return err
	}
	if err := writeObservationJSON(directory, observations); err != nil {
		return err
	}
	if err := writeShuffledJSONL(directory, observations); err != nil {
		return err
	}
	binaryData, err := format.EncodeBinary(observations)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "observations.bin"), binaryData, 0o644); err != nil {
		return err
	}
	legacyData, err := legacyObservationSet()
	if err != nil {
		return err
	}
	if err := writeLegacyJSONL(directory, legacyData); err != nil {
		return err
	}
	legacyBinary, err := format.EncodeBinaryWithOptions(legacyData, format.BinaryEncodeOptions{
		Version: 0, TimestampInSeconds: true, OmitReportedVersion: true,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "legacy.bin"), legacyBinary, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "compat.json"), compatJSON(), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "README.md"), []byte(sampleReadme), 0o644); err != nil {
		return err
	}
	return nil
}

func canonicalObservations() ([]domain.Observation, error) {
	type spec struct {
		replica  string
		time     int64
		segments []string
		lag      int64
	}
	specs := []spec{
		{"replica-a", 1700000000000, []string{SourceOne + ":1-3", "4-5"}, 120},
		{"replica-a", 1700000001000, []string{SourceOne + ":1-8"}, 1500},
		{"replica-a", 1700000002000, []string{SourceOne + ":1-8"}, 1800},
		{"replica-a", 1700000003000, []string{SourceOne + ":1-8"}, 200},
		{"replica-a", 1700000004000, []string{SourceOne + ":1-8"}, 100},
		{"replica-a", 1700000005000, []string{SourceOne + ":1-8"}, 100},
		{"replica-a", 1700000006000, []string{SourceOne + ":1-12"}, 100},
		{"replica-a", 1700000007000, []string{SourceOne + ":1-14"}, 50},
		{"replica-b", 1700000000500, []string{SourceOne + ":1-3", "4-10", SourceTwo + ":1-2"}, 6000},
		{"replica-b", 1700000001500, []string{SourceOne + ":1-12", SourceTwo + ":1-2"}, 1500},
		{"replica-b", 1700000002500, []string{SourceOne + ":1-11", SourceTwo + ":1-2"}, 100},
		{"replica-b", 1700000003500, []string{SourceOne + ":1-13", SourceTwo + ":1-3"}, 100},
		{"replica-b", 1700000004500, []string{SourceOne + ":1-15", SourceTwo + ":1-4"}, 50},
	}
	result := make([]domain.Observation, 0, len(specs))
	for index, item := range specs {
		gtid, err := domain.ParseGTID(strings.Join(item.segments, ","))
		if err != nil {
			return nil, err
		}
		version, _ := domain.ParseVersion("8.0.36")
		result = append(result, domain.Observation{
			ReplicaID: item.replica, SampledAtMS: item.time, GTID: gtid.Normalized(),
			PositionName: gtid.Normalized().String(), LagMS: item.lag, ReportedVersion: version,
			FormatVersion: 1, LastWriteMS: 1700200000000 + int64(index),
		})
	}
	return result, nil
}

func legacyObservationSet() ([]domain.Observation, error) {
	gtid1, err := domain.ParseGTID(SourceOne + ":1-5")
	if err != nil {
		return nil, err
	}
	gtid2, err := domain.ParseGTID(SourceOne + ":1-8")
	if err != nil {
		return nil, err
	}
	version, _ := domain.ParseVersion("5.7.0")
	return []domain.Observation{
		{
			ReplicaID: "legacy-a", SampledAtMS: 1700100000000, GTID: gtid1,
			PositionName: gtid1.String(), LagMS: 100, ReportedVersion: version,
			FormatVersion: 0, LastWriteMS: 1700300000000,
		},
		{
			ReplicaID: "legacy-a", SampledAtMS: 1700100010000, GTID: gtid2,
			PositionName: gtid2.String(), LagMS: 120, ReportedVersion: version,
			FormatVersion: 0, LastWriteMS: 1700300000001,
		},
	}, nil
}

func writeObservationJSON(directory string, observations []domain.Observation) error {
	type rawObservation struct {
		ReplicaID       string             `json:"replica_id"`
		SampledAtMS     int64              `json:"sampled_at_ms"`
		GTIDSegments    []string           `json:"gtid_segments"`
		GTIDIntervals   []domain.SourceSet `json:"gtid_intervals"`
		PositionName    string             `json:"position_name"`
		LagMS           int64              `json:"lag_ms"`
		ReportedVersion string             `json:"reported_version"`
	}
	shuffled := shuffleObservations(observations)
	items := make([]rawObservation, 0, len(shuffled))
	for _, observation := range shuffled {
		segments := make([]string, 0, len(observation.GTID.Sources))
		for _, source := range observation.GTID.Sources {
			segments = append(segments, source.Source+":"+intervalList(source.Intervals))
		}
		items = append(items, rawObservation{
			ReplicaID: observation.ReplicaID, SampledAtMS: observation.SampledAtMS,
			GTIDSegments: segments, GTIDIntervals: observation.GTID.Sources,
			PositionName: observation.PositionName, LagMS: observation.LagMS,
			ReportedVersion: observation.ReportedVersion.String(),
		})
	}
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, "observations.json"), append(data, '\n'), 0o644)
}

func writeShuffledJSONL(directory string, observations []domain.Observation) error {
	var buffer bytes.Buffer
	header := map[string]any{
		"format": "replication-observation-jsonl", "format_version": 1,
		"required_fields": []string{"replica_id", "sampled_at_ms", "gtid_segments", "position_name", "lag_ms", "reported_version"},
	}
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(header); err != nil {
		return err
	}
	for _, observation := range shuffleObservations(observations) {
		segments := make([]string, 0, len(observation.GTID.Sources))
		for _, source := range observation.GTID.Sources {
			segments = append(segments, source.Source+":"+intervalList(source.Intervals))
		}
		record := map[string]any{
			"replica_id": observation.ReplicaID, "sampled_at_ms": observation.SampledAtMS,
			"gtid_segments": segments, "position_name": observation.PositionName,
			"lag_ms": observation.LagMS, "reported_version": observation.ReportedVersion.String(),
			"last_write_ms": observation.LastWriteMS, "format_version": 1,
		}
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(directory, "observations.jsonl"), buffer.Bytes(), 0o644)
}

func writeLegacyJSONL(directory string, observations []domain.Observation) error {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	header := map[string]any{"format": "replication-observation-jsonl", "format_version": 0}
	if err := encoder.Encode(header); err != nil {
		return err
	}
	// This sample intentionally uses all four historical forms: second time
	// precision, integer range shorthand, tilde position syntax, and a missing
	// reported version.
	fmt.Fprintf(&buffer, "{\"replica_id\":%q,\"sampled_at\":%d,\"gtid_segments\":[\"1-5\"],\"position_name\":%q,\"lag_ms\":100}\n",
		observations[0].ReplicaID, observations[0].SampledAtMS/1000, SourceOne+":1~5")
	fmt.Fprintf(&buffer, "{\"replica_id\":%q,\"sampled_at\":%d,\"gtid_segments\":[\"1-8\"],\"position_name\":%q,\"lag_ms\":120}\n",
		observations[1].ReplicaID, observations[1].SampledAtMS/1000, SourceOne+":1~8")
	return os.WriteFile(filepath.Join(directory, "legacy.jsonl"), buffer.Bytes(), 0o644)
}

func shuffleObservations(observations []domain.Observation) []domain.Observation {
	out := append([]domain.Observation(nil), observations...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ReplicaID != out[j].ReplicaID {
			return out[i].ReplicaID > out[j].ReplicaID
		}
		return out[i].SampledAtMS > out[j].SampledAtMS
	})
	return out
}

func intervalList(intervals []domain.Interval) string {
	parts := make([]string, len(intervals))
	for i, interval := range intervals {
		parts[i] = interval.String()
	}
	return strings.Join(parts, ":")
}

func compatJSON() []byte {
	matrix := map[string]any{
		"rules": map[string]any{
			"jsonl/1":  map[string]any{"decision": "ACCEPT", "time_precision": "ms"},
			"binary/1": map[string]any{"decision": "ACCEPT", "time_precision": "ms"},
			"jsonl/0": map[string]any{
				"decision": "DEGRADE", "time_precision": "s", "default_version": "5.7.0",
				"allow_tilde_ranges": true, "allow_integer_shorthand": true,
				"default_gtid_source": SourceOne, "allow_missing_version": true,
			},
			"binary/0": map[string]any{
				"decision": "DEGRADE", "time_precision": "s", "default_version": "5.7.0",
				"allow_tilde_ranges": true, "allow_integer_shorthand": true,
				"default_gtid_source": SourceOne, "allow_missing_version": true,
			},
		},
		"default": map[string]any{"decision": "REJECT"},
		"known_required_fields": []string{
			"replica_id", "sampled_at_ms", "sampled_at", "gtid_segments", "gtid", "position_name",
			"executed_position", "lag_ms", "reported_version", "last_write_ms",
		},
	}
	data, _ := json.MarshalIndent(matrix, "", "  ")
	return append(data, '\n')
}

const sampleReadme = "# Deterministic samples\n\n" +
	"- observations.json: human-readable, intentionally not delivery-ordered observations.\n" +
	"- observations.jsonl: current JSON Lines v1 input, intentionally not delivery-ordered.\n" +
	"- observations.bin: current compact binary v1 representation of the same observations.\n" +
	"- legacy.jsonl: JSON Lines v0 with second timestamps, integer interval shorthand, tilde positions and no reported version.\n" +
	"- legacy.bin: compact binary v0 with second timestamps and no reported version.\n" +
	"- compat.json: explicit compatibility matrix enabling the legacy forms.\n\n" +
	"The canonical source UUID is 3e11fa47-71ca-11e1-9e33-c80aa9429562; a second source\n" +
	"exercises multi-source interval merging. Times and write timestamps are fixed.\n"
