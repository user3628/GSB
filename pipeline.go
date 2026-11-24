package cockpitaudio

import (
	"os"
	"path/filepath"
)

type ShardResult struct {
	ShardID          string            `json:"shard_id"`
	ShardFingerprint string            `json:"shard_fingerprint"`
	VAD              *ShardVAD         `json:"vad"`
	Attribution      *ShardAttribution `json:"attribution"`
}

type Summary struct {
	ShardCount        int            `json:"shard_count"`
	ZoneCount         int            `json:"zone_count"`
	IntervalCount     int            `json:"interval_count"`
	OverlapCount      int            `json:"overlap_count"`
	CommandCount      int            `json:"command_count"`
	ResolvedCount     int            `json:"resolved_count"`
	AmbiguousCount    int            `json:"ambiguous_count"`
	ResponsibleByZone map[string]int `json:"responsible_by_zone"`
	MaxEditVersion    int            `json:"max_edit_version"`
	ConfigVersion     string         `json:"config_version"`
	ConfigFingerprint string         `json:"config_fingerprint"`
}

type SessionResult struct {
	SessionID         string        `json:"session_id"`
	InputFingerprint  string        `json:"input_fingerprint"`
	ConfigVersion     string        `json:"config_version"`
	ConfigFingerprint string        `json:"config_fingerprint"`
	EditVersion       int           `json:"edit_version"`
	EditLog           []EditRecord  `json:"edit_log"`
	Shards            []ShardResult `json:"shards"`
	Timeline          Timeline      `json:"timeline"`
	Summary           Summary       `json:"summary"`
}

func ShardFingerprint(shard *ShardManifest, wavBytes []byte) (string, error) {
	wavSum := FingerprintBytes("wav:", wavBytes)
	return Fingerprint(struct {
		Shard     *ShardManifest `json:"shard"`
		WAVSHA256 string         `json:"wav_sha256"`
	}{Shard: shard, WAVSHA256: wavSum})
}

func InputFingerprint(ds *Dataset) (string, error) {
	parts := make([]string, 0, len(ds.Manifest.Shards))
	for i := range ds.Manifest.Shards {
		shard := ds.Manifest.Shards[i]
		b, err := os.ReadFile(filepath.Join(ds.Root, shard.WAVFile))
		if err != nil {
			return "", err
		}
		fp, err := ShardFingerprint(&shard, b)
		if err != nil {
			return "", err
		}
		parts = append(parts, fp)
	}
	return Fingerprint(struct {
		Manifest *SessionManifest `json:"manifest"`
		Parts    []string         `json:"parts"`
	}{Manifest: ds.Manifest, Parts: parts})
}

func BuildSession(ds *Dataset, cfg Config, edit *EditState) (*SessionResult, *Error) {
	configShardID := ""
	if ds != nil && len(ds.Manifest.Shards) > 0 {
		configShardID = ds.Manifest.Shards[0].ShardID
	}
	resolved, verr := cfg.ResolveVAD(configShardID)
	if verr != nil {
		return nil, verr
	}
	inputFP, err := InputFingerprint(ds)
	if err != nil {
		return nil, NewError(CodeInternal, StageIngest, "", "input fingerprint: %v", err)
	}
	results, baseIntervals, verr := ComputeVADStages(ds, cfg)
	if verr != nil {
		return nil, verr
	}
	return AssembleSessionFromVAD(ds, cfg, edit, results, inputFP, resolved, baseIntervals)
}

func ComputeVADStages(ds *Dataset, cfg Config) ([]ShardResult, []Interval, *Error) {
	baseIntervals := []Interval{}
	results := make([]ShardResult, 0, len(ds.Manifest.Shards))

	for i := range ds.Manifest.Shards {
		shard := ds.Manifest.Shards[i]
		wavPath := filepath.Join(ds.Root, shard.WAVFile)
		wavBytes, err := os.ReadFile(wavPath)
		if err != nil {
			return nil, nil, NewError(CodeInternal, StageIngest, shard.ShardID, "read wav: %v", err)
		}
		wav, err := ReadWAV(wavPath)
		if err != nil {
			return nil, nil, NewError(CodeInternal, StageIngest, shard.ShardID, "parse wav: %v", err)
		}
		fp, err := ShardFingerprint(&shard, wavBytes)
		if err != nil {
			return nil, nil, NewError(CodeInternal, StageIngest, shard.ShardID, "fingerprint: %v", err)
		}
		vad, verr := RunVAD(&shard, wav, cfg)
		if verr != nil {
			return nil, nil, verr
		}
		vad.ShardFingerprint = fp
		_ = wav
		_ = wavBytes
		for _, z := range vad.Zones {
			baseIntervals = append(baseIntervals, z.Intervals...)
		}
		results = append(results, ShardResult{ShardID: shard.ShardID, ShardFingerprint: fp, VAD: vad})
	}
	return results, baseIntervals, nil
}

func AssembleSessionFromVAD(ds *Dataset, cfg Config, edit *EditState, results []ShardResult, inputFP string, resolved ResolvedConfig, baseIntervals []Interval) (*SessionResult, *Error) {
	if edit == nil {
		edit = NewEditState(baseIntervals, cfg.Priorities)
	}
	editVersion := edit.CurrentVersion()
	effective := edit.EffectiveIntervals()
	priorityOverrides := edit.PriorityOverrideMap()
	hits := edit.HitOverrides()

	allAttributions := []Attribution{}
	commandCount, resolvedCount, ambiguousCount := 0, 0, 0
	responsible := map[string]int{}
	for i := range ds.Manifest.Shards {
		shard := ds.Manifest.Shards[i]
		vad := results[i].VAD
		attr, aerr := RunAttribution(&shard, vad, cfg, AttributionOptions{
			Intervals: effective, PriorityOverrides: priorityOverrides, HitOverrides: hits, EditVersion: editVersion,
		})
		if aerr != nil {
			return nil, aerr
		}
		results[i].Attribution = attr
		for _, item := range attr.Items {
			allAttributions = append(allAttributions, item)
			commandCount++
			if item.Ambiguous {
				ambiguousCount++
			} else {
				resolvedCount++
				responsible[item.ResponsibleZoneID]++
			}
		}
	}

	timeline := BuildTimeline(edit.TimelineIntervals(), allAttributions)
	zoneCount := len(ds.Manifest.Shards) * len(ZoneOrder)
	summary := Summary{
		ShardCount: len(ds.Manifest.Shards), ZoneCount: zoneCount,
		IntervalCount: len(timeline.Intervals), OverlapCount: len(timeline.Overlaps),
		CommandCount: commandCount, ResolvedCount: resolvedCount, AmbiguousCount: ambiguousCount,
		ResponsibleByZone: responsible, MaxEditVersion: editVersion,
		ConfigVersion: resolved.ConfigVersion, ConfigFingerprint: resolved.ConfigFingerprint,
	}

	return &SessionResult{
		SessionID: ds.Manifest.SessionID, InputFingerprint: inputFP,
		ConfigVersion: resolved.ConfigVersion, ConfigFingerprint: resolved.ConfigFingerprint,
		EditVersion: editVersion, EditLog: edit.LogCopy(), Shards: results, Timeline: timeline, Summary: summary,
	}, nil
}
