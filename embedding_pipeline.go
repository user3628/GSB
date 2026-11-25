package speakerdiar

import (
	"math"
	"sort"
)

// RunEmbeddingStage creates voiceprints, assigns unique speakers, and arbitrates close frames.
func RunEmbeddingStage(chunk ChunkManifest, original *SpectralResult, sep *SeparationResult, cfg Config, injection Injection) (*EmbeddingResult, error) {
	cfg = cfg.withDefaults()
	rawDim := 2 * cfg.MelBands
	if cfg.ProjectionInputDim != 0 && cfg.ProjectionInputDim != rawDim {
		de := newError(CodeEmbedding, "configured projection dimension does not match mel statistic dimension")
		de.ChunkID = chunk.ChunkID
		de.Stage = StageEmbed
		de.Details = map[string]any{"configured_dim": cfg.ProjectionInputDim, "actual_dim": rawDim}
		return nil, de
	}
	if injection == InjectEmbeddingTie {
		_, _, _, err := ArbitrateFrame(ArbitrationRequest{
			ChunkID: chunk.ChunkID, FrameIndex: 0, SourceIndex: 0, Threshold: cfg.MarginThreshold,
			Scores: []SpeakerScore{{SpeakerID: "S1", Score: 0.91}, {SpeakerID: "S2", Score: 0.91}},
		})
		return nil, err
	}

	refMel := MelEnergies(original, cfg.MelBands)
	refFeatures := LocalStatisticsFeatures(refMel, 2)
	centroids, err := BuildCentroids(chunk, refFeatures, cfg)
	if err != nil {
		return nil, err
	}
	if len(centroids) == 0 {
		de := newError(CodeEmbedding, "manifest contains no reference speaker centroids")
		de.ChunkID = chunk.ChunkID
		de.Stage = StageEmbed
		return nil, de
	}

	result := &EmbeddingResult{ChunkID: chunk.ChunkID, Centroids: centroids}
	allPairs := make([]pairScore, 0, len(sep.Tracks)*len(centroids))
	sourceScores := make([][]SpeakerScore, len(sep.Tracks))
	sourceVectors := make([][]float64, len(sep.Tracks))

	for sourceIndex, track := range sep.Tracks {
		trackSpec, err := AnalyzeSpectrum(chunk.ChunkID, track.FloatTrack(), cfg)
		if err != nil {
			return nil, err
		}
		mel := MelEnergies(trackSpec, cfg.MelBands)
		active := make([]int, 0, len(mel))
		for f := range mel {
			if f < len(track.FrameRMS) && track.FrameRMS[f] >= cfg.VoiceEnergyThreshold {
				active = append(active, f)
			}
		}
		stats := NormalizeMelStatistics(GlobalStatistics(mel, active), cfg.MelBands)
		vec, err := HashProject(stats, cfg.ProjectionInputDim, cfg.EmbeddingDim, cfg.ProjectionSeed, chunk.ChunkID)
		if err != nil {
			return nil, err
		}
		sourceVectors[sourceIndex] = vec
		scores := scoreAgainstCentroids(vec, centroids)
		sourceScores[sourceIndex] = scores
		for _, sc := range scores {
			allPairs = append(allPairs, pairScore{source: sourceIndex, speaker: sc.SpeakerID, score: sc.Score})
		}
	}

	assignments := assignUniqueSpeakers(len(sep.Tracks), centroids, allPairs)
	result.Speakers = assignments
	assignmentBySource := map[int]SpeakerAssignment{}
	for _, a := range assignments {
		assignmentBySource[a.SourceIndex] = a
	}

	for sourceIndex, vec := range sourceVectors {
		a := assignmentBySource[sourceIndex]
		emb := SourceEmbedding{
			SourceIndex: sourceIndex, LocalNumber: sourceIndex + 1,
			SpeakerID: a.SpeakerID, Vector: vec, Similarities: sourceScores[sourceIndex],
		}
		emb.Digest = mustDigestValue(struct {
			SourceIndex int            `json:"source_index"`
			Vector      []float64      `json:"vector"`
			Scores      []SpeakerScore `json:"scores"`
		}{sourceIndex, vec, sourceScores[sourceIndex]})
		result.Embeddings = append(result.Embeddings, emb)
	}

	provisional := make([]FrameLabel, 0)
	for sourceIndex, track := range sep.Tracks {
		a := assignmentBySource[sourceIndex]
		trackSpec, err := AnalyzeSpectrum(chunk.ChunkID, track.FloatTrack(), cfg)
		if err != nil {
			return nil, err
		}
		mel := MelEnergies(trackSpec, cfg.MelBands)
		features := LocalStatisticsFeatures(mel, 2)
		for f := 0; f < len(trackSpec.Frames); f++ {
			energy := 0.0
			if f < len(track.FrameRMS) {
				energy = track.FrameRMS[f]
			}
			active := energy >= cfg.VoiceEnergyThreshold
			scores := []SpeakerScore{}
			margin := 2.0
			chosen := a.SpeakerID
			arbitrated := false
			if active && f < len(features) {
				feature := NormalizeMelStatistics(features[f], cfg.MelBands)
				vec, err := HashProject(feature, cfg.ProjectionInputDim, cfg.EmbeddingDim, cfg.ProjectionSeed, chunk.ChunkID)
				if err != nil {
					return nil, err
				}
				scores = scoreAgainstCentroids(vec, centroids)
				if len(scores) >= 2 {
					margin = scores[0].Score - scores[1].Score
				} else if len(scores) == 1 {
					margin = 1
				}
				if margin < cfg.MarginThreshold {
					arbitrated = true
				} else if len(scores) > 0 {
					chosen = scores[0].SpeakerID
				}
			}
			confidence := 0.0
			if len(scores) > 0 {
				confidence = clip01(0.5 + 0.5*scores[0].Score)
			}
			provisional = append(provisional, FrameLabel{
				FrameIndex: f, StartMS: f * cfg.HopDurationMS,
				EndMS: (f + 1) * cfg.HopDurationMS, SourceIndex: sourceIndex,
				LocalNumber: sourceIndex + 1, SpeakerID: chosen, Energy: energy,
				Active: active, Confidence: confidence, Margin: margin,
				Arbitrated: arbitrated, Scores: scores,
			})
		}
	}

	labelsBySource := map[int][]int{}
	for i, label := range provisional {
		labelsBySource[label.SourceIndex] = append(labelsBySource[label.SourceIndex], i)
	}
	for i := range provisional {
		label := &provisional[i]
		if !label.Active || !label.Arbitrated {
			result.FrameLabels = append(result.FrameLabels, *label)
			continue
		}
		votes := map[string]int{}
		for _, j := range labelsBySource[label.SourceIndex] {
			other := provisional[j]
			if other.FrameIndex >= label.FrameIndex-5 && other.FrameIndex <= label.FrameIndex+5 &&
				other.FrameIndex != label.FrameIndex && other.Active && other.Margin >= cfg.MarginThreshold {
				votes[other.SpeakerID]++
			}
		}
		chosen, reason, evidence, err := ArbitrateFrame(ArbitrationRequest{
			ChunkID: chunk.ChunkID, FrameIndex: label.FrameIndex, StartMS: label.StartMS,
			SourceIndex: label.SourceIndex, Scores: label.Scores, Threshold: cfg.MarginThreshold,
			SourceSpeaker: assignmentBySource[label.SourceIndex].SpeakerID, NeighborVotes: votes,
		})
		if err != nil {
			return nil, err
		}
		label.SpeakerID = chosen
		label.Arbitrated = true
		if label.Confidence == 0 && len(label.Scores) > 0 {
			label.Confidence = clip01(0.5 + 0.5*label.Scores[0].Score)
		}
		result.Arbitrations = append(result.Arbitrations, ArbitrationRecord{
			FrameIndex: label.FrameIndex, StartMS: label.StartMS, SourceIndex: label.SourceIndex,
			Scores: label.Scores, Margin: label.Margin, Threshold: cfg.MarginThreshold,
			Chosen: chosen, Reason: reason, Evidence: evidence,
		})
		result.FrameLabels = append(result.FrameLabels, *label)
	}

	sort.SliceStable(result.Arbitrations, func(i, j int) bool {
		if result.Arbitrations[i].FrameIndex != result.Arbitrations[j].FrameIndex {
			return result.Arbitrations[i].FrameIndex < result.Arbitrations[j].FrameIndex
		}
		return result.Arbitrations[i].SourceIndex < result.Arbitrations[j].SourceIndex
	})
	sort.SliceStable(result.FrameLabels, func(i, j int) bool {
		if result.FrameLabels[i].FrameIndex != result.FrameLabels[j].FrameIndex {
			return result.FrameLabels[i].FrameIndex < result.FrameLabels[j].FrameIndex
		}
		return result.FrameLabels[i].SourceIndex < result.FrameLabels[j].SourceIndex
	})

	d, _, err := DigestValue(embeddingDigestData{
		ChunkID: result.ChunkID, Speakers: result.Speakers, Embeddings: result.Embeddings,
		FrameLabels: result.FrameLabels, Arbitrations: result.Arbitrations,
	})
	if err != nil {
		return nil, err
	}
	result.Digest = d
	return result, nil
}

type pairScore struct {
	source  int
	speaker string
	score   float64
}

func scoreAgainstCentroids(vec []float64, centroids []SpeakerCentroid) []SpeakerScore {
	out := make([]SpeakerScore, 0, len(centroids))
	for _, c := range centroids {
		out = append(out, SpeakerScore{SpeakerID: c.SpeakerID, Score: cosine(vec, c.Vector)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].SpeakerID < out[j].SpeakerID
	})
	return out
}

func assignUniqueSpeakers(sourceCount int, centroids []SpeakerCentroid, pairs []pairScore) []SpeakerAssignment {
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].score != pairs[j].score {
			return pairs[i].score > pairs[j].score
		}
		if pairs[i].source != pairs[j].source {
			return pairs[i].source < pairs[j].source
		}
		return pairs[i].speaker < pairs[j].speaker
	})
	usedSource := map[int]bool{}
	usedSpeaker := map[string]bool{}
	out := make([]SpeakerAssignment, 0, sourceCount)
	for _, p := range pairs {
		if usedSource[p.source] || usedSpeaker[p.speaker] {
			continue
		}
		usedSource[p.source] = true
		usedSpeaker[p.speaker] = true
		out = append(out, SpeakerAssignment{
			LocalNumber: p.source + 1, SourceIndex: p.source, SpeakerID: p.speaker, Similarity: p.score,
		})
	}
	// Deterministic fallback for pathological missing scores; numbering stays contiguous.
	for source := 0; source < sourceCount; source++ {
		if usedSource[source] {
			continue
		}
		for _, c := range centroids {
			if !usedSpeaker[c.SpeakerID] {
				usedSource[source] = true
				usedSpeaker[c.SpeakerID] = true
				out = append(out, SpeakerAssignment{LocalNumber: source + 1, SourceIndex: source, SpeakerID: c.SpeakerID})
				break
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SourceIndex < out[j].SourceIndex })
	return out
}

func clip01(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	return math.Max(0, math.Min(1, v))
}
