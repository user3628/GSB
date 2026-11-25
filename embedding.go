package speakerdiar

import (
	"fmt"
	"math"
	"sort"
)

// SpeakerScore is a cosine score against one manifest centroid.
type SpeakerScore struct {
	SpeakerID string  `json:"speaker_id"`
	Score     float64 `json:"score"`
}

// SpeakerAssignment maps one separated source to a unique manifest speaker.
type SpeakerAssignment struct {
	LocalNumber int     `json:"local_number"`
	SourceIndex int     `json:"source_index"`
	SpeakerID   string  `json:"speaker_id"`
	Similarity  float64 `json:"similarity"`
}

// SourceEmbedding contains the deterministic hashed voiceprint vector.
type SourceEmbedding struct {
	SourceIndex  int            `json:"source_index"`
	LocalNumber  int            `json:"local_number"`
	SpeakerID    string         `json:"speaker_id"`
	Vector       []float64      `json:"vector"`
	Similarities []SpeakerScore `json:"similarities"`
	Digest       string         `json:"digest"`
}

// SpeakerCentroid is derived only from manifest-declared reference intervals.
type SpeakerCentroid struct {
	SpeakerID  string    `json:"speaker_id"`
	Vector     []float64 `json:"vector"`
	FrameCount int       `json:"frame_count"`
}

// ArbitrationRecord explains every close-margin frame decision.
type ArbitrationRecord struct {
	FrameIndex  int            `json:"frame_index"`
	StartMS     int            `json:"start_ms"`
	SourceIndex int            `json:"source_index"`
	Scores      []SpeakerScore `json:"scores"`
	Margin      float64        `json:"margin"`
	Threshold   float64        `json:"threshold"`
	Chosen      string         `json:"chosen"`
	Reason      string         `json:"reason"`
	Evidence    []string       `json:"evidence"`
}

// FrameLabel is the per-frame input to timeline construction.
type FrameLabel struct {
	FrameIndex  int            `json:"frame_index"`
	StartMS     int            `json:"start_ms"`
	EndMS       int            `json:"end_ms"`
	SourceIndex int            `json:"source_index"`
	LocalNumber int            `json:"local_number"`
	SpeakerID   string         `json:"speaker_id"`
	Energy      float64        `json:"energy"`
	Active      bool           `json:"active"`
	Confidence  float64        `json:"confidence"`
	Margin      float64        `json:"margin"`
	Arbitrated  bool           `json:"arbitrated"`
	Scores      []SpeakerScore `json:"scores"`
}

// EmbeddingResult is the third pipeline stage product.
type EmbeddingResult struct {
	ChunkID      string              `json:"chunk_id"`
	Speakers     []SpeakerAssignment `json:"speakers"`
	Embeddings   []SourceEmbedding   `json:"embeddings"`
	Centroids    []SpeakerCentroid   `json:"centroids"`
	FrameLabels  []FrameLabel        `json:"frame_labels"`
	Arbitrations []ArbitrationRecord `json:"arbitrations"`
	Digest       string              `json:"digest"`
}

type embeddingDigestData struct {
	ChunkID      string              `json:"chunk_id"`
	Speakers     []SpeakerAssignment `json:"speakers"`
	Embeddings   []SourceEmbedding   `json:"embeddings"`
	FrameLabels  []FrameLabel        `json:"frame_labels"`
	Arbitrations []ArbitrationRecord `json:"arbitrations"`
}

func hzToMel(hz float64) float64  { return 2595 * math.Log10(1+hz/700) }
func melToHZ(mel float64) float64 { return 700 * (math.Pow(10, mel/2595) - 1) }

func melFilterWeights(bandCount, binCount, fftSize, rate int) [][]float64 {
	lowMel := hzToMel(80)
	highMel := hzToMel(float64(rate) / 2)
	points := make([]int, bandCount+2)
	for i := 0; i < bandCount+2; i++ {
		mel := lowMel + (highMel-lowMel)*float64(i)/float64(bandCount+1)
		hz := melToHZ(mel)
		points[i] = int(math.Floor((float64(fftSize) + 1) * hz / float64(rate)))
	}
	filters := make([][]float64, bandCount)
	for band := 0; band < bandCount; band++ {
		filters[band] = make([]float64, binCount)
		left, center, right := points[band], points[band+1], points[band+2]
		for b := 0; b < binCount; b++ {
			switch {
			case b >= left && b <= center && center != left:
				filters[band][b] = float64(b-left) / float64(center-left)
			case b > center && b <= right && right != center:
				filters[band][b] = float64(right-b) / float64(right-center)
			}
		}
	}
	return filters
}

// MelEnergies computes deterministic log mel-band energies for every frame.
func MelEnergies(spec *SpectralResult, bandCount int) [][]float64 {
	binCount := spec.FFTSize/2 + 1
	filters := melFilterWeights(bandCount, binCount, spec.FFTSize, spec.SampleRate)
	out := make([][]float64, len(spec.Frames))
	for f, frame := range spec.Frames {
		out[f] = make([]float64, bandCount)
		for band := 0; band < bandCount; band++ {
			power := 0.0
			for b := 0; b < binCount; b++ {
				p := frame.Magnitudes[b] * frame.Magnitudes[b]
				power += filters[band][b] * p
			}
			out[f][band] = math.Log(power + 1e-10)
		}
	}
	return out
}

// LocalStatisticsFeatures builds per-frame mean/std mel statistics in a centered window.
func LocalStatisticsFeatures(mel [][]float64, radius int) [][]float64 {
	if len(mel) == 0 {
		return nil
	}
	bands := len(mel[0])
	out := make([][]float64, len(mel))
	for f := range mel {
		start := maxInt(0, f-radius)
		end := minInt(len(mel), f+radius+1)
		count := float64(end - start)
		means := make([]float64, bands)
		for i := start; i < end; i++ {
			for b := 0; b < bands; b++ {
				means[b] += mel[i][b]
			}
		}
		for b := range means {
			means[b] /= count
		}
		stds := make([]float64, bands)
		for i := start; i < end; i++ {
			for b := 0; b < bands; b++ {
				d := mel[i][b] - means[b]
				stds[b] += d * d
			}
		}
		features := make([]float64, 0, 2*bands)
		for b := 0; b < bands; b++ {
			stds[b] = math.Sqrt(stds[b] / count)
			features = append(features, means[b])
		}
		for b := 0; b < bands; b++ {
			features = append(features, stds[b])
		}
		out[f] = features
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// HashProject applies a deterministic hash-derived random projection.
func HashProject(features []float64, inputDim, outputDim int, seed uint64, chunkID string) ([]float64, error) {
	if inputDim <= 0 {
		inputDim = len(features)
	}
	if inputDim != len(features) {
		de := newError(CodeEmbedding, "voiceprint projection input dimension mismatch")
		de.ChunkID = chunkID
		de.Stage = StageEmbed
		de.Details = map[string]any{"expected_input_dim": inputDim, "actual_dim": len(features), "output_dim": outputDim}
		return nil, de
	}
	if outputDim <= 0 {
		de := newError(CodeEmbedding, "voiceprint projection output dimension must be positive")
		de.ChunkID = chunkID
		de.Stage = StageEmbed
		return nil, de
	}
	out := make([]float64, outputDim)
	scale := 1.0 / math.Sqrt(float64(inputDim))
	for j := 0; j < outputDim; j++ {
		sum := 0.0
		for i, v := range features {
			h := splitMix64(seed ^ uint64(j)*0x9E3779B185EBCA87 ^ uint64(i)*0xC2B2AE3D27D4EB4F)
			u := float64(h>>11) / float64(uint64(1)<<53)
			w := 2*u - 1
			sum += w * v * scale
		}
		out[j] = sum
	}
	if err := validateVectorFinite(out, chunkID); err != nil {
		return nil, err
	}
	normalizeL2(out)
	return out, nil
}

func validateVectorFinite(v []float64, chunkID string) error {
	for i, x := range v {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			de := newError(CodeEmbedding, "embedding contains NaN or Inf")
			de.ChunkID = chunkID
			de.Stage = StageEmbed
			de.FrameIndex = i
			return de
		}
	}
	return nil
}

func normalizeL2(v []float64) float64 {
	norm := 0.0
	for _, x := range v {
		norm += x * x
	}
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range v {
			v[i] /= norm
		}
	}
	return norm
}

func cosine(a, b []float64) float64 {
	dot, na, nb := 0.0, 0.0, 0.0
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// GlobalStatistics returns concatenated mel mean and standard deviation.
func GlobalStatistics(mel [][]float64, active []int) []float64 {
	if len(mel) == 0 {
		return nil
	}
	bands := len(mel[0])
	indices := active
	if len(indices) == 0 {
		indices = make([]int, len(mel))
		for i := range mel {
			indices[i] = i
		}
	}
	means := make([]float64, bands)
	for _, f := range indices {
		for b := 0; b < bands; b++ {
			means[b] += mel[f][b]
		}
	}
	n := float64(len(indices))
	for b := range means {
		means[b] /= n
	}
	stds := make([]float64, bands)
	for _, f := range indices {
		for b := 0; b < bands; b++ {
			d := mel[f][b] - means[b]
			stds[b] += d * d
		}
	}
	out := make([]float64, 0, 2*bands)
	for b := 0; b < bands; b++ {
		out = append(out, means[b])
	}
	for b := 0; b < bands; b++ {
		out = append(out, math.Sqrt(stds[b]/n))
	}
	return out
}

// NormalizeMelStatistics removes absolute level and balances mean/variance blocks.
func NormalizeMelStatistics(v []float64, bands int) []float64 {
	if len(v) != 2*bands {
		return append([]float64(nil), v...)
	}
	out := make([]float64, len(v))
	means := append([]float64(nil), v[:bands]...)
	meanCenter := 0.0
	for _, x := range means {
		meanCenter += x
	}
	meanCenter /= float64(bands)
	for b := range means {
		means[b] -= meanCenter
	}
	normalizeL2(means)

	stds := append([]float64(nil), v[bands:]...)
	stdCenter := 0.0
	for _, x := range stds {
		stdCenter += x
	}
	stdCenter /= float64(bands)
	for b := range stds {
		stds[b] -= stdCenter
	}
	normalizeL2(stds)
	copy(out[:bands], means)
	copy(out[bands:], stds)
	return out
}

// BuildCentroids computes projected reference-segment centroids from original audio features.
func BuildCentroids(chunk ChunkManifest, refFeatures [][]float64, cfg Config) ([]SpeakerCentroid, error) {
	inputDim := 2 * cfg.MelBands
	type accum struct {
		sum    []float64
		weight float64
		count  int
	}
	accs := map[string]*accum{}
	order := []string{}
	for _, ref := range chunk.ReferenceSegments {
		if _, ok := accs[ref.SpeakerID]; !ok {
			accs[ref.SpeakerID] = &accum{sum: make([]float64, cfg.EmbeddingDim)}
			order = append(order, ref.SpeakerID)
		}
		a := accs[ref.SpeakerID]
		weight := math.Max(ref.Confidence, 0.1)
		if ref.Trusted {
			weight += 0.25
		}
		for f, feat := range refFeatures {
			centerMS := f * cfg.HopDurationMS // feature index follows STFT/hop grid; center offset is stable.
			_ = centerMS
			if frameInsideReference(f, cfg, ref) {
				normalized := NormalizeMelStatistics(feat, cfg.MelBands)
				vec, err := HashProject(normalized, inputDim, cfg.EmbeddingDim, cfg.ProjectionSeed, chunk.ChunkID)
				if err != nil {
					return nil, err
				}
				for j := range vec {
					a.sum[j] += vec[j] * weight
				}
				a.weight += weight
				a.count++
			}
		}
	}
	sort.Strings(order)
	centroids := make([]SpeakerCentroid, 0, len(order))
	for _, id := range order {
		a := accs[id]
		if a.count == 0 {
			de := newError(CodeEmbedding, "reference speaker has no analyzable frames")
			de.ChunkID = chunk.ChunkID
			de.Stage = StageEmbed
			de.Details = map[string]any{"speaker_id": id}
			return nil, de
		}
		vec := make([]float64, cfg.EmbeddingDim)
		for j := range a.sum {
			vec[j] = a.sum[j] / a.weight
		}
		normalizeL2(vec)
		centroids = append(centroids, SpeakerCentroid{SpeakerID: id, Vector: vec, FrameCount: a.count})
	}
	return centroids, nil
}

func frameInsideReference(frame int, cfg Config, ref ReferenceSegment) bool {
	startMS := frame * cfg.HopDurationMS
	endMS := (frame + 1) * cfg.HopDurationMS
	return startMS < ref.EndMS && endMS > ref.StartMS
}

// ArbitrationRequest contains all deterministic evidence for a close frame.
type ArbitrationRequest struct {
	ChunkID       string
	FrameIndex    int
	StartMS       int
	SourceIndex   int
	Scores        []SpeakerScore
	Threshold     float64
	SourceSpeaker string
	NeighborVotes map[string]int
}

// ArbitrateFrame resolves close cosine margins or returns E_EMBEDDING on an evidence-free tie.
func ArbitrateFrame(req ArbitrationRequest) (string, string, []string, error) {
	scores := append([]SpeakerScore(nil), req.Scores...)
	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].Score != scores[j].Score {
			return scores[i].Score > scores[j].Score
		}
		return scores[i].SpeakerID < scores[j].SpeakerID
	})
	if len(scores) == 0 {
		de := newError(CodeEmbedding, "no similarity scores for arbitration")
		de.ChunkID = req.ChunkID
		de.Stage = StageEmbed
		de.FrameIndex = req.FrameIndex
		de.SourceIndex = req.SourceIndex
		return "", "", nil, de
	}
	for _, s := range scores {
		if math.IsNaN(s.Score) || math.IsInf(s.Score, 0) {
			de := newError(CodeEmbedding, "similarity is non-finite")
			de.ChunkID = req.ChunkID
			de.Stage = StageEmbed
			de.FrameIndex = req.FrameIndex
			return "", "", nil, de
		}
	}
	top := scores[0]
	second := SpeakerScore{}
	if len(scores) > 1 {
		second = scores[1]
	}
	margin := top.Score - second.Score
	evidence := []string{fmt.Sprintf("top=%s:%.6f", top.SpeakerID, top.Score), fmt.Sprintf("margin=%.6f", margin)}
	if margin >= req.Threshold && top.Score != second.Score {
		return top.SpeakerID, "clear_nearest_centroid", evidence, nil
	}

	tied := []string{top.SpeakerID}
	for _, s := range scores[1:] {
		if s.Score == top.Score {
			tied = append(tied, s.SpeakerID)
		}
	}
	if len(tied) > 1 {
		evidence = append(evidence, "exact_tie="+fmt.Sprint(tied))
		if req.SourceSpeaker != "" {
			for _, id := range tied {
				if id == req.SourceSpeaker {
					evidence = append(evidence, "source_global_assignment")
					return id, "source_assignment_breaks_tie", evidence, nil
				}
			}
		}
		bestVotes := 0
		voteSpeaker := ""
		tieVotes := false
		for id, votes := range req.NeighborVotes {
			if votes > bestVotes {
				bestVotes = votes
				voteSpeaker = id
				tieVotes = false
			} else if votes == bestVotes && votes > 0 {
				tieVotes = true
			}
		}
		if voteSpeaker != "" && !tieVotes {
			evidence = append(evidence, fmt.Sprintf("neighbor_majority=%s:%d", voteSpeaker, bestVotes))
			return voteSpeaker, "neighbor_majority_breaks_tie", evidence, nil
		}
		de := newError(CodeEmbedding, "identical embedding similarity without arbitration evidence")
		de.ChunkID = req.ChunkID
		de.Stage = StageEmbed
		de.FrameIndex = req.FrameIndex
		de.SourceIndex = req.SourceIndex
		de.Details = map[string]any{"tied_speakers": tied}
		return "", "", evidence, de
	}

	if req.SourceSpeaker != "" {
		evidence = append(evidence, "source_global_assignment")
		return req.SourceSpeaker, "close_margin_source_consistency", evidence, nil
	}
	if len(req.NeighborVotes) > 0 {
		bestVotes := -1
		voteSpeaker := ""
		tieVotes := false
		for id, votes := range req.NeighborVotes {
			if votes > bestVotes {
				bestVotes, voteSpeaker, tieVotes = votes, id, false
			} else if votes == bestVotes {
				tieVotes = true
			}
		}
		if !tieVotes && voteSpeaker != "" {
			evidence = append(evidence, fmt.Sprintf("neighbor_majority=%s:%d", voteSpeaker, bestVotes))
			return voteSpeaker, "close_margin_neighbor_majority", evidence, nil
		}
	}
	return top.SpeakerID, "deterministic_score_order", evidence, nil
}
