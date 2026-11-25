package ttsflow

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

const (
	splitmixGamma = uint64(0x9e3779b97f4a7c15)
	floatScale    = 1.0 / float64(uint64(1)<<53)
)

func mix64(x uint64) uint64 {
	x += splitmixGamma
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func hash64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func unitFloat(seed, index uint64) float64 {
	u := mix64(seed + index*splitmixGamma)
	return (float64(u>>11)*floatScale)*2.0 - 1.0
}

func unitFrameCount(unit Unit, sample TextSample, cfg Config) int {
	span := cfg.Acoustic.MaxFramesPerUnit - cfg.Acoustic.MinFramesPerUnit + 1
	if span <= 1 {
		return cfg.Acoustic.MinFramesPerUnit
	}
	seed := hash64(unit.Phoneme + "\x00" + unit.Symbol + "\x00" + sample.ControlDigest())
	return cfg.Acoustic.MinFramesPerUnit + int(seed%uint64(span))
}

func layerWeights(weightSeed int64, layerIndex, in, out int) []float64 {
	seed := mix64(uint64(weightSeed) ^ mix64(uint64(layerIndex+1)*0x9e3779b97f4a7c15) ^ uint64(in)<<32 ^ uint64(out))
	weights := make([]float64, in*out)
	for i := range weights {
		weights[i] = unitFloat(seed, uint64(i)) * 0.35
	}
	return weights
}

func layerBias(weightSeed int64, layerIndex, out int) []float64 {
	seed := mix64(uint64(weightSeed) ^ uint64(layerIndex+1)*0xd1b54a32d192ed03 ^ uint64(out))
	bias := make([]float64, out)
	for i := range bias {
		bias[i] = unitFloat(seed, uint64(i)) * 0.1
	}
	return bias
}

func networkInput(unit Unit, sample TextSample, cfg Config, frameInUnit int) []float64 {
	seedText := fmt.Sprintf("%s\x00%s\x00%s\x00%d", unit.Symbol, unit.Phoneme, sample.ControlDigest(), sample.SpeakerID)
	baseSeed := mix64(uint64(cfg.Acoustic.WeightSeed) ^ hash64(seedText))
	in := make([]float64, cfg.Acoustic.EmbeddingDim)
	for i := range in {
		// The per-unit frame ordinal intentionally excludes any global frame or
		// chunk offset. Chunk-local prediction can therefore be concatenated
		// byte-for-byte with whole-utterance prediction.
		in[i] = unitFloat(baseSeed, uint64(i*997+frameInUnit*17))
	}
	p := sample.Prosody
	controls := []float64{p.Rate - 1, p.Pitch - 1, p.Energy - 1, p.Emphasis, p.PitchCurve}
	for i, v := range controls {
		in[i%len(in)] += v
	}
	return in
}

func forwardNetwork(input []float64, cfg AcousticConfig) []float64 {
	current := append([]float64(nil), input...)
	for li, layer := range cfg.Layers {
		weights := layerWeights(cfg.WeightSeed, li, layer.In, layer.Out)
		bias := layerBias(cfg.WeightSeed, li, layer.Out)
		next := make([]float64, layer.Out)
		for o := 0; o < layer.Out; o++ {
			sum := bias[o]
			row := o * layer.In
			for i := 0; i < layer.In; i++ {
				sum += current[i] * weights[row+i]
			}
			next[o] = math.Tanh(sum)
		}
		current = next
	}
	return current
}

func predictFrame(unit Unit, sample TextSample, cfg Config, frameInUnit int) AcousticFrame {
	raw := forwardNetwork(networkInput(unit, sample, cfg, frameInUnit), cfg.Acoustic)
	frame := AcousticFrame{Energies: make([]float64, cfg.Acoustic.Bands)}
	for b := 0; b < cfg.Acoustic.Bands; b++ {
		frame.Energies[b] = 0.02 + 1.35*(raw[b]+1.0)/2.0
	}
	f0raw := raw[cfg.Acoustic.Bands]
	frame.F0 = 72.0 + 340.0/(1.0+math.Exp(-2.2*f0raw))
	frame.F0 *= 1.0 + 0.15*(sample.Prosody.Pitch-1.0)
	frame.F0 += 12.0 * math.Sin(float64(frameInUnit+1)*0.7+sample.Prosody.PitchCurve)
	return frame
}

func encodeAcousticFrames(frames []AcousticFrame, uttID string, segmentSeq, frameStart int) ([]byte, error) {
	bands := -1
	for i, frame := range frames {
		if bands < 0 {
			bands = len(frame.Energies)
		}
		if len(frame.Energies) != bands {
			return nil, &BizError{
				Code: CodeNumeric, Message: "frame band count changed",
				UTTID: uttID, SegmentSeq: segmentSeq, Stage: StageAcoustic.String(), FrameNo: frameStart + i,
			}
		}
		if !finite(frame.F0) {
			return nil, &BizError{
				Code: CodeNumeric, Message: "non-finite F0", UTTID: uttID,
				SegmentSeq: segmentSeq, Stage: StageAcoustic.String(), FrameNo: frameStart + i,
			}
		}
		for _, energy := range frame.Energies {
			if !finite(energy) {
				return nil, &BizError{
					Code: CodeNumeric, Message: "non-finite band energy", UTTID: uttID,
					SegmentSeq: segmentSeq, Stage: StageAcoustic.String(), FrameNo: frameStart + i,
				}
			}
		}
	}
	out := make([]byte, 0, len(frames)*(bands+1)*8)
	var buf [8]byte
	for _, frame := range frames {
		for _, energy := range frame.Energies {
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(energy))
			out = append(out, buf[:]...)
		}
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(frame.F0))
		out = append(out, buf[:]...)
	}
	return out, nil
}

func decodeAcousticFrames(data []byte, bands int) ([]AcousticFrame, error) {
	if bands <= 0 || len(data)%((bands+1)*8) != 0 {
		return nil, fmt.Errorf("invalid acoustic payload length %d for %d bands", len(data), bands)
	}
	count := len(data) / ((bands + 1) * 8)
	frames := make([]AcousticFrame, count)
	offset := 0
	for i := 0; i < count; i++ {
		frame := AcousticFrame{Energies: make([]float64, bands)}
		for b := 0; b < bands; b++ {
			frame.Energies[b] = math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8
		}
		frame.F0 = math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
		frames[i] = frame
	}
	return frames, nil
}

// PredictFrames predicts a flat sequence of units. No chunk or global frame
// index influences frame values, making arbitrary legal chunking produce the
// same concatenated bytes.
func PredictFrames(sample TextSample, units []Unit, cfg Config, startFrame int) ([]AcousticSegment, []byte, error) {
	segments := make([]AcousticSegment, 0, len(units))
	var all []byte
	frameCursor := startFrame
	for _, unit := range units {
		count := unitFrameCount(unit, sample, cfg)
		frames := make([]AcousticFrame, count)
		for i := 0; i < count; i++ {
			frames[i] = predictFrame(unit, sample, cfg, i)
		}
		data, err := encodeAcousticFrames(frames, sample.UTTID, unit.Index, frameCursor)
		if err != nil {
			return nil, nil, err
		}
		segments = append(segments, AcousticSegment{
			SegmentSeq: unit.Index,
			Unit:       unit,
			FrameStart: frameCursor,
			Frames:     frames,
			Bytes:      data,
			Digest:     digestBytes(data),
		})
		all = append(all, data...)
		frameCursor += count
	}
	return segments, all, nil
}
