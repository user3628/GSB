package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"h2diag/internal/model"
)

type builder struct {
	sequence int64
	frames   []model.Frame
}

func (b *builder) add(timestamp int64, connectionID string, streamID int64, frameType model.FrameType, flags []string, payload, increment int64) {
	b.sequence++
	if flags == nil {
		flags = []string{}
	}
	b.frames = append(b.frames, model.Frame{
		Sequence:        b.sequence,
		Timestamp:       timestamp,
		ConnectionID:    connectionID,
		StreamID:        streamID,
		Type:            frameType,
		Flags:           flags,
		PayloadBytes:    payload,
		WindowIncrement: increment,
	})
}

func main() {
	output := flag.String("out", "samples", "sample output directory")
	flag.Parse()
	if err := os.MkdirAll(*output, 0o755); err != nil {
		panic(err)
	}
	samplingSample := baseline("sampling-v1", "bucketed")
	samplingSample.Config.SchedulingThreshold = 8
	samplingSample.Config.SamplingFallbackBelow = 4
	samples := map[string]*model.Sample{
		"baseline.json":           baseline("baseline-v1", "exact"),
		"sampling.json":           samplingSample,
		"blocked.json":            blocked(),
		"window-overflow.json":    windowOverflow(),
		"invalid-transition.json": invalidTransition(),
		"invalid-frame.json":      invalidFrame(),
		"conflict-original.json":  conflict("conflict-v1", 10),
		"conflict-mutated.json":   conflict("conflict-v1", 11),
		"reset.json":              resetSample(),
	}
	for name, sample := range samples {
		data, err := json.MarshalIndent(sample, "", "  ")
		if err != nil {
			panic(err)
		}
		data = append(data, '\n')
		path := filepath.Join(*output, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			panic(err)
		}
		fmt.Printf("wrote %s (%d frames, %d streams)\n", path, len(sample.Frames), len(sample.Streams))
	}
}

func baseline(sampleID, approximation string) *model.Sample {
	cfg := model.DefaultSampleConfig()
	cfg.RoundBytes = 16 * 1024
	cfg.SchedulingThreshold = 32
	cfg.Approximation = approximation
	cfg.SamplingFallbackBelow = 8
	cfg.MaxSchedulingRounds = 100_000
	sample := &model.Sample{
		SampleID: sampleID,
		Config:   cfg,
		Connections: []model.ConnectionSpec{
			{
				ID:                      "conn-a",
				InitialConnectionWindow: 262_144,
				MaxConnectionWindow:     model.MaxHTTP2StreamID,
				InitialStreamWindow:     65_535,
				MaxStreamWindow:         model.MaxHTTP2StreamID,
				PriorityLevels:          2,
			},
			{
				ID:                      "conn-b",
				InitialConnectionWindow: 262_144,
				MaxConnectionWindow:     model.MaxHTTP2StreamID,
				InitialStreamWindow:     65_535,
				MaxStreamWindow:         model.MaxHTTP2StreamID,
				PriorityLevels:          2,
			},
		},
	}
	highSizes := []int64{32, 128, 512, 2048, 8192, 32768, 63, 257, 1023, 4097, 16384, 65535}
	lowSizes := []int64{16, 96, 384, 1536, 6144, 24576, 47, 193, 769, 3073, 12288, 49152}
	highWeights := []int64{1, 2, 4, 8, 16, 32, 3, 5, 7, 11, 13, 17}
	lowWeights := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}
	for _, connectionID := range []string{"conn-a", "conn-b"} {
		for index := 0; index < 12; index++ {
			streamID := int64(index + 1)
			sample.Streams = append(sample.Streams, model.StreamSpec{
				ConnectionID: connectionID,
				StreamID:     streamID,
				Weight:       highWeights[index],
				Priority:     0,
			})
		}
		for index := 0; index < 12; index++ {
			streamID := int64(index + 101)
			sample.Streams = append(sample.Streams, model.StreamSpec{
				ConnectionID: connectionID,
				StreamID:     streamID,
				Weight:       lowWeights[index],
				Priority:     1,
			})
		}
		sample.Streams = append(sample.Streams, model.StreamSpec{
			ConnectionID: connectionID,
			StreamID:     250,
			Weight:       5,
			Priority:     0,
		})
	}
	var b builder
	for _, connectionID := range []string{"conn-a", "conn-b"} {
		b.add(0, connectionID, 0, model.FrameSettings, nil, 0, 0)
		for index := 0; index < 12; index++ {
			streamID := int64(index + 1)
			flags := []string{"END_HEADERS"}
			if index%2 == 0 {
				flags = append(flags, "END_STREAM")
			}
			b.add(100, connectionID, streamID, model.FrameHeaders, flags, 0, 0)
		}
		for index := 0; index < 12; index++ {
			b.add(100, connectionID, int64(index+101), model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
		}
		b.add(100, connectionID, 250, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
		for index := 0; index < 12; index++ {
			b.add(1000, connectionID, int64(index+1), model.FrameData, nil, highSizes[index], 0)
			b.add(1000, connectionID, int64(index+101), model.FrameData, nil, lowSizes[index], 0)
		}
		b.add(2000, connectionID, 0, model.FrameWindowUpdate, nil, 0, 4_194_304)
		for index := 0; index < 12; index++ {
			b.add(2000, connectionID, int64(index+1), model.FrameWindowUpdate, nil, 0, 65_535)
			b.add(2000, connectionID, int64(index+101), model.FrameWindowUpdate, nil, 0, 65_535)
		}
		b.add(2000, connectionID, 250, model.FrameResetStream, nil, 0, 0)
		for index := 0; index < 12; index++ {
			b.add(3000, connectionID, int64(index+1), model.FrameData, []string{"END_STREAM"}, highSizes[index], 0)
			b.add(3000, connectionID, int64(index+101), model.FrameData, []string{"END_STREAM"}, lowSizes[index], 0)
		}
	}
	sort.SliceStable(b.frames, func(i, j int) bool {
		return b.frames[i].Timestamp < b.frames[j].Timestamp
	})
	for i := range b.frames {
		b.frames[i].Sequence = int64(i + 1)
	}
	sample.Frames = b.frames
	return sample
}

func blocked() *model.Sample {
	cfg := model.DefaultSampleConfig()
	cfg.RoundBytes = 1024
	var b builder
	b.add(0, "loopback", 0, model.FrameSettings, nil, 0, 0)
	b.add(10, "loopback", 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
	b.add(20, "loopback", 1, model.FrameData, nil, 4096, 0)
	return &model.Sample{
		SampleID: "blocked-v1",
		Config:   cfg,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 1024,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     65_535,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  b.frames,
	}
}

func windowOverflow() *model.Sample {
	cfg := model.DefaultSampleConfig()
	var b builder
	b.add(0, "loopback", 0, model.FrameSettings, nil, 0, 0)
	b.add(0, "loopback", 0, model.FrameWindowUpdate, nil, 0, 2048)
	return &model.Sample{
		SampleID: "window-overflow-v1",
		Config:   cfg,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 1024,
			MaxConnectionWindow:     2048,
			InitialStreamWindow:     1024,
			MaxStreamWindow:         2048,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  b.frames,
	}
}

func invalidTransition() *model.Sample {
	cfg := model.DefaultSampleConfig()
	var b builder
	b.add(0, "loopback", 0, model.FrameSettings, nil, 0, 0)
	b.add(10, "loopback", 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
	b.add(20, "loopback", 1, model.FrameResetStream, nil, 0, 0)
	b.add(30, "loopback", 1, model.FrameData, []string{"END_STREAM"}, 10, 0)
	return &model.Sample{
		SampleID: "invalid-transition-v1",
		Config:   cfg,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 65_535,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     65_535,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  b.frames,
	}
}

func invalidFrame() *model.Sample {
	cfg := model.DefaultSampleConfig()
	var b builder
	b.add(10, "loopback", 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
	b.add(5, "loopback", 1, model.FrameData, nil, 10, 0)
	return &model.Sample{
		SampleID: "invalid-frame-v1",
		Config:   cfg,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 65_535,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     65_535,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  b.frames,
	}
}

func conflict(sampleID string, payload int64) *model.Sample {
	cfg := model.DefaultSampleConfig()
	var b builder
	b.add(0, "loopback", 0, model.FrameSettings, nil, 0, 0)
	b.add(10, "loopback", 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
	b.add(20, "loopback", 1, model.FrameData, []string{"END_STREAM"}, payload, 0)
	return &model.Sample{
		SampleID: sampleID,
		Config:   cfg,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 65_535,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     65_535,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  b.frames,
	}
}

func resetSample() *model.Sample {
	cfg := model.DefaultSampleConfig()
	var b builder
	b.add(0, "loopback", 0, model.FrameSettings, nil, 0, 0)
	b.add(10, "loopback", 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0)
	b.add(20, "loopback", 1, model.FrameData, nil, 512, 0)
	b.add(20, "loopback", 1, model.FrameResetStream, nil, 0, 0)
	return &model.Sample{
		SampleID: "reset-v1",
		Config:   cfg,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 65_535,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     65_535,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  b.frames,
	}
}
