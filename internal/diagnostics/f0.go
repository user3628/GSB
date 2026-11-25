package diagnostics

import (
	"math"

	"voiceline/internal/dsp"
	"voiceline/internal/model"
)

type F0Report struct {
	SegmentID            string     `json:"segment_id"`
	ReferenceRangeHz     [2]float64 `json:"reference_f0_mean_range_hz"`
	ToleranceHz          float64    `json:"tolerance_hz"`
	MinInRangeRatio      float64    `json:"min_in_range_ratio"`
	FrameCount           int        `json:"frame_count"`
	InRangeFrameCount    int        `json:"in_range_frame_count"`
	InRangeRatio         float64    `json:"in_range_ratio"`
	OutOfRangeFrames     []int      `json:"out_of_range_frames"`
	EstimatedMeanF0Hz    float64    `json:"estimated_mean_f0_hz"`
	MeanRangeDeviationHz float64    `json:"mean_range_deviation_hz"`
	MaxFrameDeviationHz  float64    `json:"max_frame_deviation_hz"`
	Passed               bool       `json:"passed"`
}

func AlignFrameTable(segment model.SegmentManifest, table *dsp.FrameTable) (*F0Report, error) {
	tolerance := segment.F0ToleranceHz
	if tolerance <= 0 {
		tolerance = 12
	}
	minRatio := segment.MinInRangeRatio
	if minRatio <= 0 {
		minRatio = 0.9
	}

	low := segment.ReferenceF0MeanRange[0]
	high := segment.ReferenceF0MeanRange[1]
	report := &F0Report{
		SegmentID:        segment.ID,
		ReferenceRangeHz: segment.ReferenceF0MeanRange,
		ToleranceHz:      tolerance,
		MinInRangeRatio:  minRatio,
		FrameCount:       len(table.Frames),
		OutOfRangeFrames: make([]int, 0),
	}

	sum := 0.0
	voiced := 0
	for _, frame := range table.Frames {
		f0 := frame.F0Hz
		if frame.Voiced && f0 > 0 {
			sum += f0
			voiced++
		}
		deviation := 0.0
		inRange := f0 >= low && f0 <= high
		if inRange {
			report.InRangeFrameCount++
			continue
		}
		if f0 < low {
			deviation = low - f0
		} else if f0 > high {
			deviation = f0 - high
		}
		if deviation > report.MaxFrameDeviationHz {
			report.MaxFrameDeviationHz = deviation
		}
		report.OutOfRangeFrames = append(report.OutOfRangeFrames, frame.Index)
	}
	if voiced > 0 {
		report.EstimatedMeanF0Hz = math.Round(sum/float64(voiced)*1000) / 1000
	}
	if report.EstimatedMeanF0Hz < low {
		report.MeanRangeDeviationHz = low - report.EstimatedMeanF0Hz
	} else if report.EstimatedMeanF0Hz > high {
		report.MeanRangeDeviationHz = report.EstimatedMeanF0Hz - high
	}
	if report.FrameCount > 0 {
		report.InRangeRatio = float64(report.InRangeFrameCount) / float64(report.FrameCount)
	}

	report.Passed = report.MaxFrameDeviationHz <= tolerance &&
		report.InRangeRatio+1e-12 >= minRatio &&
		report.MeanRangeDeviationHz <= tolerance
	if !report.Passed {
		return report, &model.BusinessError{
			Code:         model.CodeToleranceExceeded,
			SegmentID:    segment.ID,
			Message:      "estimated F0 alignment exceeds configured tolerance",
			MaxDeviation: math.Max(report.MaxFrameDeviationHz, report.MeanRangeDeviationHz),
			Details: map[string]any{
				"in_range_ratio":          report.InRangeRatio,
				"min_in_range_ratio":      minRatio,
				"out_of_range_frames":     report.OutOfRangeFrames,
				"max_frame_deviation_hz":  report.MaxFrameDeviationHz,
				"mean_range_deviation_hz": report.MeanRangeDeviationHz,
				"tolerance_hz":            tolerance,
			},
		}
	}
	return report, nil
}

// VerifyConvertedSequence checks the mapped F0 sequence against the same truth
// interval when a caller wants post-conversion diagnostics.
func VerifyConvertedSequence(segment model.SegmentManifest, values []float64) *F0Report {
	table := &dsp.FrameTable{SegmentID: segment.ID}
	for i, value := range values {
		table.Frames = append(table.Frames, dsp.FrameParams{Index: i, F0Hz: value, Voiced: value > 0})
	}
	report, _ := AlignFrameTable(segment, table)
	return report
}
