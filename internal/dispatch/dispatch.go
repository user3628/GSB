package dispatch

import (
	"fmt"

	"voiceline/internal/dsp"
	"voiceline/internal/model"
)

type Config struct {
	MaxGroups        int   `json:"max_groups"`
	FramesPerGroup   int   `json:"frames_per_group"`
	RoundBudgetNanos int64 `json:"round_budget_nanos"`
	RecycleQuota     bool  `json:"recycle_quota"`
}

func (c Config) Normalize(segment model.SegmentManifest) Config {
	if c.MaxGroups <= 0 {
		c.MaxGroups = segment.MaxGroups
	}
	if c.MaxGroups <= 0 {
		c.MaxGroups = 2
	}
	if c.FramesPerGroup <= 0 {
		c.FramesPerGroup = segment.FramesPerGroup
	}
	if c.FramesPerGroup <= 0 {
		c.FramesPerGroup = 8
	}
	if c.RoundBudgetNanos <= 0 {
		c.RoundBudgetNanos = segment.RoundBudgetNanos
	}
	if c.RoundBudgetNanos <= 0 {
		c.RoundBudgetNanos = 20_000_000
	}
	return c
}

type BlockEstimate struct {
	Index          int   `json:"index"`
	FrameStart     int   `json:"frame_start"`
	FrameEnd       int   `json:"frame_end"`
	Frames         int   `json:"frames"`
	SampleStart    int   `json:"sample_start"`
	SampleEnd      int   `json:"sample_end"`
	Samples        int   `json:"samples"`
	EstimatedNanos int64 `json:"estimated_nanos"`
}

func EstimateBlocks(segment model.SegmentManifest, table *dsp.FrameTable, blockFrames int) []BlockEstimate {
	if blockFrames <= 0 {
		blockFrames = segment.EffectiveBlockFrames()
	}
	hop := table.FrameShiftSamples
	count := dsp.BlockCount(table, blockFrames)
	lagRange := table.SampleRate/70 - table.SampleRate/400
	if lagRange <= 0 {
		lagRange = 1
	}

	blocks := make([]BlockEstimate, 0, count)
	for index := 0; index < count; index++ {
		frameStart := index * blockFrames
		frameEnd := minInt(frameStart+blockFrames, len(table.Frames))
		sampleStart := frameStart * hop
		sampleEnd := minInt(frameEnd*hop, table.ExpectedSamples)
		frameCount := frameEnd - frameStart
		sampleCount := sampleEnd - sampleStart
		if sampleCount < 0 {
			sampleCount = 0
		}

		cost := int64(25_000)
		cost += int64(18 * sampleCount)
		cost += int64(2_100 * table.MelBands * frameCount)
		cost += int64(650 * lagRange * frameCount)
		cost += int64(float64(segment.EffectiveExpectedSamples()) * 0.012 / float64(maxInt(1, count)))

		blocks = append(blocks, BlockEstimate{
			Index:          index,
			FrameStart:     frameStart,
			FrameEnd:       frameEnd,
			Frames:         frameCount,
			SampleStart:    sampleStart,
			SampleEnd:      sampleEnd,
			Samples:        sampleCount,
			EstimatedNanos: cost,
		})
	}
	return blocks
}

type GroupAssignment struct {
	GroupID           int   `json:"group_id"`
	GrantedFrameQuota int   `json:"granted_frame_quota"`
	UsedFrames        int   `json:"used_frames"`
	RecycledFramesIn  int   `json:"recycled_frames_in,omitempty"`
	RecycledFramesOut int   `json:"recycled_frames_out,omitempty"`
	BlockIndexes      []int `json:"block_indexes"`
}

type QuotaTransfer struct {
	Step         int    `json:"step"`
	FromGroupID  int    `json:"from_group_id"`
	ToGroupID    int    `json:"to_group_id"`
	Frames       int    `json:"frames"`
	AtBlockIndex int    `json:"at_block_index"`
	Rule         string `json:"rule"`
}

type UnprocessedBlock struct {
	Index                int    `json:"index"`
	Reason               string `json:"reason"`
	EstimatedNanos       int64  `json:"estimated_nanos"`
	BudgetRemainingNanos int64  `json:"budget_remaining_nanos"`
}

type Plan struct {
	SegmentID            string             `json:"segment_id"`
	Round                int                `json:"round"`
	Config               Config             `json:"config"`
	Blocks               []BlockEstimate    `json:"blocks"`
	Groups               []GroupAssignment  `json:"groups"`
	AdmissionOrder       []int              `json:"admission_order"`
	Transfers            []QuotaTransfer    `json:"transfers"`
	Unprocessed          []UnprocessedBlock `json:"unprocessed"`
	ProcessableBlocks    []int              `json:"processable_blocks"`
	MaxConcurrency       int                `json:"max_concurrency"`
	ObservedConcurrency  int                `json:"observed_concurrency,omitempty"`
	BudgetNanos          int64              `json:"budget_nanos"`
	BudgetSpentNanos     int64              `json:"budget_spent_nanos"`
	BudgetRemainingNanos int64              `json:"budget_remaining_nanos"`
}

func PlanDispatch(segment model.SegmentManifest, table *dsp.FrameTable, cfg Config) (*Plan, error) {
	cfg = cfg.Normalize(segment)
	blockFrames := segment.EffectiveBlockFrames()
	blocks := EstimateBlocks(segment, table, blockFrames)
	plan := &Plan{
		SegmentID:            segment.ID,
		Round:                1,
		Config:               cfg,
		Blocks:               blocks,
		Groups:               make([]GroupAssignment, 0),
		AdmissionOrder:       make([]int, 0, len(blocks)),
		Transfers:            make([]QuotaTransfer, 0),
		Unprocessed:          make([]UnprocessedBlock, 0),
		ProcessableBlocks:    make([]int, 0, len(blocks)),
		BudgetNanos:          cfg.RoundBudgetNanos,
		BudgetRemainingNanos: cfg.RoundBudgetNanos,
	}

	nextBlock := 0
	step := 0
	for nextBlock < len(blocks) {
		estimate := blocks[nextBlock]
		if estimate.EstimatedNanos > plan.BudgetRemainingNanos {
			for _, remaining := range blocks[nextBlock:] {
				plan.Unprocessed = append(plan.Unprocessed, UnprocessedBlock{
					Index:                remaining.Index,
					Reason:               model.CodeBudgetExhausted,
					EstimatedNanos:       remaining.EstimatedNanos,
					BudgetRemainingNanos: plan.BudgetRemainingNanos,
				})
			}
			break
		}

		targetIndex := -1
		if len(plan.Groups) > 0 {
			last := len(plan.Groups) - 1
			remainingQuota := plan.Groups[last].GrantedFrameQuota - plan.Groups[last].UsedFrames
			if remainingQuota >= estimate.Frames {
				targetIndex = last
			}
		}

		if targetIndex < 0 {
			newGroup := GroupAssignment{
				GroupID:           len(plan.Groups) + 1,
				GrantedFrameQuota: cfg.FramesPerGroup,
				BlockIndexes:      make([]int, 0),
			}

			if len(plan.Groups) > 0 {
				last := len(plan.Groups) - 1
				leftover := plan.Groups[last].GrantedFrameQuota - plan.Groups[last].UsedFrames
				if leftover > 0 {
					plan.Groups[last].RecycledFramesOut = leftover
					if cfg.RecycleQuota {
						newGroup.GrantedFrameQuota += leftover
						newGroup.RecycledFramesIn = leftover
						step++
						plan.Transfers = append(plan.Transfers, QuotaTransfer{
							Step:         step,
							FromGroupID:  plan.Groups[last].GroupID,
							ToGroupID:    newGroup.GroupID,
							Frames:       leftover,
							AtBlockIndex: estimate.Index,
							Rule:         "earliest_waiting_group_receives_remaining_quota",
						})
					}
				}
			}

			if newGroup.GrantedFrameQuota < estimate.Frames {
				for _, remaining := range blocks[nextBlock:] {
					plan.Unprocessed = append(plan.Unprocessed, UnprocessedBlock{
						Index:                remaining.Index,
						Reason:               model.CodeQuotaExceeded,
						EstimatedNanos:       remaining.EstimatedNanos,
						BudgetRemainingNanos: plan.BudgetRemainingNanos,
					})
				}
				return plan, model.NewError(model.CodeQuotaExceeded, segment.ID,
					"block %d needs %d frames but group quota is %d", estimate.Index, estimate.Frames, newGroup.GrantedFrameQuota)
			}

			plan.Groups = append(plan.Groups, newGroup)
			targetIndex = len(plan.Groups) - 1
		}

		group := &plan.Groups[targetIndex]
		group.UsedFrames += estimate.Frames
		group.BlockIndexes = append(group.BlockIndexes, estimate.Index)
		plan.AdmissionOrder = append(plan.AdmissionOrder, estimate.Index)
		plan.ProcessableBlocks = append(plan.ProcessableBlocks, estimate.Index)
		plan.BudgetSpentNanos += estimate.EstimatedNanos
		plan.BudgetRemainingNanos -= estimate.EstimatedNanos
		nextBlock++
	}

	plan.MaxConcurrency = minInt(cfg.MaxGroups, len(plan.Groups))
	if len(plan.ProcessableBlocks) == 0 && len(plan.Unprocessed) == 0 {
		return plan, fmt.Errorf("dispatch plan admitted no blocks without an explicit reason")
	}
	return plan, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
