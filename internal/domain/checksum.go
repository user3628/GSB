package domain

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"
)

func sortedPlacements(in []Placement) []Placement {
	out := append([]Placement(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

func policyChecksum(p PolicySnapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "policy/v1\nversion=%d\ngenerated_at=%s\nvram=%d\nwrite_budget=%d\nround_time_ms=%d\nqueue_capacity=%d\ntask_time_ms=%d\nloss_scale=%d\ngrowth=%d\noverflow=%d\n",
		p.Version, p.GeneratedAt.UTC().Format(time.RFC3339Nano), p.VRAMPoolBytes, p.WriteBudgetBytes,
		p.RoundTimeBudgetMS, p.WriteQueueCapacity, p.TaskTimeMS, p.LossScaleFixed,
		p.GrowthFactorFixed, p.OverflowThresholdFixed)
	for _, placement := range sortedPlacements(p.Placements) {
		fmt.Fprintf(&b, "placement=%s|%s|%s|%d\n", placement.ShardID, placement.Precision, placement.Pool, placement.LastAccessNS)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", sum[:])
}

func PolicyChecksum(p PolicySnapshot) string {
	p.Checksum = ""
	return policyChecksum(p)
}

func DeltaChecksum(d DeltaFragment) string {
	var b strings.Builder
	fmt.Fprintf(&b, "delta/v1\nbase=%d\ngenerated_at=%s\n", d.BaseVersion, d.GeneratedAt.UTC().Format(time.RFC3339Nano))
	for _, placement := range sortedPlacements(d.Placements) {
		fmt.Fprintf(&b, "placement=%s|%s|%s|%d\n", placement.ShardID, placement.Precision, placement.Pool, placement.LastAccessNS)
	}
	if d.LossScale != nil {
		fmt.Fprintf(&b, "loss_action=%s\n", d.LossScale.Action)
		if d.LossScale.ValueFixed != nil {
			fmt.Fprintf(&b, "loss_value=%d\n", *d.LossScale.ValueFixed)
		}
		if d.LossScale.GrowthFactorFixed != nil {
			fmt.Fprintf(&b, "growth=%d\n", *d.LossScale.GrowthFactorFixed)
		}
		if d.LossScale.OverflowThresholdFixed != nil {
			fmt.Fprintf(&b, "overflow=%d\n", *d.LossScale.OverflowThresholdFixed)
		}
	}
	sum := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", sum[:])
}
