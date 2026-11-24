package cockpitaudio

func BuildSampleEditState(ds *Dataset, cfg Config) (*EditState, *Error) {
	base, err := BuildSession(ds, cfg, nil)
	if err != nil {
		return nil, err
	}
	intervals := make([]Interval, 0, len(base.Timeline.Intervals))
	for _, in := range base.Timeline.Intervals {
		intervals = append(intervals, in.Interval)
	}
	edit := NewEditState(intervals, cfg.Priorities)
	if len(edit.Intervals) == 0 {
		return nil, NewError(CodeInternal, StageTimeline, "", "no intervals available for sample edit")
	}
	original := edit.Intervals[0]
	mid := (original.StartMS + original.EndMS) / 2
	split, serr := edit.Apply(EditOp{
		Type: "split_interval", Operator: "demo-operator", Reason: "演示切分区间",
		IntervalID: original.ID, SplitAtMS: mid,
	})
	if serr != nil {
		return nil, serr
	}
	_, merr := edit.Apply(EditOp{
		Type: "merge_intervals", Operator: "demo-operator", Reason: "演示合并相邻同类区间",
		IntervalIDs: []string{split.AffectedIDs[1], split.AffectedIDs[2]},
	})
	if merr != nil {
		return nil, merr
	}
	if _, perr := edit.Apply(EditOp{
		Type: "set_priority", Operator: "demo-operator", Reason: "提高后排左优先级",
		ZoneID: ZoneRearLeft, Priority: 2.25,
	}); perr != nil {
		return nil, perr
	}
	commandID := ds.Manifest.Shards[0].Commands[1].ID
	if _, herr := edit.Apply(EditOp{
		Type: "upsert_hit", Operator: "demo-operator", Reason: "人工确认后排右词表命中",
		CommandID: commandID,
		Hit:       &CandidateHit{ZoneID: ZoneRearRight, Word: "温度调高两度", Strength: 4.15},
	}); herr != nil {
		return nil, herr
	}
	return edit, nil
}
