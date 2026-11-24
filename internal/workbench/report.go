package workbench

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

func BuildReport(state State, chain []Snapshot) Report {
	report := Report{
		Format:         ReportFormat,
		SamplingMarker: SamplingMarker,
		SnapshotMarker: DefaultSnapshot,
		Scenario:       state.Config.Scenario,
		Config:         state.Config,
		Tasks:          []TaskReport{},
		RejectedTasks:  []TaskReport{},
		SnapshotChain:  []ChainEntry{},
	}
	results := make(map[string]TaskResult, len(state.Results))
	for _, result := range state.Results {
		results[result.TaskID] = result
	}
	for _, task := range state.SortedTasks() {
		result, exists := results[task.TaskID]
		if !exists {
			report.Tasks = append(report.Tasks, TaskReport{
				TaskID:         task.TaskID,
				SampleKind:     task.SampleKind,
				Description:    task.Description,
				DeclaredBudget: task.DeclaredBudget,
				AdmissionCode:  CodeNotRun,
				DecodeCode:     CodeNotRun,
				DecisionCode:   CodeNotRun,
				FinalState:     FinalPending,
				Records:        []RecordSummary{},
				Issues:         []Issue{},
				Trajectory:     []TraceEvent{},
			})
			continue
		}
		taskReport := toTaskReport(result)
		report.Tasks = append(report.Tasks, taskReport)
	}
	sort.Slice(report.Tasks, func(i, j int) bool {
		if report.Tasks[i].TaskID == report.Tasks[j].TaskID {
			return report.Tasks[i].DecisionCode < report.Tasks[j].DecisionCode
		}
		return report.Tasks[i].TaskID < report.Tasks[j].TaskID
	})
	for _, task := range report.Tasks {
		sort.Slice(task.Records, func(i, j int) bool {
			if task.Records[i].Index == task.Records[j].Index {
				return task.Records[i].Code < task.Records[j].Code
			}
			return task.Records[i].Index < task.Records[j].Index
		})
	}
	for _, task := range report.Tasks {
		if task.FinalState == FinalRejected || task.FinalState == FinalBudgetExceeded {
			report.RejectedTasks = append(report.RejectedTasks, task)
		}
	}
	for _, snapshot := range chain {
		report.SnapshotChain = append(report.SnapshotChain, ChainEntry{
			Version:      snapshot.Version,
			Kind:         snapshot.Kind,
			ParentDigest: snapshot.ParentDigest,
			StateSHA256:  snapshot.StateSHA256,
			ChainDigest:  snapshot.ChainDigest,
		})
	}
	sort.Slice(report.SnapshotChain, func(i, j int) bool { return report.SnapshotChain[i].Version < report.SnapshotChain[j].Version })
	report.Summary = summarizeReport(report)
	return report
}

func toTaskReport(result TaskResult) TaskReport {
	records := append([]RecordSummary(nil), result.Records...)
	issues := append([]Issue(nil), result.Issues...)
	trajectory := append([]TraceEvent(nil), result.Trajectory...)
	if records == nil {
		records = []RecordSummary{}
	}
	if issues == nil {
		issues = []Issue{}
	}
	if trajectory == nil {
		trajectory = []TraceEvent{}
	}
	return TaskReport{
		TaskID:              result.TaskID,
		SampleKind:          result.SampleKind,
		Description:         result.Description,
		DeclaredBudget:      result.DeclaredBudget,
		AdmissionCode:       result.AdmissionCode,
		Admitted:            result.Admitted,
		DecodeCode:          result.DecodeCode,
		DecisionCode:        result.DecisionCode,
		FinalState:          result.FinalState,
		RejectionReason:     result.RejectionReason,
		SampleDigest:        result.SampleDigest,
		SampleBytes:         result.SampleBytes,
		PeakMemoryBytes:     result.PeakMemoryBytes,
		Allocations:         result.Allocations,
		GlobalReservedAfter: result.GlobalReservedAfter,
		SnapshotVersion:     result.SnapshotVersion,
		Records:             records,
		Failure:             result.Failure,
		Issues:              issues,
		Trajectory:          trajectory,
	}
}

func summarizeReport(report Report) ReportSummary {
	summary := ReportSummary{
		TaskCount:       len(report.Tasks),
		TaskBudgetLimit: report.Config.TaskMemoryBudget,
		GlobalBudget:    report.Config.GlobalMemoryBudget,
		SnapshotCount:   len(report.SnapshotChain),
	}
	for _, task := range report.Tasks {
		if task.GlobalReservedAfter > summary.GlobalReservedBytes {
			summary.GlobalReservedBytes = task.GlobalReservedAfter
		}
		if task.Admitted {
			summary.GlobalPeakSumBytes += task.PeakMemoryBytes
		}
		switch task.FinalState {
		case FinalRejected, FinalBudgetExceeded:
			summary.RejectedCount++
		case FinalDecodeFinding:
			summary.FindingCount++
			summary.CompletedCount++
		case FinalOK:
			summary.CompletedCount++
		}
	}
	return summary
}

func MarshalReport(report Report) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ReportDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func FindTaskReport(report Report, taskID string) (TaskReport, bool) {
	for _, task := range report.Tasks {
		if task.TaskID == taskID {
			return task, true
		}
	}
	return TaskReport{}, false
}
