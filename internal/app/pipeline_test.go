package app

import (
	"os"
	"path/filepath"
	"testing"

	"csrouter/internal/domain"
)

func newSeededApp(t *testing.T) *App {
	t.Helper()
	a, err := New(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.GenerateAndImport(0); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestSmallReportDeterministicAndReferenceMatch(t *testing.T) {
	a := newSeededApp(t)
	result, err := a.RunTier("small", RunOptions{ClearStop: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Report == nil || !result.Report.Reference.Pass {
		t.Fatalf("reference comparison did not pass: %+v", result.Report)
	}
	first, err := os.ReadFile(a.ReportPath("small"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.RunTier("small", RunOptions{ClearStop: true}); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(a.ReportPath("small"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("rerun report bytes differ")
	}
	if result.Report.Totals["empty_text"] < 3 || result.Report.InvalidCount < 1 {
		t.Fatalf("edge samples not accounted: totals=%+v invalid=%d", result.Report.Totals, result.Report.InvalidCount)
	}
}

func TestStopAndResumeReportByteIdenticalToContinuous(t *testing.T) {
	continuous := newSeededApp(t)
	if _, err := continuous.RunTier("small", RunOptions{ClearStop: true}); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(continuous.ReportPath("small"))
	if err != nil {
		t.Fatal(err)
	}

	interrupted := newSeededApp(t)
	stopResult, err := interrupted.RunTier("small", RunOptions{StopAfterTickets: 41})
	if err != nil {
		t.Fatal(err)
	}
	if stopResult.Code != domain.CodeStopRequested || stopResult.CompletedTickets != 41 {
		t.Fatalf("unexpected stop result: %+v", stopResult)
	}
	resumeResult, err := interrupted.RunTier("small", RunOptions{ClearStop: true})
	if err != nil {
		t.Fatal(err)
	}
	if resumeResult.Code != domain.CodeRunCompleted {
		t.Fatalf("resume did not complete: %+v", resumeResult)
	}
	got, err := os.ReadFile(interrupted.ReportPath("small"))
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(got) {
		t.Fatal("continuous and stop/resume reports are not byte-identical")
	}
}

func TestCheckpointStageAndSequenceErrors(t *testing.T) {
	a := newSeededApp(t)
	rows, err := a.DB.ListTickets("small")
	if err != nil || len(rows) == 0 {
		t.Fatalf("list tickets: %v", err)
	}
	row := rows[0]
	policy, err := a.DB.LatestPolicy("small")
	if err != nil || policy == nil {
		t.Fatalf("policy: %v", err)
	}
	input := storePublishInput(row, domain.StageNormalize, *policy, domain.NormalizeArtifact{TicketID: row.Ticket.TicketID}, "completed", "")
	if _, biz := a.DB.PublishArtifact(input); biz != nil {
		t.Fatalf("first normalize publish: %+v", biz)
	}

	illegal := input
	illegal.Stage = domain.StagePriority
	illegal.Artifact = domain.PriorityArtifact{TicketID: row.Ticket.TicketID}
	_, biz := a.DB.PublishArtifact(illegal)
	if biz == nil || biz.Code != domain.ErrIllegalStage || biz.TicketID == "" || biz.ConfigVersion == 0 {
		t.Fatalf("expected E_ILLEGAL_STAGE with ticket/version, got %+v", biz)
	}

	duplicate := input
	_, biz = a.DB.PublishArtifact(duplicate)
	if biz == nil || biz.Code != domain.ErrStaleCheckpoint {
		t.Fatalf("expected duplicate E_STALE_CHECKPOINT, got %+v", biz)
	}

	head := int64(999)
	stale := input
	stale.TicketID = rows[1].Ticket.TicketID
	stale.ExpectedHead = &head
	_, biz = a.DB.PublishArtifact(stale)
	if biz == nil || biz.Code != domain.ErrStaleCheckpoint {
		t.Fatalf("expected stale head E_STALE_CHECKPOINT, got %+v", biz)
	}
}

func TestConfigChangeInvalidatesOnlyDownstreamPriority(t *testing.T) {
	a := newSeededApp(t)
	if _, err := a.RunTier("small", RunOptions{ClearStop: true}); err != nil {
		t.Fatal(err)
	}
	result, biz := a.ChangeConfig(ConfigChangeRequest{Tier: "small", Key: "priority_thresholds.p3", Value: 0.91})
	if biz != nil {
		t.Fatal(biz)
	}
	if result.EarliestStage != domain.StagePriority {
		t.Fatalf("earliest=%s", result.EarliestStage)
	}
	statuses, err := a.DB.StageStatuses("small")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, status := range statuses {
		counts[status.Stage] = status.Completed
	}
	if counts[domain.StageNormalize] != 400 || counts[domain.StageIntent] != 400 {
		t.Fatalf("upstream artifacts should be reused: %+v", counts)
	}
	if counts[domain.StagePriority] != 0 || counts[domain.StageVerify] != 0 {
		t.Fatalf("downstream artifacts should be invalidated: %+v", counts)
	}
}
