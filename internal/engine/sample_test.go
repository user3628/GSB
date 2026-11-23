package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"local/objectstore/internal/model"
)

type sampleScenario struct {
	ScenarioVersion int            `json:"scenario_version"`
	Objects         []sampleObject `json:"objects"`
}

type sampleObject struct {
	ObjectKey  string          `json:"object_key"`
	LowerBound int64           `json:"lower_bound"`
	TTLMS      int64           `json:"ttl_ms"`
	Sessions   []sampleSession `json:"sessions"`
}

type sampleSession struct {
	Parts                []samplePart `json:"parts"`
	SubmitOrder          []int64      `json:"submit_order"`
	DuplicatePartNumber  *int64       `json:"duplicate_part_number"`
	Cancel               bool         `json:"cancel"`
	Merge                bool         `json:"merge"`
	ExpectedTotalSize    *int64       `json:"expected_total_size"`
	ExpectedPartCount    *int64       `json:"expected_part_count"`
	ExpectedObjectSHA256 string       `json:"expected_object_sha256"`
}

type samplePart struct {
	PartNumber    int64  `json:"part_number"`
	ContentBase64 string `json:"content_base64"`
	SHA256        string `json:"sha256"`
	ArrivedAtMS   int64  `json:"arrived_at_ms"`
}

func TestFixedSampleScenario(t *testing.T) {
	path := filepath.Join("..", "..", "samples", "scenario.json")
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	var scenario sampleScenario
	if err := json.Unmarshal(encoded, &scenario); err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	if scenario.ScenarioVersion != 1 || len(scenario.Objects) < 3 {
		t.Fatalf("unexpected sample scenario: %+v", scenario)
	}

	harness := newHarness(t, Options{VerifyBudget: 5 * time.Second, VerifyBudgetSet: true})
	expectedGenerations := make(map[string]int64)
	expectedObjects := make(map[string]string)
	for objectIndex, sampleObject := range scenario.Objects {
		for sessionIndex, sampleSession := range sampleObject.Sessions {
			created, createErr := harness.engine.CreateSession(context.Background(), CreateSessionInput{
				RequestID: fmt.Sprintf("sample-%d-%d-create", objectIndex, sessionIndex),
				ObjectKey: sampleObject.ObjectKey, LowerBound: sampleObject.LowerBound, TTLMS: sampleObject.TTLMS,
			})
			if createErr != nil {
				t.Fatalf("create sample session %s/%d: %v", sampleObject.ObjectKey, sessionIndex, createErr)
			}
			if sessionIndex > 0 && created.Session.Generation != expectedGenerations[sampleObject.ObjectKey]+1 {
				t.Fatalf("generation for %s = %d", sampleObject.ObjectKey, created.Session.Generation)
			}
			expectedGenerations[sampleObject.ObjectKey] = created.Session.Generation

			partsByNumber := make(map[int64]samplePart)
			for _, part := range sampleSession.Parts {
				partsByNumber[part.PartNumber] = part
			}
			for _, partNumber := range sampleSession.SubmitOrder {
				part, ok := partsByNumber[partNumber]
				if !ok {
					t.Fatalf("sample submit order references unknown part %d", partNumber)
				}
				content, decodeErr := base64.StdEncoding.DecodeString(part.ContentBase64)
				if decodeErr != nil {
					t.Fatalf("decode sample part: %v", decodeErr)
				}
				result, submitErr := harness.engine.SubmitPart(context.Background(), SubmitPartInput{
					RequestID: fmt.Sprintf("sample-%d-%d-part-%d", objectIndex, sessionIndex, partNumber),
					SessionID: created.Session.SessionID, PartNumber: partNumber, Size: int64(len(content)),
					SHA256: part.SHA256, ArrivedAtMS: int64p(part.ArrivedAtMS), Content: content,
				})
				if submitErr != nil {
					t.Fatalf("submit sample part %d: %v", partNumber, submitErr)
				}
				if result.Duplicate {
					t.Fatalf("sample first submission %d was a duplicate", partNumber)
				}
			}
			if sampleSession.DuplicatePartNumber != nil {
				part := partsByNumber[*sampleSession.DuplicatePartNumber]
				content, _ := base64.StdEncoding.DecodeString(part.ContentBase64)
				result, submitErr := harness.engine.SubmitPart(context.Background(), SubmitPartInput{
					RequestID: fmt.Sprintf("sample-%d-%d-duplicate-%d", objectIndex, sessionIndex, *sampleSession.DuplicatePartNumber),
					SessionID: created.Session.SessionID, PartNumber: *sampleSession.DuplicatePartNumber,
					Size: int64(len(content)), SHA256: part.SHA256,
					ArrivedAtMS: int64p(part.ArrivedAtMS + 1), Content: content,
				})
				if submitErr != nil || !result.Duplicate || result.Part.DuplicateCount != 1 {
					t.Fatalf("sample duplicate result = %+v err=%v", result, submitErr)
				}
			}
			if sampleSession.Cancel {
				if _, cancelErr := harness.engine.CancelSession(context.Background(), CancelSessionInput{
					RequestID: fmt.Sprintf("sample-%d-%d-cancel", objectIndex, sessionIndex),
					SessionID: created.Session.SessionID, Reason: "sample_cancelled",
				}); cancelErr != nil {
					t.Fatalf("cancel sample session: %v", cancelErr)
				}
			}
			if sampleSession.Merge {
				merged, mergeErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
					RequestID: fmt.Sprintf("sample-%d-%d-merge", objectIndex, sessionIndex),
					SessionID: created.Session.SessionID, ExpectedTotalSize: sampleSession.ExpectedTotalSize,
					ExpectedPartCount: sampleSession.ExpectedPartCount,
				})
				if mergeErr != nil {
					t.Fatalf("merge sample session: %v", mergeErr)
				}
				if merged.Object.SHA256 != sampleSession.ExpectedObjectSHA256 {
					t.Fatalf("sample object SHA = %s, want %s", merged.Object.SHA256, sampleSession.ExpectedObjectSHA256)
				}
				expectedObjects[sampleObject.ObjectKey] = sampleSession.ExpectedObjectSHA256
			}
		}
	}

	report, recoverErr := harness.engine.Recover(context.Background())
	if recoverErr != nil {
		t.Fatalf("recover sample: %v", recoverErr)
	}
	if !report.Checks.Passed {
		t.Fatalf("sample recovery checks: %+v", report.Checks)
	}
	if len(report.Objects) != len(expectedObjects) {
		t.Fatalf("sample objects = %+v", report.Objects)
	}
	for _, object := range report.Objects {
		if expectedObjects[object.ObjectKey] != object.SHA256 {
			t.Fatalf("recovered object mismatch: %+v", object)
		}
	}
	voidParts := 0
	for _, part := range report.Parts {
		if part.SessionID != "" && part.Status != model.PartActive && part.Status != model.PartVoid {
			t.Fatalf("unexpected sample part status: %+v", part)
		}
		if part.Status == model.PartVoid {
			voidParts++
		}
	}
	if voidParts != 2 {
		t.Fatalf("void parts = %d, want 2 (one cancelled object and one cancelled generation)", voidParts)
	}
}
