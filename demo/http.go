package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"walgroup"
	walsample "walgroup/sample"
)

// RunHTTP replays the same workload through the local HTTP API. It never
// accesses an external network and is the transport used by the CLI demo.
func RunHTTP(ctx context.Context, baseURL string, w walsample.Workload) (Report, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	base := strings.TrimRight(baseURL, "/")
	report := Report{Seed: w.Seed, AllOK: true}
	for i, a := range w.Actions {
		res := ActionResult{Index: i, Type: a.Type, Expect: a.Expect, ObservedCode: "OK"}
		fail := func(err error, msg string) {
			if err != nil {
				res.ObservedCode = walgroup.CodeOf(err)
				if res.ObservedCode == "" {
					res.ObservedCode = "E_INTERNAL"
				}
				res.Message = err.Error()
			} else if msg != "" {
				res.Message = msg
			}
			res.OK = expectationMet(a.Expect, res.ObservedCode, a.Type, &res)
			if !res.OK {
				report.AllOK = false
			}
		}
		switch a.Type {
		case "commit_group":
			var group walgroup.GroupResult
			err := httpJSON(ctx, client, http.MethodPost, base+"/v1/commit-groups",
				map[string]any{"transactions": toInputs(a.Group)}, &group)
			if err == nil {
				res.Group = &group
			}
			fail(err, "")
			if err == nil && a.Expect == "OK_DUPLICATE" {
				allDup := group.FsyncCount == 0
				for _, tx := range group.Transactions {
					allDup = allDup && tx.Duplicate
				}
				res.OK = allDup
				if !allDup {
					res.Message = "expected all transactions to be idempotent duplicates"
					report.AllOK = false
				}
			}
		case "commit_conflict":
			inputs := toInputs(a.Group)
			if len(inputs) != 0 && len(inputs[0].Records) != 0 {
				inputs[0].Records[0].Value += "-conflict"
			}
			err := httpJSON(ctx, client, http.MethodPost, base+"/v1/commit-groups",
				map[string]any{"transactions": inputs}, nil)
			fail(err, "")
		case "checkpoint":
			var cp walgroup.Checkpoint
			err := httpJSON(ctx, client, http.MethodPost, base+"/v1/checkpoints",
				map[string]any{"sequence": a.Checkpoint.Sequence, "replay_start_lsn": a.Checkpoint.ReplayStartLSN}, &cp)
			if err == nil {
				res.Checkpoint = &cp
			}
			fail(err, "")
		case "reclaim":
			var nos []uint64
			if a.Reclaim != nil {
				nos = a.Reclaim.SegmentNos
			}
			var rr walgroup.ReclaimResult
			err := httpJSON(ctx, client, http.MethodPost, base+"/v1/reclaim",
				map[string]any{"segment_nos": nos}, &rr)
			if err == nil {
				res.Reclaim = &rr
			}
			fail(err, "")
		case "reclaim_current":
			var listing struct {
				Segments []walgroup.SegmentInfo `json:"segments"`
			}
			if err := httpJSON(ctx, client, http.MethodGet, base+"/v1/segments", nil, &listing); err != nil {
				return report, err
			}
			var open []uint64
			for _, s := range listing.Segments {
				if s.State == walgroup.SegmentOpen {
					open = append(open, s.SegmentNo)
				}
			}
			if len(open) == 0 {
				res.ObservedCode = "E_TEST_NO_OPEN_SEGMENT"
				res.OK = false
				report.AllOK = false
			} else {
				err := httpJSON(ctx, client, http.MethodPost, base+"/v1/reclaim",
					map[string]any{"segment_nos": open[:1]}, nil)
				fail(err, "")
			}
		case "read":
			q := url.Values{}
			q.Set("lsn", fmt.Sprintf("%d", a.Read.LSN))
			var page walgroup.PageResult
			err := httpJSON(ctx, client, http.MethodGet,
				fmt.Sprintf("%s/v1/pages/%d?%s", base, a.Read.PageNo, q.Encode()), nil, &page)
			if err == nil {
				res.Page = &page
			}
			fail(err, "")
		default:
			res.ObservedCode = "E_UNKNOWN_ACTION"
			res.Message = fmt.Sprintf("unknown action type %q", a.Type)
			res.OK = false
			report.AllOK = false
		}
		report.Actions = append(report.Actions, res)
	}
	var listing struct {
		Segments []walgroup.SegmentInfo `json:"segments"`
	}
	if err := httpJSON(ctx, client, http.MethodGet, base+"/v1/segments", nil, &listing); err != nil {
		return report, err
	}
	report.Segments = listing.Segments
	return report, ctx.Err()
}

func httpJSON(ctx context.Context, client *http.Client, method, endpoint string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var envelope struct {
			Error *walgroup.Error `json:"error"`
		}
		if err := json.Unmarshal(b, &envelope); err == nil && envelope.Error != nil {
			return envelope.Error
		}
		return &walgroup.Error{Code: walgroup.CodeIO, Message: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("decode HTTP response: %w", err)
	}
	return nil
}
