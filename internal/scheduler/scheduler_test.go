package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/normalizer"
)

func jsonlSource(path string, n int) Source {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"event_id":"%d","event_type":"FILE_ACCESS","time":"2024-01-02T03:04:0%dZ","user":"u","target":"t","session_id":"S","pid":1}`+"\n", i, i%10)
	}
	return Source{Path: path, Format: codes.SourceJSONL, Data: []byte(b.String())}
}

func TestConcurrencyAndWaitQueue(t *testing.T) {
	var srcs []Source
	for i := 0; i < 6; i++ {
		srcs = append(srcs, jsonlSource(fmt.Sprintf("dir/s%d.jsonl", i), 2))
	}
	out := Run(srcs, Config{Concurrency: 2, EventBudget: 0})
	if out.MaxParallel > 2 {
		t.Fatalf("concurrency bound violated: peak %d", out.MaxParallel)
	}
	if len(out.Results) != len(srcs) {
		t.Fatalf("every source must produce an isolated result: %d vs %d", len(out.Results), len(srcs))
	}
	// With 6 sources and 2 slots, at least 4 had to wait at some point.
	if len(out.WaitQueue) < 4 {
		t.Fatalf("expected waiting queue entries, got %d", len(out.WaitQueue))
	}
	// Session isolation: each result keeps its own path and event slice.
	paths := map[string]bool{}
	for _, r := range out.Results {
		if paths[r.SourcePath] {
			t.Fatal("duplicate/merged session result")
		}
		paths[r.SourcePath] = true
		if len(r.Events) != 2 {
			t.Fatalf("isolated session event count wrong: %d", len(r.Events))
		}
	}
}

func TestEventBudgetCapsAndAggregates(t *testing.T) {
	srcs := []Source{jsonlSource("big.jsonl", 10)}
	out := Run(srcs, Config{Concurrency: 1, EventBudget: 3})
	r := out.Results[0]
	if len(r.Events) != 3 {
		t.Fatalf("budget must retain first 3 events, got %d", len(r.Events))
	}
	found := false
	for _, a := range out.Aggregates {
		if a.SourcePath == "big.jsonl" && a.Code == codes.EEventBudget && a.Count == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("E_EVENT_BUDGET not aggregated: %+v", out.Aggregates)
	}
}

func TestMalformedSourceDoesNotStopOthers(t *testing.T) {
	good := jsonlSource("good.jsonl", 2)
	bad := Source{Path: "bad.jsonl", Format: codes.SourceJSONL,
		Data: []byte("{not valid json}\n")}
	out := Run([]Source{bad, good}, Config{Concurrency: 2})
	var goodRes *normalizer.Result
	for _, r := range out.Results {
		if r.SourcePath == "good.jsonl" {
			goodRes = r
		}
		if r.SourcePath == "bad.jsonl" && (len(r.Errors) == 0 || r.Errors[0].Code != codes.EJSONFieldType) {
			t.Fatalf("bad source not tagged: %+v", r.Errors)
		}
	}
	if goodRes == nil || len(goodRes.Events) != 2 {
		t.Fatal("malformed source interrupted a healthy source")
	}
}

func TestAggregatesDeterministic(t *testing.T) {
	srcs := []Source{jsonlSource("a.jsonl", 1), jsonlSource("b.jsonl", 1)}
	var wg sync.WaitGroup
	var first, second []AggCount
	for iter := 0; iter < 2; iter++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := Run(srcs, Config{Concurrency: 2})
			if i == 0 {
				first = out.Aggregates
			} else {
				second = out.Aggregates
			}
		}(iter)
	}
	wg.Wait()
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatalf("non-deterministic aggregates:\n%v\n%v", first, second)
	}
}
