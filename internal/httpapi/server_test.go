package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"local/eventbus/internal/agg"
	"local/eventbus/internal/bus"
	"local/eventbus/internal/httpapi"
	"local/eventbus/internal/sample"
)

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func TestHTTPPublishPullAckRewindQueriesAndErrors(t *testing.T) {
	service, err := bus.Open(context.Background(), bus.ServiceOptions{
		DBPath:        filepath.Join(t.TempDir(), "http.sqlite"),
		LeaseDuration: time.Minute,
		Now:           sample.FixedClock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	server := httptest.NewServer(httpapi.New(service))
	defer server.Close()

	call := func(method, path string, payload any, idempotencyKey string) (int, envelope) {
		t.Helper()
		var body io.Reader
		if payload != nil {
			data, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			body = bytes.NewReader(data)
		}
		request, err := http.NewRequest(method, server.URL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			request.Header.Set("Idempotency-Key", idempotencyKey)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var decoded envelope
		if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return response.StatusCode, decoded
	}
	post := func(path string, payload any) (int, envelope) {
		return call(http.MethodPost, path, payload, "")
	}
	get := func(path string) (int, envelope) {
		return call(http.MethodGet, path, nil, "")
	}
	decodeResult := func(env envelope, target any) {
		t.Helper()
		if !env.OK {
			t.Fatalf("expected success, got %+v", env.Error)
		}
		if err := json.Unmarshal(env.Result, target); err != nil {
			t.Fatalf("decode result: %v", err)
		}
	}

	status, env := get("/healthz")
	if status != http.StatusOK || !env.OK {
		t.Fatalf("health failed: %d %+v", status, env)
	}
	status, env = post("/v1/topics", bus.CreateTopicRequest{
		RequestID: "http-topic", Topic: "http-topic", Partitions: 1,
		Groups: []bus.GroupSpec{{Name: "g", Algorithm: string(agg.Kahan)}},
	})
	if status != http.StatusCreated {
		t.Fatalf("create topic status=%d env=%+v", status, env)
	}

	specs := sample.Generate(777, 10, 3)
	var firstPublish bus.PublishResult
	for i, spec := range specs {
		status, env = call(http.MethodPost, "/v1/events/publish",
			sample.ToPublishRequest(fmt.Sprintf("http-pub-%d", i), "http-topic", spec),
			fmt.Sprintf("idem-pub-%d", i))
		if status != http.StatusCreated || !env.OK {
			t.Fatalf("publish %d failed: status=%d %+v", i, status, env)
		}
		if i == 0 {
			decodeResult(env, &firstPublish)
		}
	}

	// Reusing the idempotency key and body returns the persisted result.
	status, env = call(http.MethodPost, "/v1/events/publish",
		sample.ToPublishRequest("ignored", "http-topic", specs[0]), "idem-pub-0")
	if status != http.StatusCreated {
		t.Fatalf("duplicate publish failed: %d %+v", status, env)
	}
	var duplicatePublish bus.PublishResult
	decodeResult(env, &duplicatePublish)
	if !duplicatePublish.Idempotent || duplicatePublish.Seq != firstPublish.Seq {
		t.Fatalf("idempotent HTTP publish mismatch: %+v vs %+v", duplicatePublish, firstPublish)
	}

	status, env = post("/v1/pull", bus.PullRequest{
		RequestID: "http-pull", Topic: "http-topic", Group: "g", Partition: 0,
		ConsumerID: "http-consumer", MaxEvents: 4,
	})
	if status != http.StatusOK {
		t.Fatalf("pull failed: %d %+v", status, env)
	}
	var pulled bus.PullResult
	decodeResult(env, &pulled)
	if len(pulled.Events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(pulled.Events))
	}
	seqs := make([]int64, 0, len(pulled.Events))
	for _, event := range pulled.Events {
		seqs = append(seqs, event.Seq)
	}
	status, env = post("/v1/ack", bus.AckRequest{
		RequestID: "http-ack", Topic: "http-topic", Group: "g", Partition: 0,
		ConsumerID: "http-consumer", LeaseToken: pulled.LeaseToken, Seqs: seqs,
		AckBatchID: "http-batch",
	})
	if status != http.StatusOK {
		t.Fatalf("ack failed: %d %+v", status, env)
	}

	status, env = get("/v1/offsets?topic=http-topic&group=g&partition=0")
	if status != http.StatusOK {
		t.Fatalf("offset query failed: %d %+v", status, env)
	}
	var offset bus.OffsetState
	decodeResult(env, &offset)
	if offset.AckedOffset != 4 {
		t.Fatalf("unexpected offset: %+v", offset)
	}
	status, env = get("/v1/events/state?topic=http-topic&group=g&partition=0&seq=4")
	if status != http.StatusOK {
		t.Fatalf("event state query failed: %d %+v", status, env)
	}
	var eventState bus.EventStateView
	decodeResult(env, &eventState)
	if eventState.State != bus.StateAcked {
		t.Fatalf("unexpected state: %+v", eventState)
	}
	status, env = get("/v1/aggregate?topic=http-topic&group=g&partition=0")
	if status != http.StatusOK {
		t.Fatalf("aggregate query failed: %d %+v", status, env)
	}
	var aggregate bus.AggregateResult
	decodeResult(env, &aggregate)
	if aggregate.EventCount != 4 || aggregate.Offset != 4 || aggregate.Algorithm != agg.Kahan {
		t.Fatalf("unexpected aggregate: %+v", aggregate)
	}
	status, env = get("/v1/errors?topic=http-topic&group=g&partition=0")
	if status != http.StatusOK {
		t.Fatalf("error ledger query failed: %d %+v", status, env)
	}
	var ledger bus.ErrorLedger
	decodeResult(env, &ledger)
	if len(ledger.Batches) != 1 {
		t.Fatalf("unexpected error ledger: %+v", ledger)
	}
	status, env = get("/v1/recovery")
	if status != http.StatusOK {
		t.Fatalf("recovery query failed: %d %+v", status, env)
	}

	status, env = post("/v1/rewind", bus.RewindRequest{
		RequestID: "http-rewind", Topic: "http-topic", Group: "g", Partition: 0, TargetSeq: 2,
	})
	if status != http.StatusOK {
		t.Fatalf("rewind failed: %d %+v", status, env)
	}
	var rewound bus.RewindResult
	decodeResult(env, &rewound)
	if rewound.NewOffset != 2 || rewound.ReplayUntil != 4 {
		t.Fatalf("unexpected rewind: %+v", rewound)
	}

	status, env = post("/v1/rewind", bus.RewindRequest{
		RequestID: "http-bad-rewind", Topic: "http-topic", Group: "g", Partition: 0, TargetSeq: 30,
	})
	if status != http.StatusConflict || env.OK || env.Error.Code != bus.CodeOffsetInvalid {
		t.Fatalf("expected JSON %s, got status=%d env=%+v", bus.CodeOffsetInvalid, status, env)
	}
	status, env = post("/v1/events/publish", bus.PublishRequest{
		RequestID: "http-conflict-event", EventID: specs[0].EventID, Topic: "http-topic",
		PartitionKey: specs[0].PartitionKey, EventTimeMS: specs[0].EventTimeMS,
		BatchID: specs[0].BatchID, AggregateKey: specs[0].AggregateKey,
		Metric: specs[0].Metric + 1, Body: []byte(`{"different":true}`),
	})
	if status != http.StatusConflict || env.OK || env.Error.Code != bus.CodeConflictingEvent {
		t.Fatalf("expected JSON %s, got status=%d env=%+v", bus.CodeConflictingEvent, status, env)
	}

	status, env = post("/v1/topics", bus.CreateTopicRequest{
		RequestID: "http-dl-topic", Topic: "http-dl", Partitions: 1,
		Groups: []bus.GroupSpec{{Name: "g", Algorithm: string(agg.Naive)}},
	})
	if status != http.StatusCreated {
		t.Fatalf("create dead-letter topic failed: %d %+v", status, env)
	}
	dlSpec := sample.Generate(778, 1, 1)[0]
	dlSpec.EventID = "http-dl-event"
	status, env = post("/v1/events/publish", sample.ToPublishRequest("http-dl-publish", "http-dl", dlSpec))
	if status != http.StatusCreated {
		t.Fatalf("publish dead-letter event failed: %d %+v", status, env)
	}
	status, env = post("/v1/pull", bus.PullRequest{
		RequestID: "http-dl-pull", Topic: "http-dl", Group: "g", Partition: 0,
		ConsumerID: "dl-consumer", MaxEvents: 1,
	})
	if status != http.StatusOK {
		t.Fatalf("pull dead-letter event failed: %d %+v", status, env)
	}
	var dlPull bus.PullResult
	decodeResult(env, &dlPull)
	status, env = post("/v1/dead-letter", bus.DeadLetterRequest{
		RequestID: "http-dl", Topic: "http-dl", Group: "g", Partition: 0,
		ConsumerID: "dl-consumer", LeaseToken: dlPull.LeaseToken, Seq: 1, Reason: "http-test",
	})
	if status != http.StatusOK {
		t.Fatalf("dead-letter request failed: %d %+v", status, env)
	}
	var deadLettered bus.DeadLetterResult
	decodeResult(env, &deadLettered)
	if deadLettered.State != bus.StateDeadLettered || deadLettered.Offset != 1 {
		t.Fatalf("unexpected HTTP dead-letter result: %+v", deadLettered)
	}
	status, env = get("/does-not-exist")
	if status != http.StatusNotFound || env.OK || env.Error.Code != bus.CodeNotFound {
		t.Fatalf("unexpected unknown-route response: %d %+v", status, env)
	}
}
