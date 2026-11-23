package sim

import (
	"encoding/json"
	"os"
	"testing"

	"upstreampool/internal/config"
	"upstreampool/internal/model"
	"upstreampool/internal/statemachine"
)

func TestReuseQueueRejectAndUnhealthy(t *testing.T) {
	cfg, reqs := loadFixture(t, "../../samples/track-full.json")
	plan, err := Run(Input{TrackID: "demo-full", Requests: reqs, Endpoints: endpointMap(cfg), Defaults: cfg.Defaults})
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]RequestResult)
	for _, r := range plan.Requests {
		byID[r.RequestID] = r
	}
	if byID["r1"].ConnectionID != "edge-a-c000001" || byID["r2"].ConnectionID != byID["r1"].ConnectionID || byID["r3"].ConnectionID != byID["r1"].ConnectionID {
		t.Fatalf("expected reuse on edge-a-c000001, got %+v %+v %+v", byID["r1"], byID["r2"], byID["r3"])
	}
	if byID["r2"].Status != StatusDone || byID["r3"].Status != StatusDone {
		t.Fatalf("reuse requests must succeed: %+v %+v", byID["r2"], byID["r3"])
	}
	if byID["j2"].ErrorCode != model.ErrPoolExhausted {
		t.Fatalf("j2 code = %q", byID["j2"].ErrorCode)
	}
	if byID["d1"].ErrorCode != model.ErrEndpointUnhealthy {
		t.Fatalf("d1 code = %q", byID["d1"].ErrorCode)
	}
	assertTransition(t, plan, "edge-a-c000001", statemachine.IDLE, statemachine.HANDSHAKING)
	assertTransition(t, plan, "edge-a-c000001", statemachine.RECV_RESPONSE, statemachine.DRAINING)
	assertTransition(t, plan, "edge-a-c000001", statemachine.DRAINING, statemachine.CLOSED)
	finalConn := findConn(plan, "edge-a-c000001")
	if finalConn.ReuseCount != 2 || finalConn.TotalUses != 3 || finalConn.Stage != statemachine.CLOSED {
		t.Fatalf("unexpected final connection: %+v", finalConn)
	}
}

func TestQueueWaitTimeout(t *testing.T) {
	cfg, _ := loadFixture(t, "../../samples/track-full.json")
	reqs := []model.Request{
		req("slow", 0, "edge-queue", 10000, 10000, 100),
		req("wait1", 10, "edge-queue", 100, 10000, 100),
		req("wait2", 20, "edge-queue", 100, 10000, 100),
	}
	plan, err := Run(Input{TrackID: "q", Requests: reqs, Endpoints: endpointMap(cfg), Defaults: cfg.Defaults})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range plan.Requests {
		if r.RequestID == "wait1" || r.RequestID == "wait2" {
			if r.ErrorCode != model.ErrPoolAcquireTimeout || r.WaitedMS != 120 {
				t.Fatalf("unexpected timeout result: %+v", r)
			}
		}
	}
}

func TestSnapshotAtIsDeterministic(t *testing.T) {
	cfg, reqs := loadFixture(t, "../../samples/track-full.json")
	plan1, err := Run(Input{TrackID: "demo-full", Requests: reqs, Endpoints: endpointMap(cfg), Defaults: cfg.Defaults})
	if err != nil {
		t.Fatal(err)
	}
	plan2, err := Run(Input{TrackID: "demo-full", Requests: reqs, Endpoints: endpointMap(cfg), Defaults: cfg.Defaults})
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int64{0, 150, 300, 401, 500, 602, 5000} {
		left, _ := json.Marshal(SnapshotAt(plan1, at))
		right, _ := json.Marshal(SnapshotAt(plan2, at))
		if string(left) != string(right) {
			t.Fatalf("snapshot at %d differs\n%s\n%s", at, left, right)
		}
	}
}

func TestDialAndSendFailureOverridesCloseConnections(t *testing.T) {
	cfg, _ := loadFixture(t, "../../samples/track-full.json")
	reqs := []model.Request{
		req("dial-timeout", 0, "edge-a", 10, 10000, 10),
		req("send-timeout", 1000, "edge-a", 10000, 10000, 10),
	}
	plan, err := Run(Input{
		TrackID: "errors", Requests: reqs, Endpoints: endpointMap(cfg), Defaults: cfg.Defaults,
		Overrides: map[string]string{"dial-timeout": model.ErrDialTimeout, "send-timeout": model.ErrSendTimeout},
	})
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]RequestResult)
	for _, r := range plan.Requests {
		byID[r.RequestID] = r
	}
	if byID["dial-timeout"].ErrorCode != model.ErrDialTimeout || byID["send-timeout"].ErrorCode != model.ErrSendTimeout {
		t.Fatalf("unexpected overrides: %+v", byID)
	}
	assertTransition(t, plan, byID["dial-timeout"].ConnectionID, statemachine.CONNECTING, statemachine.CLOSED)
	assertTransition(t, plan, byID["send-timeout"].ConnectionID, statemachine.SEND_BODY, statemachine.DRAINING)
	assertTransition(t, plan, byID["send-timeout"].ConnectionID, statemachine.DRAINING, statemachine.CLOSED)
}

func assertTransition(t *testing.T, plan *Plan, conn string, from, to statemachine.Stage) {
	t.Helper()
	for _, tr := range plan.Transitions {
		if tr.ConnectionID == conn && tr.From == string(from) && tr.To == string(to) {
			return
		}
	}
	t.Fatalf("missing transition %s: %s -> %s", conn, from, to)
}

func findConn(plan *Plan, id string) FinalConnection {
	for _, c := range plan.Connections {
		if c.ID == id {
			return c
		}
	}
	return FinalConnection{}
}

func loadFixture(t *testing.T, path string) (model.Config, []model.Request) {
	t.Helper()
	cfg, err := model.LoadConfig("../../samples/lab.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Validate(&cfg); err != nil {
		t.Fatal(err)
	}
	_ = path
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sub model.SubmitRequest
	if err := json.Unmarshal(raw, &sub); err != nil {
		t.Fatal(err)
	}
	var reqs []model.Request
	for i, in := range sub.Requests {
		if err := config.ValidateRequest(&in, endpointMap(cfg)); err != nil {
			t.Fatal(err)
		}
		reqs = append(reqs, model.Request{TrackID: sub.TrackID, BatchID: sub.BatchID, RequestKey: in.RequestID, RequestID: in.RequestID, TimestampMS: in.TimestampMS, ClientID: in.ClientID, EndpointID: in.EndpointID, RequestBytes: in.RequestBytes, UploadRateBPS: in.UploadRateBPS, ResponseBytes: in.ResponseBytes, Ordinal: i})
	}
	return cfg, reqs
}

func endpointMap(cfg model.Config) map[string]model.EndpointSpec {
	out := make(map[string]model.EndpointSpec)
	for _, ep := range cfg.Endpoints {
		out[ep.ID] = ep
	}
	return out
}

func req(id string, ts int64, endpoint string, bytes, rate, response int64) model.Request {
	return model.Request{RequestKey: id, RequestID: id, ClientID: "c-" + id, EndpointID: endpoint, TimestampMS: ts, RequestBytes: bytes, UploadRateBPS: rate, ResponseBytes: response}
}
