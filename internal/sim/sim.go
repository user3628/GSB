package sim

import (
	"container/heap"
	"fmt"
	"sort"

	"upstreampool/internal/config"
	"upstreampool/internal/model"
	"upstreampool/internal/statemachine"
)

const (
	StatusDone  = "DONE"
	StatusError = "ERROR"
)

type RequestResult struct {
	TrackID            string `json:"track_id"`
	BatchID            string `json:"batch_id"`
	RequestKey         string `json:"request_key"`
	RequestID          string `json:"request_id"`
	ClientID           string `json:"client_id"`
	EndpointID         string `json:"endpoint_id"`
	Status             string `json:"status"`
	ErrorCode          string `json:"error_code,omitempty"`
	ErrorMessage       string `json:"error_message,omitempty"`
	ConnectionID       string `json:"connection_id,omitempty"`
	QueuedAtMS         int64  `json:"queued_at_ms,omitempty"`
	AcquiredAtMS       int64  `json:"acquired_at_ms,omitempty"`
	StartedMS          int64  `json:"started_ms,omitempty"`
	CompletedMS        int64  `json:"completed_ms"`
	WaitedMS           int64  `json:"waited_ms"`
	UploadDurationMS   int64  `json:"upload_duration_ms"`
	ResponseDurationMS int64  `json:"response_duration_ms"`
}

type ConnectionUse struct {
	ConnectionID string `json:"connection_id"`
	RequestKey   string `json:"request_key"`
	RequestID    string `json:"request_id"`
	AcquiredMS   int64  `json:"acquired_ms"`
	Reused       bool   `json:"reused"`
}

type Transition struct {
	ConnectionID string `json:"connection_id"`
	EndpointID   string `json:"endpoint_id"`
	Seq          int64  `json:"seq"`
	AtMS         int64  `json:"at_ms"`
	From         string `json:"from"`
	To           string `json:"to"`
	RequestKey   string `json:"request_key,omitempty"`
	Reason       string `json:"reason"`
}

type ConnectionState struct {
	ID                string             `json:"id"`
	EndpointID        string             `json:"endpoint_id"`
	Stage             statemachine.Stage `json:"stage"`
	ReuseCount        int                `json:"reuse_count"`
	TotalUses         int                `json:"total_uses"`
	CreatedMS         int64              `json:"created_ms"`
	LastUsedMS        int64              `json:"last_used_ms"`
	CurrentRequestKey string             `json:"current_request_key,omitempty"`
	CloseReason       string             `json:"close_reason,omitempty"`
}

type EndpointPool struct {
	EndpointID        string   `json:"endpoint_id"`
	MaxConnections    int      `json:"max_connections"`
	ActiveConnections int      `json:"active_connections"`
	AvailableSlots    int      `json:"available_slots"`
	IdleConnections   []string `json:"idle_connections"`
	Connecting        int      `json:"connecting"`
	Handshaking       int      `json:"handshaking"`
	Sending           int      `json:"sending"`
	Receiving         int      `json:"receiving"`
	Draining          int      `json:"draining"`
	Queued            int      `json:"queued"`
}

type QueueEntry struct {
	EndpointID string `json:"endpoint_id"`
	RequestKey string `json:"request_key"`
	RequestID  string `json:"request_id"`
	ClientID   string `json:"client_id"`
	EnqueuedMS int64  `json:"enqueued_ms"`
	DeadlineMS int64  `json:"deadline_ms"`
	Position   int    `json:"position"`
}

type Snapshot struct {
	TrackID     string            `json:"track_id"`
	Seq         int64             `json:"seq"`
	AtMS        int64             `json:"at_ms"`
	Endpoints   []EndpointPool    `json:"endpoints"`
	Connections []ConnectionState `json:"connections"`
	Queue       []QueueEntry      `json:"queue"`
}

type FinalConnection struct {
	ConnectionState
	ClosedMS    int64  `json:"closed_ms,omitempty"`
	CloseReason string `json:"close_reason,omitempty"`
}

type Plan struct {
	TrackID     string            `json:"track_id"`
	HorizonMS   int64             `json:"horizon_ms"`
	FinalSeq    int64             `json:"final_seq"`
	Requests    []RequestResult   `json:"requests"`
	Connections []FinalConnection `json:"connections"`
	Uses        []ConnectionUse   `json:"-"`
	Transitions []Transition      `json:"transitions"`
	Snapshots   []Snapshot        `json:"-"`
}

type Input struct {
	TrackID   string
	Requests  []model.Request
	Endpoints map[string]model.EndpointSpec
	Defaults  model.Defaults
	Overrides map[string]string
}

type connState struct {
	id                string
	endpointID        string
	stage             statemachine.Stage
	reuseCount        int
	totalUses         int
	createdMS         int64
	lastUsedMS        int64
	currentRequestKey string
	closeReason       string
	closedMS          int64
}

type waiter struct {
	seq        int64
	reqKey     string
	enqueuedMS int64
	deadlineMS int64
	active     bool
}

type endpointState struct {
	spec    model.EndpointSpec
	conns   map[string]*connState
	idle    []string
	waiters []*waiter
	nextSeq int
}

func (e *endpointState) activeCount() int {
	n := 0
	for _, c := range e.conns {
		if c.stage != statemachine.CLOSED {
			n++
		}
	}
	return n
}

type eventKind int

const (
	evDialDone eventKind = iota
	evHandshakeDone
	evSendDone
	evResponseDone
	evClose
	evDialFail
	evSendFail
	evRecvFail
	evArrival
	evIdleReclaim
	evWaitTimeout
)

func (k eventKind) priority() int {
	switch k {
	case evDialDone, evHandshakeDone, evSendDone, evResponseDone, evClose, evDialFail, evSendFail, evRecvFail:
		return 0
	case evArrival:
		return 1
	case evIdleReclaim:
		return 2
	default:
		return 3
	}
}

type simEvent struct {
	at      int64
	serial  int64
	kind    eventKind
	epID    string
	connID  string
	reqKey  string
	waitSeq int64
}

type eventHeap []*simEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	if h[i].kind.priority() != h[j].kind.priority() {
		return h[i].kind.priority() < h[j].kind.priority()
	}
	return h[i].serial < h[j].serial
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*simEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

type simulator struct {
	in       Input
	eps      map[string]*endpointState
	epOrder  []string
	results  map[string]*RequestResult
	reqByKey map[string]model.Request
	uses     []ConnectionUse
	trans    []Transition
	snaps    []Snapshot
	events   eventHeap
	serial   int64
	seq      int64
	waitSeq  int64
	err      error
}

func Run(in Input) (*Plan, error) {
	s := &simulator{
		in:       in,
		eps:      make(map[string]*endpointState, len(in.Endpoints)),
		results:  make(map[string]*RequestResult, len(in.Requests)),
		reqByKey: make(map[string]model.Request, len(in.Requests)),
	}
	for id, spec := range in.Endpoints {
		s.eps[id] = &endpointState{spec: spec, conns: make(map[string]*connState)}
		s.epOrder = append(s.epOrder, id)
	}
	sort.Strings(s.epOrder)
	s.recordSnapshot(0)

	requests := append([]model.Request(nil), in.Requests...)
	sort.SliceStable(requests, func(i, j int) bool {
		if requests[i].TimestampMS != requests[j].TimestampMS {
			return requests[i].TimestampMS < requests[j].TimestampMS
		}
		ki := keyOf(requests[i])
		kj := keyOf(requests[j])
		if ki != kj {
			return ki < kj
		}
		if requests[i].BatchID != requests[j].BatchID {
			return requests[i].BatchID < requests[j].BatchID
		}
		return requests[i].Ordinal < requests[j].Ordinal
	})
	for i := range requests {
		req := requests[i]
		key := keyOf(req)
		if _, exists := s.reqByKey[key]; exists {
			return nil, model.NewError(400, model.ErrSpecInvalid, "duplicate request key", map[string]any{"request_key": key})
		}
		s.reqByKey[key] = req
		s.results[key] = &RequestResult{
			TrackID:    req.TrackID,
			BatchID:    req.BatchID,
			RequestKey: key,
			RequestID:  req.RequestID,
			ClientID:   req.ClientID,
			EndpointID: req.EndpointID,
		}
		at := req.TimestampMS
		s.serial++
		heap.Push(&s.events, &simEvent{at: at, serial: s.serial, kind: evArrival, epID: req.EndpointID, reqKey: key})
	}

	heap.Init(&s.events)
	for s.events.Len() > 0 && s.err == nil {
		ev := heap.Pop(&s.events).(*simEvent)
		s.process(ev)
	}
	if s.err != nil {
		return nil, s.err
	}

	var orderedResults []RequestResult
	for _, req := range requests {
		res := s.results[keyOf(req)]
		if res.Status == "" {
			return nil, fmt.Errorf("internal simulation left request %s unfinished", res.RequestKey)
		}
		orderedResults = append(orderedResults, *res)
	}
	var horizon int64
	for i := range orderedResults {
		if orderedResults[i].CompletedMS > horizon {
			horizon = orderedResults[i].CompletedMS
		}
	}
	finalSeq := int64(0)
	for _, snap := range s.snaps {
		if snap.AtMS <= horizon {
			finalSeq = snap.Seq
		}
	}

	var conns []FinalConnection
	for _, epID := range s.epOrder {
		ep := s.eps[epID]
		ids := make([]string, 0, len(ep.conns))
		for id := range ep.conns {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			c := ep.conns[id]
			conns = append(conns, FinalConnection{
				ConnectionState: ConnectionState{
					ID: c.id, EndpointID: c.endpointID, Stage: c.stage,
					ReuseCount: c.reuseCount, TotalUses: c.totalUses,
					CreatedMS: c.createdMS, LastUsedMS: c.lastUsedMS,
					CurrentRequestKey: c.currentRequestKey, CloseReason: c.closeReason,
				},
				ClosedMS: c.closedMS, CloseReason: c.closeReason,
			})
		}
	}
	sort.SliceStable(s.uses, func(i, j int) bool {
		if s.uses[i].AcquiredMS != s.uses[j].AcquiredMS {
			return s.uses[i].AcquiredMS < s.uses[j].AcquiredMS
		}
		return s.uses[i].ConnectionID < s.uses[j].ConnectionID
	})
	return &Plan{
		TrackID: in.TrackID, HorizonMS: horizon, FinalSeq: finalSeq,
		Requests:    orderedResults,
		Connections: conns, Uses: s.uses, Transitions: s.trans, Snapshots: s.snaps,
	}, nil
}

func (s *simulator) process(ev *simEvent) {
	if s.err != nil {
		return
	}
	ep := s.eps[ev.epID]
	if ep == nil {
		s.err = fmt.Errorf("unknown endpoint in event: %s", ev.epID)
		return
	}
	switch ev.kind {
	case evArrival:
		s.onArrival(ep, ev)
	case evDialDone:
		s.onDialDone(ep, ev)
	case evHandshakeDone:
		s.onHandshakeDone(ep, ev)
	case evSendDone:
		s.onSendDone(ep, ev)
	case evResponseDone:
		s.onResponseDone(ep, ev)
	case evClose:
		s.onClose(ep, ev)
	case evDialFail:
		s.onDialFail(ep, ev)
	case evSendFail:
		s.onTransferFail(ep, ev, statemachine.SEND_BODY, model.ErrSendTimeout, "write timeout")
	case evRecvFail:
		s.onTransferFail(ep, ev, statemachine.RECV_RESPONSE, model.ErrRecvTimeout, "read timeout")
	case evIdleReclaim:
		s.onIdleReclaim(ep, ev)
	case evWaitTimeout:
		s.onWaitTimeout(ep, ev)
	}
}

func (s *simulator) onArrival(ep *endpointState, ev *simEvent) {
	req := s.reqByKey[ev.reqKey]
	res := s.results[ev.reqKey]
	if ep.spec.Health == model.HealthDown {
		s.finishError(res, ev.at, model.ErrEndpointUnhealthy, "endpoint health is DOWN")
		return
	}
	if c := ep.takeIdle(); c != nil {
		s.startRequest(ep, req, ev.at, ev.at, c)
		return
	}
	if ep.activeCount() < ep.spec.MaxConnections {
		c := s.newConn(ep, ev.at, ev.reqKey, "dial")
		s.startRequest(ep, req, ev.at, ev.at, c)
		return
	}
	if ep.spec.QueuePolicy == model.QueuePolicyQueue && len(ep.waiters) < ep.spec.QueueLimit {
		deadline, err := config.SafeAdd(ev.at, ep.spec.AcquireTimeoutMS)
		if err != nil {
			s.err = model.NewError(400, model.ErrSpecInvalid, "queue deadline overflow", map[string]any{"request_key": ev.reqKey})
			return
		}
		s.waitSeq++
		w := &waiter{seq: s.waitSeq, reqKey: ev.reqKey, enqueuedMS: ev.at, deadlineMS: deadline, active: true}
		ep.waiters = append(ep.waiters, w)
		res.QueuedAtMS = ev.at
		s.recordSnapshot(ev.at)
		s.schedule(deadline, evWaitTimeout, ep.spec.ID, "", ev.reqKey, w.seq)
		return
	}
	reason := "queue policy REJECT"
	if ep.spec.QueuePolicy == model.QueuePolicyQueue {
		reason = "bounded queue is full"
	}
	s.finishError(res, ev.at, model.ErrPoolExhausted, reason)
}

func (s *simulator) startRequest(ep *endpointState, req model.Request, at, queuedAt int64, c *connState) {
	res := s.results[keyOf(req)]
	if c != nil && queuedAt < at {
		res.WaitedMS = at - queuedAt
	}
	if c == nil {
		c = s.newConn(ep, at, keyOf(req), "dial")
		if c == nil || s.err != nil {
			return
		}
	}
	reused := c.totalUses > 0
	if reused {
		c.reuseCount++
	}
	c.totalUses++
	c.currentRequestKey = keyOf(req)
	res.ConnectionID = c.id
	res.AcquiredAtMS = at
	if res.WaitedMS == 0 && res.QueuedAtMS > 0 {
		res.WaitedMS = at - res.QueuedAtMS
	}
	s.uses = append(s.uses, ConnectionUse{
		ConnectionID: c.id, RequestKey: keyOf(req), RequestID: req.RequestID,
		AcquiredMS: at, Reused: reused,
	})
	if reused {
		if err := s.transition(ep, c, statemachine.HANDSHAKING, at, keyOf(req), "reuse"); err != nil {
			s.err = err
			return
		}
	}
	if override := s.in.Overrides[keyOf(req)]; override == model.ErrDialFailed || override == model.ErrDialTimeout {
		failAt, err := config.SafeAdd(at, ep.spec.DialTimeoutMS)
		if err != nil {
			s.err = specOverflow(keyOf(req), "dial failure timestamp overflow")
			return
		}
		s.schedule(failAt, evDialFail, ep.spec.ID, c.id, keyOf(req), 0)
		return
	}
	if !reused {
		dialDoneAt, err := config.SafeAdd(at, ep.spec.DialTimeoutMS)
		if err != nil {
			s.err = specOverflow(keyOf(req), "dial completion timestamp overflow")
			return
		}
		s.schedule(dialDoneAt, evDialDone, ep.spec.ID, c.id, keyOf(req), 0)
		return
	}
	handshakeAt, err := config.SafeAdd(at, s.in.Defaults.HandshakeMS)
	if err != nil {
		s.err = specOverflow(keyOf(req), "handshake completion overflow")
		return
	}
	s.schedule(handshakeAt, evHandshakeDone, ep.spec.ID, c.id, keyOf(req), 0)
}

func (s *simulator) onDialDone(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.currentRequestKey != ev.reqKey || c.stage != statemachine.CONNECTING {
		return
	}
	if err := s.transition(ep, c, statemachine.HANDSHAKING, ev.at, ev.reqKey, "dial complete"); err != nil {
		s.err = err
		return
	}
	handshakeAt, err := config.SafeAdd(ev.at, s.in.Defaults.HandshakeMS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "handshake completion overflow")
		return
	}
	s.schedule(handshakeAt, evHandshakeDone, ep.spec.ID, c.id, ev.reqKey, 0)
}

func (s *simulator) onHandshakeDone(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.currentRequestKey != ev.reqKey || c.stage != statemachine.HANDSHAKING {
		return
	}
	if err := s.transition(ep, c, statemachine.SEND_BODY, ev.at, ev.reqKey, "handshake complete"); err != nil {
		s.err = err
		return
	}
	res := s.results[ev.reqKey]
	res.StartedMS = ev.at
	req := s.reqByKey[ev.reqKey]
	uploadMS, err := config.DurationMS(req.RequestBytes, req.UploadRateBPS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "upload duration overflow")
		return
	}
	res.UploadDurationMS = uploadMS
	if override := s.in.Overrides[ev.reqKey]; override == model.ErrSendTimeout {
		delay := uploadMS
		if delay == 0 || delay > s.in.Defaults.WriteTimeoutMS {
			delay = s.in.Defaults.WriteTimeoutMS
		}
		failAt, err := config.SafeAdd(ev.at, delay)
		if err != nil {
			s.err = specOverflow(ev.reqKey, "send timeout timestamp overflow")
			return
		}
		s.schedule(failAt, evSendFail, ep.spec.ID, c.id, ev.reqKey, 0)
		return
	}
	doneAt, err := config.SafeAdd(ev.at, uploadMS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "send completion overflow")
		return
	}
	s.schedule(doneAt, evSendDone, ep.spec.ID, c.id, ev.reqKey, 0)
}

func (s *simulator) onSendDone(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.currentRequestKey != ev.reqKey || c.stage != statemachine.SEND_BODY {
		return
	}
	if err := s.transition(ep, c, statemachine.RECV_RESPONSE, ev.at, ev.reqKey, "request body sent"); err != nil {
		s.err = err
		return
	}
	req := s.reqByKey[ev.reqKey]
	responseBytesMS, err := config.DurationMS(req.ResponseBytes, s.in.Defaults.ResponseRateBPS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "response duration overflow")
		return
	}
	responseMS, err := config.SafeAdd(s.in.Defaults.ResponseBaseMS, responseBytesMS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "response duration overflow")
		return
	}
	s.results[ev.reqKey].ResponseDurationMS = responseMS
	if override := s.in.Overrides[ev.reqKey]; override == model.ErrRecvTimeout {
		delay := responseMS
		if delay == 0 || delay > s.in.Defaults.ReadTimeoutMS {
			delay = s.in.Defaults.ReadTimeoutMS
		}
		failAt, err := config.SafeAdd(ev.at, delay)
		if err != nil {
			s.err = specOverflow(ev.reqKey, "receive timeout timestamp overflow")
			return
		}
		s.schedule(failAt, evRecvFail, ep.spec.ID, c.id, ev.reqKey, 0)
		return
	}
	doneAt, err := config.SafeAdd(ev.at, responseMS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "response completion overflow")
		return
	}
	s.schedule(doneAt, evResponseDone, ep.spec.ID, c.id, ev.reqKey, 0)
}

func (s *simulator) onResponseDone(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.currentRequestKey != ev.reqKey || c.stage != statemachine.RECV_RESPONSE {
		return
	}
	reusable := ep.spec.Health == model.HealthUp && c.reuseCount < ep.spec.MaxReuses
	c.currentRequestKey = ""
	c.lastUsedMS = ev.at
	if reusable {
		ep.addIdle(c.id)
		if err := s.transition(ep, c, statemachine.IDLE, ev.at, ev.reqKey, "request complete; reusable"); err != nil {
			s.err = err
			return
		}
	} else {
		reason := "max_reuses_reached"
		if ep.spec.Health != model.HealthUp {
			reason = "endpoint_unhealthy"
		}
		c.closeReason = reason
		if err := s.transition(ep, c, statemachine.DRAINING, ev.at, ev.reqKey, reason); err != nil {
			s.err = err
			return
		}
		closeAt, err := config.SafeAdd(ev.at, s.in.Defaults.DrainMS)
		if err != nil {
			s.err = specOverflow(ev.reqKey, "drain completion overflow")
			return
		}
		s.schedule(closeAt, evClose, ep.spec.ID, c.id, ev.reqKey, 0)
	}
	s.finishSuccess(s.results[ev.reqKey], ev.at)
	s.serviceWaiters(ep, ev.at)
}

func (s *simulator) onClose(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.stage != statemachine.DRAINING {
		return
	}
	ep.removeIdle(c.id)
	if err := s.transition(ep, c, statemachine.CLOSED, ev.at, "", c.closeReason); err != nil {
		s.err = err
		return
	}
	c.closedMS = ev.at
	s.serviceWaiters(ep, ev.at)
}

func (s *simulator) onDialFail(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.currentRequestKey != ev.reqKey || c.stage != statemachine.CONNECTING {
		return
	}
	code := s.in.Overrides[ev.reqKey]
	if code != model.ErrDialFailed && code != model.ErrDialTimeout {
		code = model.ErrDialFailed
	}
	msg := "dial failed"
	if code == model.ErrDialTimeout {
		msg = "dial timed out"
	}
	c.closeReason = code
	if err := s.transition(ep, c, statemachine.CLOSED, ev.at, ev.reqKey, msg); err != nil {
		s.err = err
		return
	}
	c.currentRequestKey = ""
	c.closedMS = ev.at
	s.finishError(s.results[ev.reqKey], ev.at, code, msg)
	s.serviceWaiters(ep, ev.at)
}

func (s *simulator) onTransferFail(ep *endpointState, ev *simEvent, from statemachine.Stage, code, reason string) {
	c := ep.conns[ev.connID]
	if c == nil || c.currentRequestKey != ev.reqKey || c.stage != from {
		return
	}
	c.currentRequestKey = ""
	c.closeReason = code
	if err := s.transition(ep, c, statemachine.DRAINING, ev.at, ev.reqKey, reason); err != nil {
		s.err = err
		return
	}
	s.finishError(s.results[ev.reqKey], ev.at, code, reason)
	closeAt, err := config.SafeAdd(ev.at, s.in.Defaults.DrainMS)
	if err != nil {
		s.err = specOverflow(ev.reqKey, "drain completion overflow")
		return
	}
	s.schedule(closeAt, evClose, ep.spec.ID, c.id, ev.reqKey, 0)
}

func (s *simulator) onIdleReclaim(ep *endpointState, ev *simEvent) {
	c := ep.conns[ev.connID]
	if c == nil || c.stage != statemachine.IDLE {
		return
	}
	ep.removeIdle(c.id)
	c.closeReason = "idle_reclaim"
	if err := s.transition(ep, c, statemachine.DRAINING, ev.at, "", "idle reclaim"); err != nil {
		s.err = err
		return
	}
	closeAt, err := config.SafeAdd(ev.at, s.in.Defaults.DrainMS)
	if err != nil {
		s.err = fmt.Errorf("idle reclaim drain overflow")
		return
	}
	s.schedule(closeAt, evClose, ep.spec.ID, c.id, "", 0)
}

func (s *simulator) onWaitTimeout(ep *endpointState, ev *simEvent) {
	for i, w := range ep.waiters {
		if w.seq != ev.waitSeq || !w.active {
			continue
		}
		w.active = false
		ep.waiters = append(ep.waiters[:i], ep.waiters[i+1:]...)
		waited := ev.at - w.enqueuedMS
		res := s.results[w.reqKey]
		res.WaitedMS = waited
		s.finishError(res, ev.at, model.ErrPoolAcquireTimeout, "timed out waiting for a pool connection")
		s.recordSnapshot(ev.at)
		return
	}
}

func (s *simulator) serviceWaiters(ep *endpointState, now int64) {
	for len(ep.waiters) > 0 && s.err == nil {
		w := ep.waiters[0]
		if !w.active {
			ep.waiters = ep.waiters[1:]
			continue
		}
		if now > w.deadlineMS {
			ep.waiters = ep.waiters[1:]
			w.active = false
			res := s.results[w.reqKey]
			res.WaitedMS = now - w.enqueuedMS
			s.finishError(res, now, model.ErrPoolAcquireTimeout, "timed out waiting for a pool connection")
			s.recordSnapshot(now)
			continue
		}
		var c *connState
		if c = ep.takeIdle(); c == nil && ep.activeCount() < ep.spec.MaxConnections {
			c = s.newConn(ep, now, w.reqKey, "dial after queue")
			if s.err != nil {
				return
			}
		}
		if c == nil {
			break
		}
		ep.waiters = ep.waiters[1:]
		w.active = false
		req := s.reqByKey[w.reqKey]
		s.startRequest(ep, req, now, w.enqueuedMS, c)
	}
}

func (s *simulator) newConn(ep *endpointState, at int64, reqKey, reason string) *connState {
	ep.nextSeq++
	id := fmt.Sprintf("%s-c%06d", ep.spec.ID, ep.nextSeq)
	c := &connState{id: id, endpointID: ep.spec.ID, stage: "", createdMS: at}
	ep.conns[id] = c
	c.currentRequestKey = reqKey
	if err := s.transition(ep, c, statemachine.CONNECTING, at, reqKey, reason); err != nil {
		s.err = err
		return nil
	}
	return c
}

func (e *endpointState) addIdle(id string) {
	e.idle = append(e.idle, id)
	sort.SliceStable(e.idle, func(i, j int) bool {
		a, b := e.conns[e.idle[i]], e.conns[e.idle[j]]
		if a.lastUsedMS != b.lastUsedMS {
			return a.lastUsedMS < b.lastUsedMS
		}
		return a.id < b.id
	})
}

func (e *endpointState) removeIdle(id string) {
	for i, existing := range e.idle {
		if existing == id {
			e.idle = append(e.idle[:i], e.idle[i+1:]...)
			return
		}
	}
}

func (e *endpointState) takeIdle() *connState {
	for len(e.idle) > 0 {
		id := e.idle[0]
		e.idle = e.idle[1:]
		c := e.conns[id]
		if c != nil && c.stage == statemachine.IDLE {
			return c
		}
	}
	return nil
}

func (s *simulator) transition(ep *endpointState, c *connState, to statemachine.Stage, at int64, reqKey, reason string) error {
	from := c.stage
	if !statemachine.Valid(from, to) {
		return model.NewError(409, model.ErrInvalidTransition, "internal state machine attempted an invalid transition", map[string]any{
			"connection_id": c.id, "from": from, "to": to,
		})
	}
	s.seq++
	c.stage = to
	s.trans = append(s.trans, Transition{
		ConnectionID: c.id, EndpointID: c.endpointID, Seq: s.seq, AtMS: at,
		From: string(from), To: string(to), RequestKey: reqKey, Reason: reason,
	})
	s.recordSnapshotAtSeq(at, s.seq)
	if to == statemachine.IDLE {
		idleAt, err := config.SafeAdd(at, ep.spec.IdleReclaimMS)
		if err != nil {
			return specOverflow(reqKey, "idle reclaim timestamp overflow")
		}
		s.schedule(idleAt, evIdleReclaim, ep.spec.ID, c.id, "", 0)
	}
	return nil
}

func (s *simulator) schedule(at int64, kind eventKind, epID, connID, reqKey string, waitSeq int64) {
	s.serial++
	heap.Push(&s.events, &simEvent{at: at, serial: s.serial, kind: kind, epID: epID, connID: connID, reqKey: reqKey, waitSeq: waitSeq})
}

func (s *simulator) finishSuccess(res *RequestResult, at int64) {
	res.Status = StatusDone
	res.CompletedMS = at
}

func (s *simulator) finishError(res *RequestResult, at int64, code, message string) {
	res.Status = StatusError
	res.ErrorCode = code
	res.ErrorMessage = message
	res.CompletedMS = at
}

func (s *simulator) recordSnapshot(at int64) {
	s.seq++
	s.recordSnapshotAtSeq(at, s.seq)
}

func (s *simulator) recordSnapshotAtSeq(at, seq int64) {
	snap := Snapshot{TrackID: s.in.TrackID, Seq: seq, AtMS: at}
	for _, epID := range s.epOrder {
		ep := s.eps[epID]
		pool := EndpointPool{EndpointID: epID, MaxConnections: ep.spec.MaxConnections, IdleConnections: []string{}}
		ids := make([]string, 0, len(ep.conns))
		for id := range ep.conns {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			c := ep.conns[id]
			state := ConnectionState{
				ID: c.id, EndpointID: c.endpointID, Stage: c.stage, ReuseCount: c.reuseCount,
				TotalUses: c.totalUses, CreatedMS: c.createdMS, LastUsedMS: c.lastUsedMS,
				CurrentRequestKey: c.currentRequestKey, CloseReason: c.closeReason,
			}
			snap.Connections = append(snap.Connections, state)
			if c.stage == statemachine.CLOSED {
				continue
			}
			pool.ActiveConnections++
			switch c.stage {
			case statemachine.CONNECTING:
				pool.Connecting++
			case statemachine.HANDSHAKING:
				pool.Handshaking++
			case statemachine.SEND_BODY:
				pool.Sending++
			case statemachine.RECV_RESPONSE:
				pool.Receiving++
			case statemachine.DRAINING:
				pool.Draining++
			}
		}
		pool.AvailableSlots = ep.spec.MaxConnections - pool.ActiveConnections
		for _, id := range ep.idle {
			if c := ep.conns[id]; c != nil && c.stage == statemachine.IDLE {
				pool.IdleConnections = append(pool.IdleConnections, id)
			}
		}
		pool.Queued = len(ep.waiters)
		snap.Endpoints = append(snap.Endpoints, pool)
		for i, w := range ep.waiters {
			req := s.reqByKey[w.reqKey]
			snap.Queue = append(snap.Queue, QueueEntry{
				EndpointID: epID, RequestKey: w.reqKey, RequestID: req.RequestID,
				ClientID: req.ClientID, EnqueuedMS: w.enqueuedMS, DeadlineMS: w.deadlineMS, Position: i + 1,
			})
		}
	}
	s.snaps = append(s.snaps, snap)
}

func keyOf(req model.Request) string {
	if req.RequestKey != "" {
		return req.RequestKey
	}
	if req.RequestID != "" {
		return req.RequestID
	}
	return fmt.Sprintf("%s:%d:%s", req.EndpointID, req.TimestampMS, req.ClientID)
}

func specOverflow(reqKey, message string) error {
	return model.NewError(400, model.ErrSpecInvalid, message, map[string]any{"request_key": reqKey})
}

func SnapshotAt(plan *Plan, atMS int64) *Snapshot {
	if len(plan.Snapshots) == 0 {
		return &Snapshot{TrackID: plan.TrackID}
	}
	var chosen *Snapshot
	for i := range plan.Snapshots {
		if plan.Snapshots[i].AtMS <= atMS && plan.Snapshots[i].Seq >= 0 {
			chosen = &plan.Snapshots[i]
		}
	}
	if chosen == nil {
		chosen = &plan.Snapshots[0]
	}
	return chosen
}

func FinalSnapshot(plan *Plan) *Snapshot {
	for i := len(plan.Snapshots) - 1; i >= 0; i-- {
		if plan.Snapshots[i].Seq == plan.FinalSeq {
			return &plan.Snapshots[i]
		}
	}
	return SnapshotAt(plan, plan.HorizonMS)
}
