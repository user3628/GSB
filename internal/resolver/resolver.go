package resolver

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
	"example.com/gsb/dnscache/internal/scheduler"
	"example.com/gsb/dnscache/internal/store"
)

type Config struct {
	Upstreams       []string
	Network         string
	UpstreamTimeout time.Duration
	MaxCNAMEHops    int
}

type Resolver struct {
	store   *store.Store
	sched   *scheduler.Scheduler
	cfg     Config
	metrics *Metrics
	logger  *log.Logger
}

func New(st *store.Store, sched *scheduler.Scheduler, cfg Config, metrics *Metrics, logger *log.Logger) *Resolver {
	if len(cfg.Upstreams) == 0 {
		cfg.Upstreams = []string{"127.0.0.1:53"}
	}
	if cfg.Network == "" {
		cfg.Network = "udp"
	}
	if cfg.UpstreamTimeout <= 0 {
		cfg.UpstreamTimeout = time.Second
	}
	if cfg.MaxCNAMEHops <= 0 {
		cfg.MaxCNAMEHops = 8
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Resolver{store: st, sched: sched, cfg: cfg, metrics: metrics, logger: logger}
}

func (r *Resolver) Metrics() *Metrics               { return r.metrics }
func (r *Resolver) Store() *store.Store             { return r.store }
func (r *Resolver) Scheduler() *scheduler.Scheduler { return r.sched }

func (r *Resolver) Resolve(ctx context.Context, query *dnsmsg.Message) (*dnsmsg.Message, error) {
	if query.Header.Flags&dnsmsg.FlagQR != 0 {
		return nil, errs.New(errs.EMessageMalformed, "received a DNS response on the query listener")
	}
	if len(query.Questions) != 1 {
		return nil, errs.New(errs.EMessageMalformed, "exactly one question is required")
	}
	q := query.Questions[0]
	if !dnsmsg.SupportedType(q.Type) {
		return nil, errs.New(errs.EUnsupportedType, "query type %s is not supported", q.Type)
	}
	q.Name = dnsmsg.CanonicalName(q.Name)

	cached, err := r.store.Lookup(ctx, q.Name, q.Type)
	if err != nil {
		return nil, fmt.Errorf("cache lookup: %w", err)
	}
	if cached.Hit {
		r.metrics.RecordCache(true)
		return responseFromEntry(query, cached.Entry), nil
	}
	r.metrics.RecordCache(false)

	server := r.cfg.Upstreams[0]
	req := scheduler.Request{Key: fmt.Sprintf("%s|%d", q.Name, q.Type), Server: server, Priority: 100}
	value, err := r.sched.Do(ctx, req, func(taskCtx context.Context) (any, error) {
		return r.resolveAndCache(taskCtx, q)
	})
	if err != nil {
		return nil, err
	}
	resp, ok := value.(*dnsmsg.Message)
	if !ok || resp == nil {
		return nil, errs.New(errs.EInternal, "scheduler returned an invalid resolution value")
	}
	return sanitizeResponse(query, resp), nil
}

func (r *Resolver) resolveAndCache(ctx context.Context, q dnsmsg.Question) (*dnsmsg.Message, error) {
	expectedZone, expectedVersion, err := r.store.ZoneForName(ctx, q.Name)
	if err != nil {
		return nil, fmt.Errorf("read zone version before upstream query: %w", err)
	}
	expectedEpoch, err := r.store.ZoneEpoch(ctx)
	if err != nil {
		return nil, fmt.Errorf("read zone epoch before upstream query: %w", err)
	}
	resp, err := r.resolveRecursive(ctx, q, 0)
	if err != nil {
		return nil, err
	}
	write := store.CacheWrite{
		QName: q.Name, QType: q.Type, SourceType: q.Type,
		ExpectedZone: expectedZone, ExpectedZoneVersion: expectedVersion,
		ExpectedZoneEpoch: expectedEpoch, RequireZoneStable: true, RequireEpochStable: true,
		Payload: store.CachePayload{RCode: resp.Header.RCode(), Answers: resp.Answers, Authorities: resp.Authorities, Additionals: resp.Additionals},
	}
	if _, err := r.store.WriteCache(ctx, write); err != nil {
		r.metrics.WriteErrors.Add(1)
		r.metrics.RecordError("cache_write", err)
		switch errs.CodeOf(err) {
		case errs.ENegativeTTLUndefined:
			return nil, err
		case errs.EStaleBase:
			// A zone commit raced this upstream answer. Do not insert stale data;
			// the answer is still usable for this request.
			r.logger.Printf("discarding cache write after zone changed: %v", err)
		default:
			r.logger.Printf("upstream answer was not cached: %v", err)
		}
	}
	return resp, nil
}

func (r *Resolver) resolveRecursive(ctx context.Context, q dnsmsg.Question, hops int) (*dnsmsg.Message, error) {
	if hops > r.cfg.MaxCNAMEHops {
		return nil, errs.New(errs.EUpstreamBadResponse, "CNAME chain exceeds %d hops", r.cfg.MaxCNAMEHops)
	}
	q.Name = dnsmsg.CanonicalName(q.Name)
	up := &upstreamClient{addr: r.cfg.Upstreams[0], network: r.cfg.Network, timeout: r.cfg.UpstreamTimeout}
	resp, err := up.exchange(ctx, q.Name, q.Type)
	if err != nil {
		return nil, err
	}
	if resp.Header.RCode() == dnsmsg.RCodeNXDomain || q.Type == dnsmsg.TypeCNAME {
		return resp, nil
	}
	if hasAnswerType(resp.Answers, q.Type) {
		return resp, nil
	}
	target := cnameTarget(resp.Answers, q.Name)
	if target == "" {
		return resp, nil
	}
	next, err := r.resolveRecursive(ctx, dnsmsg.Question{Name: target, Type: q.Type, Class: q.Class}, hops+1)
	if err != nil {
		return nil, err
	}
	combined := resp.Clone()
	seen := make(map[string]bool)
	for _, group := range [][]dnsmsg.RR{combined.Answers, combined.Authorities, combined.Additionals} {
		for _, rr := range group {
			seen[rr.Name+"|"+fmt.Sprint(rr.Type)+"|"+rr.Canonical()] = true
		}
	}
	for _, rr := range next.Answers {
		key := rr.Name + "|" + fmt.Sprint(rr.Type) + "|" + rr.Canonical()
		if !seen[key] {
			combined.Answers = append(combined.Answers, rr)
			seen[key] = true
		}
	}
	if len(next.Authorities) > 0 {
		combined.Authorities = append([]dnsmsg.RR(nil), next.Authorities...)
	}
	for _, rr := range next.Additionals {
		key := rr.Name + "|" + fmt.Sprint(rr.Type) + "|" + rr.Canonical()
		if !seen[key] {
			combined.Additionals = append(combined.Additionals, rr)
			seen[key] = true
		}
	}
	combined.Header.Flags = (combined.Header.Flags &^ uint16(0x000f)) | uint16(next.Header.RCode())
	return combined, nil
}

func hasAnswerType(rrs []dnsmsg.RR, typ dnsmsg.Type) bool {
	for _, rr := range rrs {
		if rr.Type == typ {
			return true
		}
	}
	return false
}

func cnameTarget(rrs []dnsmsg.RR, owner string) string {
	owner = dnsmsg.CanonicalName(owner)
	for _, rr := range rrs {
		if rr.Type == dnsmsg.TypeCNAME && dnsmsg.CanonicalName(rr.Name) == owner && rr.Data.Target != "" {
			return dnsmsg.CanonicalName(rr.Data.Target)
		}
	}
	return ""
}

func responseFromEntry(query *dnsmsg.Message, entry *store.CacheEntry) *dnsmsg.Message {
	msg := &dnsmsg.Message{
		Questions:   append([]dnsmsg.Question(nil), query.Questions...),
		Answers:     dnsmsg.CloneRRs(entry.Payload.Answers),
		Authorities: dnsmsg.CloneRRs(entry.Payload.Authorities),
		Additionals: dnsmsg.CloneRRs(entry.Payload.Additionals),
	}
	msg.Header.ID = query.Header.ID
	msg.Header.Flags = dnsmsg.FlagQR | dnsmsg.FlagRA | (query.Header.Flags & dnsmsg.FlagRD) | uint16(entry.Payload.RCode)
	return msg
}

func sanitizeResponse(query, upstream *dnsmsg.Message) *dnsmsg.Message {
	msg := upstream.Clone()
	msg.Header.ID = query.Header.ID
	msg.Header.Flags = dnsmsg.FlagQR | dnsmsg.FlagRA | (query.Header.Flags & dnsmsg.FlagRD) | uint16(upstream.Header.RCode())
	msg.Questions = append([]dnsmsg.Question(nil), query.Questions...)
	msg.Header.QDCount = 1
	return msg
}

func ResolveErrorResponse(query *dnsmsg.Message, code errs.Code, detail string) *dnsmsg.Message {
	rcode := dnsmsg.RCodeServFail
	switch code {
	case errs.EMessageMalformed, errs.ETCPFrameInvalid:
		rcode = dnsmsg.RCodeFormErr
	case errs.EUnsupportedType:
		rcode = dnsmsg.RCodeNotImp
	}
	var msg *dnsmsg.Message
	if query != nil {
		msg = dnsmsg.NewResponse(query, rcode)
	} else {
		msg = &dnsmsg.Message{Header: dnsmsg.Header{Flags: dnsmsg.FlagQR | dnsmsg.FlagRA | uint16(rcode)}}
	}
	attachEDE(msg, string(code), detail)
	return msg
}

// attachEDE adds RFC 8914 Extended DNS Error option 15 ("Other") so local
// clients can observe the stable business code while still seeing the standard
// FORMERR/SERVFAIL rcode. The wire encoding is deliberately done here to keep
// the codec's OPT representation lossless.
func attachEDE(msg *dnsmsg.Message, code, detail string) {
	text := strings.TrimSpace(code + ": " + detail)
	if len(text) > 250 {
		text = text[:250]
	}
	raw := []byte{0, 15, byte(len(text) >> 8), byte(len(text))}
	raw = append(raw, []byte(text)...)
	msg.Additionals = append(msg.Additionals, dnsmsg.RR{
		Name: ".", Type: dnsmsg.TypeOPT, Class: 1232, TTL: 0, Data: dnsmsg.RData{Raw: raw},
	})
}
