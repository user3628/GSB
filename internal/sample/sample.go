// Package sample generates the fully deterministic demo inputs: tenants,
// service providers, users, signing keys and concurrency replay scenarios.
//
// Everything is derived from a fixed seed via a split-stream PRNG, so the
// generated parameters are identical on every machine and every run. No
// network access or external account is involved.
package sample

import (
	"math/rand"
)

// DefaultSeed is the fixed seed used by the built-in sample generator.
const DefaultSeed int64 = 20261124

// SP describes a protected service provider registered in a tenant.
type SP struct {
	ID   string `json:"spId"`
	Name string `json:"name"`
}

// User describes a directory user inside a tenant.
type User struct {
	ID   string `json:"userId"`
	Name string `json:"name"`
}

// Tenant is a self-contained security boundary with its own HMAC secret.
type Tenant struct {
	ID     string `json:"tenantId"`
	Name   string `json:"name"`
	Secret string `json:"secret"` // base64url HMAC-SHA256 key
	SPs    []SP   `json:"sps"`
	Users  []User `json:"users"`
}

// Params bundles every generated input needed by a deterministic run.
type Params struct {
	Seed      int64      `json:"seed"`
	Tenants   []Tenant   `json:"tenants"`
	Scenarios []Scenario `json:"scenarios"`
}

// Scenario is one replay script. Concurrency groups run sequentially; requests
// inside a group are issued concurrently in goroutines.
type Scenario struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Groups      []Group `json:"groups"`
}

// Group is a fan-out of concurrent requests over distinct tickets.
type Group struct {
	TicketIdx  []int `json:"ticketIdx"`
	Duplicates int   `json:"duplicates"` // extra concurrent attempts per ticket
}

// Request is a single validation attempt within a scenario group.
type Request struct {
	Seq          int64  `json:"seq"`
	TicketKey    string `json:"ticketKey"`
	TenantID     string `json:"tenantId"`
	SPID         string `json:"spId"`
	UserID       string `json:"userId"`
	PresentSP    string `json:"presentSp"`
	Tampered     bool   `json:"tampered"`
	TTLSeconds   int64  `json:"ttlSeconds"`
	ExpireBefore bool   `json:"expireBefore"`
}

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

func stream(seed, salt int64) *rand.Rand {
	return rand.New(rand.NewSource(seed*1_000_003 + salt))
}

func randToken(r *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

// Generate builds the deterministic parameter set.
func Generate(seed int64) *Params {
	if seed == 0 {
		seed = DefaultSeed
	}
	p := &Params{Seed: seed}

	tenantNames := []string{"acme", "globex"}
	for ti, name := range tenantNames {
		tr := stream(seed, int64(ti)+1)
		t := Tenant{
			ID:     "tenant-" + name,
			Name:   name,
			Secret: "key-" + randToken(tr, 24),
		}
		for si := 0; si < 3; si++ {
			t.SPs = append(t.SPs, SP{
				ID:   "sp-" + name + "-" + string(rune('a'+si)),
				Name: "Service " + string(rune('A'+si)),
			})
		}
		for ui := 0; ui < 4; ui++ {
			t.Users = append(t.Users, User{
				ID:   "user-" + name + "-" + string(rune('0'+ui)),
				Name: "User " + string(rune('0'+ui)) + " (" + name + ")",
			})
		}
		p.Tenants = append(p.Tenants, t)
	}

	p.Scenarios = []Scenario{
		{
			Name:        "happy-path",
			Description: "distinct tickets, each validated once -> all succeed",
			Groups: []Group{
				{TicketIdx: []int{0, 1, 2}, Duplicates: 0},
			},
		},
		{
			Name:        "concurrent-one-time",
			Description: "two tickets hit by 4 concurrent requests each; exactly one success per ticket",
			Groups: []Group{
				{TicketIdx: []int{3, 4}, Duplicates: 3},
			},
		},
		{
			Name:        "failure-mix",
			Description: "tampered signature, wrong audience, expired ticket, and a healthy control",
			Groups: []Group{
				{TicketIdx: []int{5, 6, 7, 8}, Duplicates: 0},
			},
		},
	}
	return p
}

// Requests expands a scenario into deterministic request descriptors. The
// ticket index space is global across scenarios in issuance order.
func (p *Params) Requests(sc Scenario, baseIdx int) []Request {
	var out []Request
	var seq int64 = 1
	for _, g := range sc.Groups {
		for _, idx := range g.TicketIdx {
			t := p.ticketSpec(baseIdx + idx)
			out = append(out, t.request(seq, false, 0))
			seq++
			for d := 0; d < g.Duplicates; d++ {
				out = append(out, t.request(seq, false, 0))
				seq++
			}
		}
	}
	return out
}

type ticketSpec struct {
	tenant Tenant
	sp     SP
	user   User
}

func (p *Params) ticketSpec(globalIdx int) ticketSpec {
	t := p.Tenants[globalIdx%len(p.Tenants)]
	sp := t.SPs[(globalIdx/len(p.Tenants))%len(t.SPs)]
	user := t.Users[(globalIdx/len(p.Tenants))%len(t.Users)]
	return ticketSpec{tenant: t, sp: sp, user: user}
}

// TicketSpec returns the deterministic (tenant, SP, user) triple allocated to a
// global ticket index. It is exported so the CLI can expand replay scripts.
func (p *Params) TicketSpec(globalIdx int) (tenant Tenant, sp SP, user User) {
	spec := p.ticketSpec(globalIdx)
	return spec.tenant, spec.sp, spec.user
}

func (s ticketSpec) request(seq int64, tamper bool, _ int) Request {
	return Request{
		Seq:        seq,
		TenantID:   s.tenant.ID,
		SPID:       s.sp.ID,
		UserID:     s.user.ID,
		PresentSP:  s.sp.ID,
		Tampered:   tamper,
		TTLSeconds: 300,
	}
}

// FaultRequests constructs the five injection-suite request descriptors.
// Each tenant hosts its own tickets so one injection cannot affect another.
func (p *Params) FaultRequests(seqStart int64) []Request {
	var out []Request
	seq := seqStart
	caseNames := []string{"sig", "aud", "exp", "race", "ok"}
	caseNo := 0
	add := func(tenantIdx, spOff, userOff int, tamper, wrongSP, expire bool, ttl int64, dup int) {
		t := p.Tenants[tenantIdx]
		sp := t.SPs[spOff]
		user := t.Users[userOff]
		caseName := caseNames[caseNo]
		caseNo++
		present := sp.ID
		if wrongSP {
			present = t.SPs[(spOff+1)%len(t.SPs)].ID
		}
		for i := 0; i <= dup; i++ {
			out = append(out, Request{
				Seq:          seq,
				TicketKey:    "fault-" + caseName,
				TenantID:     t.ID,
				SPID:         sp.ID,
				UserID:       user.ID,
				PresentSP:    present,
				Tampered:     tamper,
				TTLSeconds:   ttl,
				ExpireBefore: expire,
			})
			seq++
		}
	}
	add(0, 0, 0, true, false, false, 315360000, 0)  // signature tamper
	add(0, 1, 1, false, true, false, 315360000, 0)  // audience mismatch
	add(0, 2, 2, false, false, true, 1, 0)          // expired (advance clock)
	add(1, 0, 0, false, false, false, 315360000, 3) // concurrent replay race
	add(1, 1, 1, false, false, false, 315360000, 0) // healthy control
	return out
}
