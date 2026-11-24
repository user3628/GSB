package engine

import (
	"fmt"

	"casengine/internal/store"
	"casengine/internal/token"
)

// IssueRequest describes a ticket to mint.
type IssueRequest struct {
	ID         string
	TenantID   string
	SPID       string
	UserID     string
	TTLSeconds int64
	Seq        int64
}

// IssuedTicket bundles the durable record and its wire representation.
type IssuedTicket struct {
	Ticket string
	Record store.Ticket
}

// Issue mints, signs and persists a ticket, producing a new snapshot version.
func (e *Engine) Issue(req IssueRequest) (*IssuedTicket, error) {
	secret, err := e.store.TenantSecret(req.TenantID)
	if err != nil {
		return nil, err
	}
	now := e.clk.Unix()
	payload := token.Payload{
		ID:        req.ID,
		TenantID:  req.TenantID,
		SPID:      req.SPID,
		UserID:    req.UserID,
		IssuedAt:  now,
		ExpiresAt: now + req.TTLSeconds,
	}
	raw, err := token.Sign([]byte(secret), payload)
	if err != nil {
		return nil, err
	}
	rec := store.Ticket{
		ID: req.ID, TenantID: req.TenantID, SPID: req.SPID, UserID: req.UserID,
		Raw: raw, IssuedAt: now, ExpiresAt: payload.ExpiresAt, Status: store.StatusValid,
	}
	if _, err := e.store.Issue(rec, e.clk.EventNS(req.Seq)); err != nil {
		return nil, fmt.Errorf("persist issue: %w", err)
	}
	return &IssuedTicket{Ticket: raw, Record: rec}, nil
}
