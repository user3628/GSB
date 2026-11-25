package model

import "fmt"

// BizError is the only error type that crosses stage boundaries. It carries a
// stable business code, the ticket and stage where it happened and a small
// diagnostic context. Generic errors must be wrapped rather than substituted,
// so the verification tool always sees one of the documented E_* codes.
type BizError struct {
	Code     string            `json:"code"`
	Stage    string            `json:"stage"`
	TicketID string            `json:"ticket_id,omitempty"`
	Message  string            `json:"message"`
	Context  map[string]string `json:"context,omitempty"`
}

func (e *BizError) Error() string {
	if e.TicketID == "" {
		return fmt.Sprintf("%s @ %s: %s", e.Code, e.Stage, e.Message)
	}
	return fmt.Sprintf("%s @ %s ticket=%s: %s", e.Code, e.Stage, e.TicketID, e.Message)
}

// NewBizError builds a business error with an optional diagnostic context.
func NewBizError(code, stage, ticketID, message string, ctx map[string]string) *BizError {
	return &BizError{Code: code, Stage: stage, TicketID: ticketID, Message: message, Context: ctx}
}

// AsBizError extracts a *BizError from err if present.
func AsBizError(err error) (*BizError, bool) {
	if err == nil {
		return nil, false
	}
	if be, ok := err.(*BizError); ok {
		return be, true
	}
	return nil, false
}
