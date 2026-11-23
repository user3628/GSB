package statemachine

import "upstreampool/internal/model"

type Stage string

const (
	IDLE          Stage = "IDLE"
	CONNECTING    Stage = "CONNECTING"
	HANDSHAKING   Stage = "HANDSHAKING"
	SEND_BODY     Stage = "SEND_BODY"
	RECV_RESPONSE Stage = "RECV_RESPONSE"
	DRAINING      Stage = "DRAINING"
	CLOSED        Stage = "CLOSED"
)

var transitions = map[Stage]map[Stage]bool{
	CONNECTING:    {HANDSHAKING: true, CLOSED: true},
	HANDSHAKING:   {SEND_BODY: true, DRAINING: true, CLOSED: true},
	SEND_BODY:     {RECV_RESPONSE: true, DRAINING: true, CLOSED: true},
	RECV_RESPONSE: {IDLE: true, DRAINING: true, CLOSED: true},
	IDLE:          {HANDSHAKING: true, DRAINING: true},
	DRAINING:      {CLOSED: true},
	CLOSED:        {},
}

func Valid(from, to Stage) bool {
	if from == "" {
		return to == CONNECTING
	}
	next, ok := transitions[from]
	return ok && next[to]
}

func Validate(from, to Stage) *model.BusinessError {
	if Valid(from, to) {
		return nil
	}
	return model.NewError(409, model.ErrInvalidTransition, "illegal connection stage transition", map[string]any{
		"from": from,
		"to":   to,
	})
}

func All() []Stage {
	return []Stage{IDLE, CONNECTING, HANDSHAKING, SEND_BODY, RECV_RESPONSE, DRAINING, CLOSED}
}
