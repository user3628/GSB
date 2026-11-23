package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type State string

const (
	StateHandshake State = "HANDSHAKE"
	StateAuth      State = "AUTH"
	StateReady     State = "READY"
	StateTransfer  State = "TRANSFER"
	StateClosed    State = "CLOSED"
)

var allStates = []State{StateHandshake, StateAuth, StateReady, StateTransfer, StateClosed}

func States() []State { return append([]State(nil), allStates...) }

func (s State) Valid() bool {
	for _, candidate := range allStates {
		if s == candidate {
			return true
		}
	}
	return false
}

func (s State) MarshalJSON() ([]byte, error) { return json.Marshal(string(s)) }

func (s *State) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	candidate := State(v)
	if !candidate.Valid() {
		return fmt.Errorf("unknown state %q", v)
	}
	*s = candidate
	return nil
}

type MessageType byte

const (
	MsgHandshake MessageType = 0x01
	MsgAuth      MessageType = 0x02
	MsgData      MessageType = 0x03
	MsgPing      MessageType = 0x04
	MsgClose     MessageType = 0x05
)

var messageNames = map[MessageType]string{
	MsgHandshake: "HANDSHAKE",
	MsgAuth:      "AUTH",
	MsgData:      "DATA",
	MsgPing:      "PING",
	MsgClose:     "CLOSE",
	ResponseType: "RESPONSE",
}

func MessageTypes() []MessageType {
	return []MessageType{MsgHandshake, MsgAuth, MsgData, MsgPing, MsgClose}
}

func (m MessageType) String() string {
	if name, ok := messageNames[m]; ok {
		return name
	}
	return fmt.Sprintf("0x%02x", byte(m))
}

func (m MessageType) Valid() bool {
	_, ok := messageNames[m]
	return ok && m != ResponseType
}

func (m MessageType) MarshalJSON() ([]byte, error) { return json.Marshal(m.String()) }

func (m *MessageType) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	for typ, name := range messageNames {
		if name == v && typ != ResponseType {
			*m = typ
			return nil
		}
	}
	return fmt.Errorf("unknown message type %q", v)
}

type Transition struct {
	From State
	Type MessageType
	To   State
	Code string
}

func (t Transition) String() string {
	return fmt.Sprintf("%s -[%s]-> %s (%s)", t.From, t.Type, t.To, t.Code)
}

type rule struct {
	from State
	typ  MessageType
	to   State
}

var transitionTable = []rule{
	{StateHandshake, MsgHandshake, StateAuth},
	{StateAuth, MsgAuth, StateReady},
	{StateReady, MsgData, StateTransfer},
	{StateTransfer, MsgData, StateTransfer},
	{StateReady, MsgPing, StateReady},
	{StateTransfer, MsgPing, StateTransfer},
	{StateReady, MsgClose, StateClosed},
	{StateTransfer, MsgClose, StateClosed},
}

func Rules() []Transition {
	out := make([]Transition, 0, len(transitionTable))
	for _, r := range transitionTable {
		out = append(out, Transition{From: r.from, Type: r.typ, To: r.to, Code: "E_OK"})
	}
	return out
}

type Machine struct {
	state State
	trace []Transition
}

func NewMachine() *Machine {
	return &Machine{state: StateHandshake}
}

func (m *Machine) State() State { return m.state }

func (m *Machine) Trace() []Transition { return append([]Transition(nil), m.trace...) }

// Apply adjudicates a parsed frame. Parsing/checksum errors are returned before
// Apply is called and therefore never mutate the state or trace.
func (m *Machine) Apply(typ MessageType) Transition {
	from := m.state
	for _, r := range transitionTable {
		if r.from == from && r.typ == typ {
			m.state = r.to
			tr := Transition{From: from, Type: typ, To: r.to, Code: "E_OK"}
			m.trace = append(m.trace, tr)
			return tr
		}
	}
	tr := Transition{From: from, Type: typ, To: from, Code: "E_ILLEGAL_TRANSITION"}
	m.trace = append(m.trace, tr)
	return tr
}

type responseWire struct {
	Status uint8 `json:"status"`
	From   State `json:"from"`
	To     State `json:"to"`
}

func EncodeResponse(code string, from, to State) (Frame, error) {
	status, err := statusForCode(code)
	if err != nil {
		return Frame{}, err
	}
	payload, err := json.Marshal(responseWire{Status: status, From: from, To: to})
	if err != nil {
		return Frame{}, err
	}
	return Frame{Type: ResponseType, Payload: payload}, nil
}

func DecodeResponse(f Frame) (code string, from, to State, err error) {
	if f.Type != ResponseType {
		return "", "", "", fmt.Errorf("unexpected response type %s", f.Type)
	}
	var rw responseWire
	dec := json.NewDecoder(bytes.NewReader(f.Payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rw); err != nil {
		return "", "", "", err
	}
	code, err = codeForStatus(rw.Status)
	return code, rw.From, rw.To, err
}

func statusForCode(code string) (uint8, error) {
	switch code {
	case "E_OK":
		return 0, nil
	case "E_ILLEGAL_TRANSITION":
		return 1, nil
	case "E_BAD_CHECKSUM":
		return 2, nil
	case "E_PAYLOAD_OVERFLOW":
		return 3, nil
	case "E_TRUNCATED_FRAME":
		return 4, nil
	default:
		return 0, fmt.Errorf("code %q is not representable on the wire", code)
	}
}

func codeForStatus(status uint8) (string, error) {
	switch status {
	case 0:
		return "E_OK", nil
	case 1:
		return "E_ILLEGAL_TRANSITION", nil
	case 2:
		return "E_BAD_CHECKSUM", nil
	case 3:
		return "E_PAYLOAD_OVERFLOW", nil
	case 4:
		return "E_TRUNCATED_FRAME", nil
	default:
		return "", fmt.Errorf("unknown response status %d", status)
	}
}
