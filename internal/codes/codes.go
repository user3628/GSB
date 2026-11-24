// Package codes defines the stable business result codes emitted by the
// validation engine. Every validation response carries one of these codes.
package codes

// Code is a machine-readable business result code.
type Code string

const (
	// OK means the ticket was valid and has been consumed exactly once.
	OK Code = "E_OK"
	// Signature means the integrity signature did not verify.
	Signature Code = "E_TICKET_SIGNATURE"
	// Expired means the ticket's expiry time is in the past.
	Expired Code = "E_TICKET_EXPIRED"
	// Audience means the presenting SP does not match the ticket's target SP.
	Audience Code = "E_TICKET_AUDIENCE"
	// Consumed means the one-time ticket was already redeemed.
	Consumed Code = "E_TICKET_CONSUMED"
	// Contention means the request exceeded the concurrency wait threshold.
	Contention Code = "E_TICKET_CONTENTION"
	// SnapshotVersion means a tenant snapshot chain gap or content mismatch
	// was detected; the tenant is taken out of service until rollback.
	SnapshotVersion Code = "E_SNAPSHOT_VERSION"
	// UnknownTicket means no ticket with the given identifier exists.
	UnknownTicket Code = "E_TICKET_UNKNOWN"
	// Internal is an unexpected storage failure.
	Internal Code = "E_INTERNAL"
)

// Failure reports whether the code represents an unsuccessful validation.
func (c Code) Failure() bool { return c != OK }

// String implements fmt.Stringer.
func (c Code) String() string { return string(c) }

// Error is a business error carrying a stable code and a descriptive message.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// New builds an Error with the given code and message.
func New(code Code, msg string) *Error { return &Error{Code: code, Message: msg} }

// From extracts the business code from an error, mapping non-business errors
// to Internal.
func From(err error) Code {
	if err == nil {
		return OK
	}
	if be, ok := err.(*Error); ok {
		return be.Code
	}
	return Internal
}
