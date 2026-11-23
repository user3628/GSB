package dnsdetect

import (
	"errors"
	"fmt"
)

// Stable business codes. Callers should branch on Code, not on Error text.
const (
	CodeBinaryMagic       = "E_BINARY_MAGIC"
	CodeBinaryTruncated   = "E_BINARY_TRUNCATED"
	CodeBinaryField       = "E_BINARY_FIELD"
	CodeUnknownVersion    = "E_UNKNOWN_VERSION"
	CodeJSONSyntax        = "E_JSON_SYNTAX"
	CodeJSONField         = "E_JSON_FIELD"
	CodeTextLength        = "E_TEXT_LENGTH"
	CodeTextField         = "E_TEXT_FIELD"
	CodeTXID              = "E_TXID"
	CodeDNSName           = "E_DNS_NAME"
	CodeTimeOverflow      = "E_TIME_OVERFLOW"
	CodeTimeFormat        = "E_TIME_FORMAT"
	CodeEventBudget       = "E_EVENT_BUDGET"
	CodeWindowBudget      = "E_WINDOW_BUDGET"
	CodeTXIDQNameConflict = "E_TXID_QNAME_CONFLICT"
	CodeSnapshotVersion   = "E_SNAPSHOT_VERSION"
	CodeSnapshotReplaced  = "E_SNAPSHOT_REPLACED"
	CodeSnapshotFormat    = "E_SNAPSHOT_FORMAT"
	CodeConfig            = "E_CONFIG"
	CodeSourceKind        = "E_SOURCE_KIND"
	CodeIO                = "E_IO"

	CodeUnsolicitedResponse  = "POISON_UNSOLICITED_RESPONSE"
	CodeIrrelevantAdditional = "POISON_IRRELEVANT_ADDITIONAL"
	CodeTTLMismatch          = "POISON_TTL_MISMATCH"
	CodeMultiAddress         = "POISON_MULTI_ADDRESS"
	CodeUncertainAssociation = "ASSOC_UNCERTAIN"
)

// BusinessError is the stable error envelope returned by the component.
// Offset is a byte offset in the source file when Source is a parsed file.
type BusinessError struct {
	Code   string
	Source string
	Offset int64
	Detail string
	Cause  error
}

func (e *BusinessError) Error() string {
	loc := e.Source
	if loc == "" {
		loc = "<component>"
	}
	if e.Offset >= 0 {
		return fmt.Sprintf("%s at %s:%d: %s", e.Code, loc, e.Offset, e.Detail)
	}
	return fmt.Sprintf("%s at %s: %s", e.Code, loc, e.Detail)
}

func (e *BusinessError) Unwrap() error { return e.Cause }

func newBusinessError(code, source string, offset int64, detail string, cause error) *BusinessError {
	return &BusinessError{Code: code, Source: source, Offset: offset, Detail: detail, Cause: cause}
}

// CodeOf extracts the stable code from err.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var businessErr *BusinessError
	if errors.As(err, &businessErr) {
		return businessErr.Code
	}
	return ""
}

// OffsetOf extracts the source offset from err, or -1 when unavailable.
func OffsetOf(err error) int64 {
	var businessErr *BusinessError
	if errors.As(err, &businessErr) {
		return businessErr.Offset
	}
	return -1
}
