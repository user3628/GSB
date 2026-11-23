// Package bincode implements the self-describing compact binary audit log:
// a fixed version header, length-prefixed variable records and a trailing
// checksum block. v1 and v2 share the wire framing but differ in field layout
// semantics (v2 adds PPID and module-load events).
package bincode

import "forensicworkbench/internal/codes"

// Wire constants.
const (
	HeaderMagic = "FWRK"
	TailMagic   = "FWTB"
	HeaderLen   = 18
	TailLen     = 16
)

// Supported binary wire versions.
const (
	V1 uint8 = 1
	V2 uint8 = 2
)

// Event type code enumeration on the wire. v2 adds TypeModuleLoad; any code not
// listed here is preserved raw and reported as E_UNKNOWN_EVENT.
const (
	TypeProcessCreate uint8 = 1
	TypeProcessExit   uint8 = 2
	TypeFileAccess    uint8 = 3
	TypeRegistryWrite uint8 = 4
	TypeNetworkConn   uint8 = 5
	TypeModuleLoad    uint8 = 6 // v2 only
)

// Timestamp encoding codes.
const (
	EncEpochMS  uint8 = 1
	EncRFC3339  uint8 = 2
	EncFiletime uint8 = 3
)

// Record is a single, version-neutral event ready to be encoded or just decoded.
type Record struct {
	TypeCode   uint8
	TimeEnc    uint8
	RawTime    string
	SessionID  string
	PID        int64
	PPID       int64 // populated by v2; v2 semantic field
	Subject    string
	Object     string
	RawPayload []byte // optional extra payload, retained verbatim
}

// NormalizeType maps a wire type code to the canonical event family.
func NormalizeType(version, code uint8) (codes.EventType, bool) {
	switch code {
	case TypeProcessCreate:
		return codes.EvProcessCreate, true
	case TypeProcessExit:
		return codes.EvProcessExit, true
	case TypeFileAccess:
		return codes.EvFileAccess, true
	case TypeRegistryWrite:
		return codes.EvRegistry, true
	case TypeNetworkConn:
		return codes.EvNetwork, true
	case TypeModuleLoad:
		// module load is only valid under v2 semantics.
		if version == V2 {
			return codes.EvModuleLoad, true
		}
		return codes.EvUnknown, false
	default:
		return codes.EvUnknown, false
	}
}

// FormatVersion returns the canonical format-version label for a wire version.
func FormatVersion(version uint8) string {
	switch version {
	case V1:
		return codes.FmtBinaryV1
	case V2:
		return codes.FmtBinaryV2
	default:
		return "binary/unknown"
	}
}
