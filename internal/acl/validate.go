package acl

import (
	"fmt"
	"regexp"
	"strings"

	"example.com/ntfsacl/internal/domain"
)

const CodeACLMalformed = "E_ACL_MALFORMED"

type MalformedError struct {
	Code     string `json:"code"`
	NodePath string `json:"node_path"`
	ACEIndex int    `json:"ace_index"`
	Detail   string `json:"detail"`
}

func (e *MalformedError) Error() string {
	return fmt.Sprintf("%s: node=%s ace=%d: %s", e.Code, e.NodePath, e.ACEIndex, e.Detail)
}

var sidPattern = regexp.MustCompile(`^S-1-(?:[0-9]+-){1,15}[0-9]+$`)

func ValidateDescriptor(nodePath, kind string, d domain.Descriptor) error {
	if d.Schema != "" && d.Schema != "ntfsacl/descriptor/v1" {
		return &MalformedError{CodeACLMalformed, nodePath, -1, "unsupported descriptor schema"}
	}
	if !d.DACLPresent {
		return &MalformedError{CodeACLMalformed, nodePath, -1, "DACL is absent"}
	}
	if len(d.ACEs) == 0 {
		return &MalformedError{CodeACLMalformed, nodePath, -1, "DACL is empty"}
	}
	for i, ace := range d.ACEs {
		if ace.Index != i {
			return &MalformedError{CodeACLMalformed, nodePath, i, fmt.Sprintf("ACE index is %d, want %d", ace.Index, i)}
		}
		switch strings.ToUpper(ace.Type) {
		case domain.ACETypeAllow, domain.ACETypeDeny, domain.ACETypeAudit:
		default:
			return &MalformedError{CodeACLMalformed, nodePath, i, "unknown ACE type"}
		}
		if !sidPattern.MatchString(ace.SID) {
			return &MalformedError{CodeACLMalformed, nodePath, i, "subject is not a SID"}
		}
		if !domain.ValidAccessMask(ace.AccessMask) {
			return &MalformedError{CodeACLMalformed, nodePath, i, fmt.Sprintf("access mask 0x%08x is empty or out of range", ace.AccessMask)}
		}
		if ace.Flags.IO && !ace.Flags.AnyInheritance() {
			return &MalformedError{CodeACLMalformed, nodePath, i, "IO requires OI and/or CI"}
		}
		if ace.Flags.NP && !ace.Flags.AnyInheritance() {
			return &MalformedError{CodeACLMalformed, nodePath, i, "NP requires OI and/or CI"}
		}
		if ace.Flags.IO && ace.Flags.NP {
			return &MalformedError{CodeACLMalformed, nodePath, i, "IO and NP are incompatible"}
		}
		if ace.Flags.CI && kind == "FILE" {
			return &MalformedError{CodeACLMalformed, nodePath, i, "CI is not valid on a file object"}
		}
	}
	intentKeys := make(map[string]bool, len(d.Intents))
	for i, intent := range d.Intents {
		key := strings.ToUpper(intent.SID) + "|" + strings.ToUpper(intent.Access)
		if intentKeys[key] {
			return &MalformedError{CodeACLMalformed, nodePath, -1, "duplicate declared intent"}
		}
		intentKeys[key] = true
		if !sidPattern.MatchString(intent.SID) {
			return &MalformedError{CodeACLMalformed, nodePath, -1, fmt.Sprintf("intent %d has invalid SID", i)}
		}
		if _, err := domain.ParseAccess(intent.Access); err != nil {
			return &MalformedError{CodeACLMalformed, nodePath, -1, fmt.Sprintf("intent %d: %v", i, err)}
		}
		switch intent.Expected {
		case domain.DecisionAllow, domain.DecisionDeny, domain.DecisionNotGranted:
		default:
			return &MalformedError{CodeACLMalformed, nodePath, -1, fmt.Sprintf("intent %d has invalid expected decision", i)}
		}
	}
	return nil
}
