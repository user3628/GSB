package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"vpnbench/internal/model"
)

// Parse parses either a top-level policy array or an object containing a
// "policies" array. It normalizes CIDRs and sorts policies by tunnel ID so that
// snapshots and reports have a stable representation.
func Parse(data []byte) (model.PolicySet, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	first, err := dec.Token()
	if err != nil {
		return model.PolicySet{}, policyError("invalid JSON", jsonOffset(err, 0))
	}

	var policies []model.Policy
	switch token := first.(type) {
	case json.Delim:
		switch token {
		case '[':
			policies, err = parseArray(dec, data)
		case '{':
			policies, err = parseEnvelope(dec, data)
		default:
			err = policyError("top-level value must be an array or object", 0)
		}
	default:
		err = policyError("top-level value must be an array or object", 0)
	}
	if err != nil {
		return model.PolicySet{}, err
	}

	if _, err := dec.Token(); err != io.EOF {
		return model.PolicySet{}, policyError("trailing data after policy table", jsonOffset(err, dec.InputOffset()))
	}

	seen := make(map[string]int64, len(policies))
	for i := range policies {
		p := &policies[i]
		if previous, exists := seen[p.TunnelID]; exists {
			return model.PolicySet{}, policyError(
				fmt.Sprintf("duplicate tunnel_id %q (first declared at byte %d)", p.TunnelID, previous),
				p.InputOffset,
			)
		}
		seen[p.TunnelID] = p.InputOffset
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].TunnelID < policies[j].TunnelID
	})
	digest, err := Digest(policies)
	if err != nil {
		return model.PolicySet{}, model.NewError(model.CodeInternal, err.Error(), 0)
	}
	return model.PolicySet{Policies: policies, Digest: digest}, nil
}

func parseArray(dec *json.Decoder, data []byte) ([]model.Policy, error) {
	var policies []model.Policy
	for dec.More() {
		start := skipSpace(data, dec.InputOffset())
		item, err := parsePolicy(dec, data, start)
		if err != nil {
			return nil, err
		}
		policies = append(policies, item)
	}
	if _, err := dec.Token(); err != nil {
		return nil, policyError("unterminated policy array", dec.InputOffset())
	}
	return policies, nil
}

func parseEnvelope(dec *json.Decoder, data []byte) ([]model.Policy, error) {
	var policies []model.Policy
	found := false
	for dec.More() {
		start := skipSpace(data, dec.InputOffset())
		keyToken, err := dec.Token()
		if err != nil {
			return nil, policyError("invalid envelope key", start)
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, policyError("envelope key must be a string", start)
		}
		if key != "policies" {
			var discard json.RawMessage
			if err := dec.Decode(&discard); err != nil {
				return nil, policyError("invalid envelope value for "+key, dec.InputOffset())
			}
			continue
		}
		if found {
			return nil, policyError("duplicate policies field", start)
		}
		found = true
		token, err := dec.Token()
		if err != nil {
			return nil, policyError("missing policies array", start)
		}
		delim, ok := token.(json.Delim)
		if !ok || delim != '[' {
			return nil, policyError("policies must be an array", start)
		}
		policies, err = parseArray(dec, data)
		if err != nil {
			return nil, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return nil, policyError("unterminated policy envelope", dec.InputOffset())
	}
	if !found {
		return nil, policyError("missing policies array", 0)
	}
	return policies, nil
}

func parsePolicy(dec *json.Decoder, data []byte, objectOffset int64) (model.Policy, error) {
	token, err := dec.Token()
	if err != nil {
		return model.Policy{}, policyError("invalid policy object", objectOffset)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return model.Policy{}, policyError("policy entry must be an object", objectOffset)
	}

	p := model.Policy{InputOffset: objectOffset}
	seen := make(map[string]bool, 5)
	for dec.More() {
		keyOffset := skipSpace(data, dec.InputOffset())
		keyToken, err := dec.Token()
		if err != nil {
			return model.Policy{}, policyError("invalid policy field", keyOffset)
		}
		key, ok := keyToken.(string)
		if !ok {
			return model.Policy{}, policyError("policy field name must be a string", keyOffset)
		}
		if seen[key] {
			return model.Policy{}, policyError("duplicate policy field "+key, keyOffset)
		}
		seen[key] = true

		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return model.Policy{}, policyError("invalid value for "+key, keyOffset)
		}
		switch key {
		case "tunnel_id":
			value, err := rawString(raw, keyOffset)
			if err != nil {
				return model.Policy{}, err
			}
			if err := validateTunnelID(value, keyOffset); err != nil {
				return model.Policy{}, err
			}
			p.TunnelID = value
		case "destination_cidr":
			value, err := rawString(raw, keyOffset)
			if err != nil {
				return model.Policy{}, err
			}
			_, network, parseErr := net.ParseCIDR(strings.TrimSpace(value))
			if parseErr != nil {
				return model.Policy{}, policyError("invalid destination_cidr "+strconv.Quote(value), keyOffset)
			}
			p.DestinationCIDR = network.String()
		case "priority":
			value, err := rawNonNegativeInt(raw, keyOffset, key)
			if err != nil {
				return model.Policy{}, err
			}
			p.Priority = value
		case "bandwidth":
			value, err := rawNonNegativeInt(raw, keyOffset, key)
			if err != nil {
				return model.Policy{}, err
			}
			p.Bandwidth = value
		case "forwarding_cost":
			value, err := rawNonNegativeInt(raw, keyOffset, key)
			if err != nil {
				return model.Policy{}, err
			}
			p.ForwardingCost = value
		default:
			return model.Policy{}, policyError("unknown policy field "+key, keyOffset)
		}
	}
	if _, err := dec.Token(); err != nil {
		return model.Policy{}, policyError("unterminated policy object", objectOffset)
	}

	required := []struct {
		name string
		ok   bool
	}{
		{"tunnel_id", p.TunnelID != ""},
		{"destination_cidr", p.DestinationCIDR != ""},
		{"priority", seen["priority"]},
		{"bandwidth", seen["bandwidth"]},
		{"forwarding_cost", seen["forwarding_cost"]},
	}
	for _, field := range required {
		if !field.ok {
			return model.Policy{}, policyError("missing required policy field "+field.name, objectOffset)
		}
	}
	return p, nil
}

func validateTunnelID(value string, offset int64) error {
	if value == "" {
		return policyError("tunnel_id must not be empty", offset)
	}
	if strings.TrimSpace(value) != value {
		return policyError("tunnel_id must not have surrounding whitespace", offset)
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return policyError("tunnel_id contains whitespace or control characters", offset)
		}
	}
	return nil
}

func rawString(raw json.RawMessage, offset int64) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", policyError("value must be a string", offset)
	}
	return value, nil
}

func rawNonNegativeInt(raw json.RawMessage, offset int64, field string) (int64, error) {
	text := strings.TrimSpace(string(raw))
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, policyError(field+" must be an integer in int64 range", offset)
	}
	if value < 0 {
		return 0, policyError(field+" must be non-negative", offset)
	}
	return value, nil
}

func Digest(policies []model.Policy) (string, error) {
	type canonicalPolicy struct {
		TunnelID        string `json:"tunnel_id"`
		DestinationCIDR string `json:"destination_cidr"`
		Priority        int64  `json:"priority"`
		Bandwidth       int64  `json:"bandwidth"`
		ForwardingCost  int64  `json:"forwarding_cost"`
	}
	canonical := make([]canonicalPolicy, len(policies))
	for i, p := range policies {
		canonical[i] = canonicalPolicy{
			TunnelID:        p.TunnelID,
			DestinationCIDR: p.DestinationCIDR,
			Priority:        p.Priority,
			Bandwidth:       p.Bandwidth,
			ForwardingCost:  p.ForwardingCost,
		}
	}
	sort.Slice(canonical, func(i, j int) bool {
		return canonical[i].TunnelID < canonical[j].TunnelID
	})
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func Summary(policies []model.Policy) (tunnelIDs []string, digest string) {
	tunnelIDs = make([]string, 0, len(policies))
	for _, p := range policies {
		tunnelIDs = append(tunnelIDs, p.TunnelID)
	}
	sort.Strings(tunnelIDs)
	digest, _ = Digest(policies)
	return tunnelIDs, digest
}

func policyError(message string, offset int64) *model.CodedError {
	return model.NewError(model.CodePolicyInvalid, message, offset)
}

func jsonOffset(err error, fallback int64) int64 {
	var syntax *json.SyntaxError
	if ok := asJSONSyntax(err, &syntax); ok {
		return syntax.Offset
	}
	return fallback
}

func asJSONSyntax(err error, target **json.SyntaxError) bool {
	for err != nil {
		if value, ok := err.(*json.SyntaxError); ok {
			*target = value
			return true
		}
		type unwrapper interface{ Unwrap() error }
		next, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = next.Unwrap()
	}
	return false
}

func skipSpace(data []byte, offset int64) int64 {
	if offset < 0 {
		offset = 0
	}
	for offset < int64(len(data)) {
		switch data[offset] {
		case ' ', '\t', '\r', '\n':
			offset++
		default:
			return offset
		}
	}
	return offset
}
