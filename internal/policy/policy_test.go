package policy

import (
	"strings"
	"testing"

	"vpnbench/internal/model"
)

func TestParseNormalizesAndSorts(t *testing.T) {
	input := `[
  {"tunnel_id":"zeta","destination_cidr":"10.1.2.3/8","priority":1,"bandwidth":2,"forwarding_cost":0},
  {"tunnel_id":"alpha","destination_cidr":"192.168.1.8/16","priority":0,"bandwidth":3,"forwarding_cost":4}
]`
	set, err := Parse([]byte(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(set.Policies) != 2 || set.Policies[0].TunnelID != "alpha" || set.Policies[1].TunnelID != "zeta" {
		t.Fatalf("policies not sorted: %+v", set.Policies)
	}
	if set.Policies[1].DestinationCIDR != "10.0.0.0/8" {
		t.Fatalf("CIDR not normalized: %s", set.Policies[1].DestinationCIDR)
	}
	if set.Digest == "" {
		t.Fatal("digest is empty")
	}
}

func TestDigestIgnoresInputOrderAndOffsets(t *testing.T) {
	left := `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":2,"forwarding_cost":3}]`
	right := `[{"forwarding_cost":3,"bandwidth":2,"priority":1,"destination_cidr":"10.0.0.0/8","tunnel_id":"a"}]`
	leftSet, err := Parse([]byte(left))
	if err != nil {
		t.Fatal(err)
	}
	rightSet, err := Parse([]byte(right))
	if err != nil {
		t.Fatal(err)
	}
	if leftSet.Digest != rightSet.Digest {
		t.Fatalf("digest mismatch: %s != %s", leftSet.Digest, rightSet.Digest)
	}
}

func TestParseRejectsInvalidPolicyWithOffset(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"invalid CIDR", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/99","priority":1,"bandwidth":1,"forwarding_cost":1}]`},
		{"negative priority", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":-1,"bandwidth":1,"forwarding_cost":1}]`},
		{"negative cost", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":1,"forwarding_cost":-1}]`},
		{"negative bandwidth", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":-1,"forwarding_cost":1}]`},
		{"fractional integer", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1.5,"bandwidth":1,"forwarding_cost":1}]`},
		{"unknown field", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":1,"forwarding_cost":1,"x":2}]`},
		{"duplicate tunnel", `[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":1,"forwarding_cost":1},{"tunnel_id":"a","destination_cidr":"192.168.0.0/16","priority":1,"bandwidth":1,"forwarding_cost":1}]`},
		{"trailing data", `[] garbage`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := Parse([]byte(testCase.body))
			if model.CodeOf(err) != model.CodePolicyInvalid {
				t.Fatalf("code = %s, want %s (err=%v)", model.CodeOf(err), model.CodePolicyInvalid, err)
			}
			coded, ok := err.(*model.CodedError)
			if !ok || coded.Offset < 0 {
				t.Fatalf("missing original offset: %#v", err)
			}
			if strings.Contains(testCase.name, "duplicate") && coded.Offset == 0 {
				t.Fatalf("duplicate tunnel offset not advanced: %#v", coded)
			}
		})
	}
}

func TestParseEnvelopeAndRequiredFields(t *testing.T) {
	input := `{"schema":"vpnbench.policy/v1","policies":[{"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":0,"bandwidth":0,"forwarding_cost":0}]}`
	set, err := Parse([]byte(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if set.Policies[0].Priority != 0 {
		t.Fatalf("zero priority not accepted: %+v", set.Policies[0])
	}
	_, err = Parse([]byte(`{"policies":[{"tunnel_id":"a"}]}`))
	if model.CodeOf(err) != model.CodePolicyInvalid {
		t.Fatalf("missing field code = %s", model.CodeOf(err))
	}
}
