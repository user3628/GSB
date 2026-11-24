//go:build linux

package fsx

import "testing"

func TestPOSIXACLRoundTrip(t *testing.T) {
	entries := []posixEntry{
		{Tag: aclUserObj, Perm: 0, ID: aclUndefID},
		{Tag: aclUser, Perm: permRead | permWrite | permExecute, ID: 61001},
		{Tag: aclGroupObj, Perm: 0, ID: aclUndefID},
		{Tag: aclMask, Perm: permRead | permWrite | permExecute, ID: aclUndefID},
		{Tag: aclOther, Perm: 0, ID: aclUndefID},
	}
	raw, err := encodePOSIXACL(entries)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePOSIXACL(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !equalPOSIXACL(entries, decoded) {
		t.Fatalf("round trip mismatch: %+v != %+v", entries, decoded)
	}
}

func TestSyntheticIdentityDeterministic(t *testing.T) {
	if subjectUID("u-api") != subjectUID("u-api") || subjectUID("u-api") == subjectUID("svc-indexer") {
		t.Fatal("deterministic synthetic UIDs are not stable and distinct")
	}
	if subjectGID("u-api") != subjectGID("u-api") {
		t.Fatal("deterministic synthetic GID changed")
	}
}
