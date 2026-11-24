package token

import "testing"

func TestSignVerifyTamper(t *testing.T) {
	p := Payload{ID: "ST-1", TenantID: "tn", SPID: "sp", UserID: "u", IssuedAt: 1, ExpiresAt: 9}
	raw, err := Sign([]byte("key"), p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify([]byte("key"), raw)
	if err != nil || got.ID != "ST-1" {
		t.Fatalf("verify failed: %v %+v", err, got)
	}
	if _, err := Verify([]byte("wrong-key"), raw); err == nil {
		t.Fatal("wrong key must fail verification")
	}
	bad, err := Tamper(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify([]byte("key"), bad); err == nil {
		t.Fatal("tampered ticket must fail verification")
	}
}
