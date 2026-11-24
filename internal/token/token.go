// Package token defines the service ticket (ST) wire format and its local
// integrity signature.
//
// A ticket is: base64url(JSON(Payload)) + "." + base64url(HMAC-SHA256(key, body)).
// The payload is readable (the signature provides integrity, not secrecy) so
// the validator can resolve the tenant key before verifying the signature.
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

// Payload is the signed service-ticket body.
type Payload struct {
	ID        string `json:"tid"`
	TenantID  string `json:"tenant"`
	SPID      string `json:"sp"`
	UserID    string `json:"user"`
	IssuedAt  int64  `json:"iat"` // unix seconds
	ExpiresAt int64  `json:"exp"` // unix seconds
}

var b64 = base64.RawURLEncoding

// Sign produces a signed ticket string from a payload and a tenant secret.
func Sign(secret []byte, p Payload) (string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	body := b64.EncodeToString(raw)
	return body + "." + b64.EncodeToString(mac(secret, body)), nil
}

// Parse decodes the ticket body without verifying the signature. It is used to
// resolve the tenant key; callers must subsequently call Verify.
func Parse(ticket string) (*Payload, error) {
	body, _, err := split(ticket)
	if err != nil {
		return nil, err
	}
	raw, err := b64.DecodeString(body)
	if err != nil {
		return nil, errors.New("malformed ticket body")
	}
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, errors.New("malformed ticket payload")
	}
	return &p, nil
}

// Verify checks the HMAC and returns the payload only when the signature is valid.
func Verify(secret []byte, ticket string) (*Payload, error) {
	body, sig, err := split(ticket)
	if err != nil {
		return nil, err
	}
	want := mac(secret, body)
	got, err := b64.DecodeString(sig)
	if err != nil {
		return nil, errors.New("malformed ticket signature")
	}
	if !hmac.Equal(got, want) {
		return nil, errors.New("signature mismatch")
	}
	raw, err := b64.DecodeString(body)
	if err != nil {
		return nil, errors.New("malformed ticket body")
	}
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, errors.New("malformed ticket payload")
	}
	return &p, nil
}

// Tamper flips one byte of the signature while keeping the body intact, which
// guarantees an HMAC verification failure for every key.
func Tamper(ticket string) (string, error) {
	body, sig, err := split(ticket)
	if err != nil {
		return "", err
	}
	if len(sig) == 0 {
		return "", errors.New("cannot tamper empty signature")
	}
	b := []byte(sig)
	b[0] = rot(b[0])
	return body + "." + string(b), nil
}

func rot(c byte) byte {
	if c == 'A' {
		return 'B'
	}
	return 'A'
}

func mac(secret []byte, body string) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(body))
	return h.Sum(nil)
}

func split(ticket string) (body, sig string, err error) {
	parts := strings.Split(ticket, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("malformed ticket")
	}
	return parts[0], parts[1], nil
}
