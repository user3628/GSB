package engine

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"testing"
)

func TestSignatureAlgorithmCompatibilityMatrix(t *testing.T) {
	rsaPublic := &rsa.PublicKey{}
	ecdsaPublic := &ecdsa.PublicKey{}
	edPublic := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	tests := []struct {
		name      string
		algorithm x509.SignatureAlgorithm
		publicKey any
		want      bool
	}{
		{"rsa-pkcs1", x509.SHA256WithRSA, rsaPublic, true},
		{"rsa-pss", x509.SHA384WithRSAPSS, rsaPublic, true},
		{"rsa-with-ec", x509.ECDSAWithSHA256, rsaPublic, false},
		{"ecdsa", x509.ECDSAWithSHA384, ecdsaPublic, true},
		{"ecdsa-with-rsa", x509.SHA256WithRSA, ecdsaPublic, false},
		{"ed25519", x509.PureEd25519, edPublic, true},
		{"ed25519-with-ecdsa", x509.ECDSAWithSHA256, edPublic, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := signatureAlgorithmCompatible(test.algorithm, test.publicKey); got != test.want {
				t.Fatalf("compatible(%v, %T)=%t, want %t", test.algorithm, test.publicKey, got, test.want)
			}
		})
	}
}
