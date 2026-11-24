package sample

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"time"

	"mitmpipeline/internal/pki"
)

var FixedTime = time.Date(2025, time.November, 24, 8, 0, 0, 0, time.UTC)

type Bundle struct {
	Root            *x509.Certificate
	Intermediate    *x509.Certificate
	Leaf            *x509.Certificate
	RootPEM         string
	IntermediatePEM string
	LeafPEM         string
	ChainPEM        string
	ShuffledPEM     string
	LeafSPKI        string
	RootHash        string
	RootCanonical   string
	LeafPin         string
	Serial          string
}

type deterministicReader struct {
	seed    []byte
	counter uint64
	buffer  []byte
}

func newReader(seed string) *deterministicReader {
	sum := sha256.Sum256([]byte("reader:" + seed))
	return &deterministicReader{seed: sum[:]}
}

func (r *deterministicReader) Read(p []byte) (int, error) {
	read := 0
	for len(p) > 0 {
		if len(r.buffer) == 0 {
			var counter [8]byte
			binary.BigEndian.PutUint64(counter[:], r.counter)
			h := sha256.Sum256(append(append([]byte{}, r.seed...), counter[:]...))
			r.buffer = h[:]
			r.counter++
		}
		n := copy(p, r.buffer)
		r.buffer = r.buffer[n:]
		p = p[n:]
		read += n
	}
	return read, nil
}

func NewBundle(seed, commonName, dnsName string) (*Bundle, error) {
	rootKey, rootPriv, err := ed25519.GenerateKey(newReader(seed + ":root-key"))
	if err != nil {
		return nil, err
	}
	intermediateKey, intermediatePriv, err := ed25519.GenerateKey(newReader(seed + ":intermediate-key"))
	if err != nil {
		return nil, err
	}
	leafKey, _, err := ed25519.GenerateKey(newReader(seed + ":leaf-key"))
	if err != nil {
		return nil, err
	}

	rootTemplate := caTemplate(seed+":root", "MTLP "+commonName+" Root CA", true)
	rootDER, err := x509.CreateCertificate(newReader(seed+":root-create"), rootTemplate, rootTemplate, rootKey, rootPriv)
	if err != nil {
		return nil, err
	}
	rootCert, err := x509.ParseCertificate(rootDER)
	if err != nil {
		return nil, err
	}

	intermediateTemplate := caTemplate(seed+":intermediate", "MTLP "+commonName+" Intermediate CA", false)
	intermediateDER, err := x509.CreateCertificate(newReader(seed+":intermediate-create"), intermediateTemplate, rootCert, intermediateKey, rootPriv)
	if err != nil {
		return nil, err
	}
	intermediateCert, err := x509.ParseCertificate(intermediateDER)
	if err != nil {
		return nil, err
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: serialFor(seed + ":leaf-serial"),
		Subject:      pkix.Name{CommonName: dnsName, Organization: []string{"MTLP Samples"}},
		DNSNames:     []string{dnsName},
		NotBefore:    FixedTime.Add(-24 * time.Hour),
		NotAfter:     FixedTime.Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(newReader(seed+":leaf-create"), leafTemplate, intermediateCert, leafKey, intermediatePriv)
	if err != nil {
		return nil, err
	}
	leafCert, err := x509.ParseCertificate(leafDER)
	if err != nil {
		return nil, err
	}

	rootPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	intermediatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: intermediateDER})
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})

	return &Bundle{
		Root: rootCert, Intermediate: intermediateCert, Leaf: leafCert,
		RootPEM: string(rootPEM), IntermediatePEM: string(intermediatePEM), LeafPEM: string(leafPEM),
		ChainPEM:      string(leafPEM) + string(intermediatePEM) + string(rootPEM),
		ShuffledPEM:   string(rootPEM) + string(leafPEM) + string(intermediatePEM),
		LeafSPKI:      pki.SPKIHash(leafCert),
		RootHash:      pki.CertHash(rootCert),
		RootCanonical: pki.CanonicalRoot(rootCert),
		LeafPin:       pki.CanonicalPin(leafCert),
		Serial:        leafCert.SerialNumber.Text(16),
	}, nil
}

func caTemplate(seed, commonName string, selfSigned bool) *x509.Certificate {
	return &x509.Certificate{
		SerialNumber:          serialFor(seed),
		Subject:               pkix.Name{CommonName: commonName, Organization: []string{"MTLP Samples"}},
		NotBefore:             FixedTime.Add(-30 * 24 * time.Hour),
		NotAfter:              FixedTime.Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
}

func serialFor(seed string) *big.Int {
	sum := sha256.Sum256([]byte(seed))
	sum[0] &= 0x7f
	return new(big.Int).SetBytes(sum[:20])
}

func MustBundle(seed, cn, sni string) *Bundle {
	bundle, err := NewBundle(seed, cn, sni)
	if err != nil {
		panic(err)
	}
	return bundle
}

var _ = rand.Reader
