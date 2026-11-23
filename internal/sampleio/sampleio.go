package sampleio

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"internalpki/workbench/internal/model"
)

// Sample is a parsed local fixture. A parse problem is represented as a
// finding instead of aborting the batch, so one malformed sample cannot hide
// the rest of the audit.
type Sample struct {
	Path        string
	Dir         string
	Meta        model.Meta
	InputDigest string
	Chain       []*x509.Certificate
	Roots       []*x509.Certificate
	RootPool    *x509.CertPool
	ChainPEM    []byte
	RootsPEM    []byte
	LoadFinding *model.Finding
}

// Discover returns every sample directory containing meta.json, sorted by a
// slash-normalized path relative to root.
func Discover(root string) ([]string, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat samples directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("samples path is not a directory: %s", root)
	}
	var paths []string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "meta.json" {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover samples: %w", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no samples found under %s", root)
	}
	return paths, nil
}

// Load reads and parses chain.pem, roots.pem, and meta.json. File read and
// certificate errors are converted into stable sample findings.
func Load(root, relativePath string) *Sample {
	sample := &Sample{
		Path: relativePath,
		Dir:  filepath.Join(root, filepath.FromSlash(relativePath)),
	}
	chainPEM, chainErr := os.ReadFile(filepath.Join(sample.Dir, "chain.pem"))
	rootsPEM, rootsErr := os.ReadFile(filepath.Join(sample.Dir, "roots.pem"))
	metaBytes, metaErr := os.ReadFile(filepath.Join(sample.Dir, "meta.json"))
	sample.ChainPEM = chainPEM
	sample.RootsPEM = rootsPEM
	sample.InputDigest = digestInputs(chainPEM, rootsPEM, metaBytes, chainErr, rootsErr, metaErr)

	if metaErr != nil {
		sample.setLoadError("meta.json", "readable", stableIOError(metaErr))
		return sample
	}
	if err := json.Unmarshal(metaBytes, &sample.Meta); err != nil {
		sample.setLoadError("meta.json", "valid JSON", err.Error())
		return sample
	}
	if chainErr != nil {
		sample.setLoadError("chain.pem", "readable", stableIOError(chainErr))
		return sample
	}
	if rootsErr != nil {
		sample.setLoadError("roots.pem", "readable", stableIOError(rootsErr))
		return sample
	}

	chain, err := parseCertificatePEM(chainPEM)
	if err != nil {
		sample.setLoadError("chain.pem", "one or more certificates", err.Error())
		return sample
	}
	roots, err := parseCertificatePEMAllowEmpty(rootsPEM)
	if err != nil {
		sample.setLoadError("roots.pem", "zero or more certificates", err.Error())
		return sample
	}
	pool := x509.NewCertPool()
	for _, root := range roots {
		pool.AddCert(root)
	}
	sample.Chain = chain
	sample.Roots = roots
	sample.RootPool = pool
	return sample
}

func (s *Sample) setLoadError(field, expected, actual string) {
	s.LoadFinding = &model.Finding{
		Code:     model.CodeSampleInvalid,
		Field:    field,
		Expected: expected,
		Actual:   actual,
		Detail:   "sample failed to load",
	}
}

func parseCertificatePEM(data []byte) ([]*x509.Certificate, error) {
	certs, err := parseCertificatePEMAllowEmpty(data)
	if err != nil {
		return nil, err
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("no CERTIFICATE PEM blocks")
	}
	return certs, nil
}

func parseCertificatePEMAllowEmpty(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	rest := data
	for {
		block, remaining := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = remaining
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	return certs, nil
}

func digestInputs(chain, roots, meta []byte, chainErr, rootsErr, metaErr error) string {
	h := sha256.New()
	writePart := func(name string, data []byte, err error) {
		h.Write([]byte(name))
		h.Write([]byte{0})
		if err != nil {
			h.Write([]byte("ERROR:"))
			h.Write([]byte(stableIOError(err)))
		} else {
			h.Write(data)
		}
		h.Write([]byte{0})
	}
	writePart("chain.pem", chain, chainErr)
	writePart("roots.pem", roots, rootsErr)
	writePart("meta.json", meta, metaErr)
	return hex.EncodeToString(h.Sum(nil))
}

func stableIOError(err error) string {
	if err == nil {
		return ""
	}
	if os.IsNotExist(err) {
		return "file not found"
	}
	if os.IsPermission(err) {
		return "permission denied"
	}
	return "I/O error"
}

// EffectiveDepth is the scheduler's deterministic chain-depth estimate. Even a
// malformed sample is assigned one unit so it remains accountable.
func (s *Sample) EffectiveDepth() int {
	if len(s.Chain) > 0 {
		return len(s.Chain)
	}
	return 1
}
