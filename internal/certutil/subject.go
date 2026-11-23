package certutil

import "crypto/x509/pkix"

func subject(commonName string) pkix.Name {
	return pkix.Name{
		CommonName:   commonName,
		Organization: []string{"mitmpin-deterministic-lab"},
		Country:      []string{"CN"},
	}
}
