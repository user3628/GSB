package normalize

import (
	"math/big"
	"strings"
	"unicode"

	"mitmpipeline/internal/domain"
)

func ConnectionID(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		if unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}

func SNI(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ".")
	return strings.ToLower(value)
}

func Serial(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(strings.ToLower(value), "0x")
	replacer := strings.NewReplacer(":", "", "-", "", " ", "")
	value = strings.ToUpper(replacer.Replace(value))
	if value == "" {
		return ""
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 16); !ok {
		return value
	}
	return strings.ToUpper(n.Text(16))
}

func CheckIdentityConflict(existingID, existingSNI, existingSerial, id, sni, serial string) error {
	canonicalID := ConnectionID(id)
	if existingID != "" && ConnectionID(existingID) != canonicalID {
		return domain.NewError(domain.CodeIdentityConflict, "connection identity changed after canonicalization").
			WithContext("existing_connection_id", existingID).
			WithContext("requested_connection_id", id)
	}
	canonicalSNI := SNI(sni)
	if existingSNI != "" && canonicalSNI != "" && SNI(existingSNI) != canonicalSNI {
		return domain.NewError(domain.CodeIdentityConflict, "same connection id has conflicting SNI").
			WithContext("connection_id", canonicalID).
			WithContext("existing_sni", existingSNI).
			WithContext("requested_sni", sni)
	}
	canonicalSerial := Serial(serial)
	if existingSerial != "" && canonicalSerial != "" && Serial(existingSerial) != canonicalSerial {
		return domain.NewError(domain.CodeIdentityConflict, "same connection id has conflicting certificate serial").
			WithContext("connection_id", canonicalID).
			WithContext("existing_serial", existingSerial).
			WithContext("requested_serial", serial)
	}
	return nil
}
