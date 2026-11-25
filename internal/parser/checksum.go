package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"configaudit/internal/fixedpoint"
	"configaudit/internal/model"
)

func verifyChecksum(s model.Spec, line int) error {
	want := s.Checksum
	clone := s
	clone.Checksum = ""
	b, err := json.Marshal(clone)
	if err != nil {
		return fixedpoint.NewError(fixedpoint.ESpecInvalid, "checksum marshal failed: %v", err).
			WithField("checksum").WithLine(line)
	}
	sum := sha256.Sum256(b)
	got := "sha256:" + hex.EncodeToString(sum[:])
	if got != want {
		return fixedpoint.NewError(fixedpoint.ESpecInvalid, "checksum mismatch: want %s got %s", want, got).
			WithField("checksum").WithLine(line)
	}
	return nil
}
