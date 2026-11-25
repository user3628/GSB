package recall

import "multirecall/internal/model"

func CanonicalResultBytes(reports []QueryReport) []byte {
	copied := append([]QueryReport(nil), reports...)
	for i := range copied {
		copied[i].InputFingerprint = ""
		copied[i].ConfigVersion = 0
		copied[i].ConfigDigest = ""
		copied[i].Budget = nil
	}
	return model.CanonicalJSON(copied)
}
