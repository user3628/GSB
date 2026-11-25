package app

import (
	"csrouter/internal/domain"
	"csrouter/internal/nlp"
	"csrouter/internal/store"
)

type storeTicketRow = store.TicketRow

func nlpNormalize(ticket domain.Ticket, version int64) (domain.NormalizeArtifact, *domain.BizError) {
	return nlp.NormalizeTicket(ticket, version)
}

func stageConfigFingerprint(policy domain.Policy, stage string) string {
	switch stage {
	case domain.StageNormalize:
		return policy.SectionFingerprint("normalize")
	case domain.StageIntent:
		return domain.FingerprintHex("intent-stage-config/v1",
			policy.SectionFingerprint("vocab_weights"),
			policy.SectionFingerprint("smoothing"),
			policy.SectionFingerprint("hash"),
			policy.SectionFingerprint("budget"),
			policy.VocabTopK,
		)
	case domain.StagePriority:
		return policy.SectionFingerprint("priority")
	case domain.StageVerify:
		return domain.FingerprintHex("verify-stage-config/v1", policy.SectionFingerprint("priority"))
	default:
		return policy.Fingerprint()
	}
}

func storePublishInput(row store.TicketRow, stage string, policy domain.Policy, artifact any, status, errorCode string) store.PublishInput {
	return store.PublishInput{
		TicketID:          row.Ticket.TicketID,
		Stage:             stage,
		InputFingerprint:  row.InputFingerprint,
		ConfigVersion:     policy.EffectiveVersion(),
		ConfigFingerprint: stageConfigFingerprint(policy, stage),
		Artifact:          artifact,
		Status:            status,
		ErrorCode:         errorCode,
	}
}
