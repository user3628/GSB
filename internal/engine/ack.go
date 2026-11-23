package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/store"
)

func (e *Engine) Acknowledge(ctx context.Context, ack core.Ack) (core.Ack, error) {
	if strings.TrimSpace(ack.AckID) == "" || strings.TrimSpace(ack.PushID) == "" {
		return core.Ack{}, core.NewBizError("E_BAD_REQUEST", "ack_id and push_id are required", nil)
	}
	if ack.IngressCount < 1 {
		ack.IngressCount = 1
	}
	if ack.Source == "" {
		ack.Source = "api"
	}
	ack.ReceivedAt = core.Now()
	var stored core.Ack
	_, err := e.store.IntentWrite(ctx, "ACK_INGEST", ack.PushID, ack, func(tx *sql.Tx) (bool, error) {
		push, err := getPushTx(ctx, tx, ack.PushID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_PUSH_NOT_FOUND", "push not found", map[string]any{"push_id": ack.PushID})
		}
		if err != nil {
			return false, err
		}
		if push.ClientID != ack.ClientID || push.Version != ack.Version {
			reason := fmt.Sprintf("confirmation version/client mismatch: push=%s/%d ack=%s/%d", push.ClientID, push.Version, ack.ClientID, ack.Version)
			if err := insertAckEventTx(ctx, tx, ack, core.DispositionBad, reason); err != nil {
				return false, err
			}
			return true, core.NewBizError("E_ACK_VERSION_MISMATCH", "confirmation does not match pushed client/version", map[string]any{
				"push_id": push.PushID, "push_client": push.ClientID, "push_version": push.Version,
				"ack_client": ack.ClientID, "ack_version": ack.Version,
			})
		}
		if ack.ContentDigest != "" && ack.ContentDigest != push.ContentDigest {
			if err := insertAckEventTx(ctx, tx, ack, core.DispositionBad, "confirmation content digest mismatch"); err != nil {
				return false, err
			}
			return true, core.NewBizError("E_ACK_VERSION_MISMATCH", "confirmation content digest differs from pushed content", map[string]any{
				"push_id": push.PushID, "expected": push.ContentDigest, "actual": ack.ContentDigest,
			})
		}

		var newer int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM pushes WHERE client_id=? AND version>? AND status IN ('PENDING','PUSHED','ACKED','APPLIED')`,
			push.ClientID, push.Version).Scan(&newer); err != nil {
			return false, err
		}
		if newer > push.Version {
			if err := insertAckEventTx(ctx, tx, ack, core.DispositionOld, fmt.Sprintf("superseded by version %d", newer)); err != nil {
				return false, err
			}
			return true, core.NewBizError("E_STALE_ACK", "confirmation belongs to a version superseded by a newer push", map[string]any{
				"push_id": push.PushID, "version": push.Version, "newer_version": newer, "disposition": core.DispositionOld,
			})
		}

		existing, existingErr := getAckByPushTx(ctx, tx, push.PushID)
		if existingErr == nil {
			if existing.AckID == ack.AckID && existing.ResultDigest == ack.ResultDigest && existing.ContentDigest == ack.ContentDigest &&
				existing.ClientID == ack.ClientID && existing.Version == ack.Version && reflect.DeepEqual(existing.Result, ack.Result) {
				if err := insertAckEventTx(ctx, tx, ack, core.DispositionDup, "duplicate confirmation reused"); err != nil {
					return false, err
				}
				stored = existing
				return true, nil
			}
			if err := insertAckEventTx(ctx, tx, ack, core.DispositionBad, "different confirmation already exists for push"); err != nil {
				return false, err
			}
			return true, core.NewBizError("E_CONFLICTING_ACK", "a different confirmation already exists for this push", map[string]any{
				"push_id": push.PushID, "existing_ack_id": existing.AckID, "received_ack_id": ack.AckID,
			})
		}
		if !errors.Is(existingErr, sql.ErrNoRows) {
			return false, existingErr
		}
		if push.Status != core.StatusPushed {
			return false, core.NewBizError("E_INVALID_TRANSITION", "push is not in PUSHED state", map[string]any{
				"push_id": push.PushID, "from": push.Status, "to": core.StatusAcked,
			})
		}
		if err := e.transitionTx(ctx, tx, push, core.StatusAcked, "client confirmation accepted"); err != nil {
			return false, err
		}
		if push.AttemptIntentID != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='COMMITTED',finished_at=?,detail_json=? WHERE intent_id=? AND status='PREPARED'`,
				core.Now(), marshal(map[string]any{"push_id": push.PushID, "status": core.StatusAcked}), push.AttemptIntentID); err != nil {
				return false, err
			}
		}
		resultJSON := marshal(ack.Result)
		if ack.Result == nil {
			resultJSON = "{}"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO acks(ack_id,push_id,client_id,version,content_digest,result_digest,result_json,ingress_count,source,received_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			ack.AckID, push.PushID, push.ClientID, push.Version, push.ContentDigest, ack.ResultDigest, resultJSON, ack.IngressCount, ack.Source, ack.ReceivedAt); err != nil {
			return false, err
		}
		if err := insertAckEventTx(ctx, tx, ack, core.DispositionOK, "confirmation accepted"); err != nil {
			return false, err
		}
		ack.ClientID = push.ClientID
		ack.Version = push.Version
		ack.ContentDigest = push.ContentDigest
		stored = ack
		return true, nil
	})
	return stored, err
}

func getAckByPushTx(ctx context.Context, tx *sql.Tx, pushID string) (core.Ack, error) {
	var ack core.Ack
	var resultJSON string
	err := tx.QueryRowContext(ctx, `SELECT ack_id,push_id,client_id,version,content_digest,result_digest,result_json,ingress_count,source,received_at FROM acks WHERE push_id=?`, pushID).
		Scan(&ack.AckID, &ack.PushID, &ack.ClientID, &ack.Version, &ack.ContentDigest, &ack.ResultDigest, &resultJSON, &ack.IngressCount, &ack.Source, &ack.ReceivedAt)
	if err != nil {
		return ack, err
	}
	ack.Result = parseMap(resultJSON)
	return ack, nil
}

func insertAckEventTx(ctx context.Context, tx *sql.Tx, ack core.Ack, disposition, reason string) error {
	resultJSON := marshal(ack.Result)
	if ack.Result == nil {
		resultJSON = "{}"
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO ack_events(event_id,ack_id,push_id,client_id,version,disposition,reason,content_digest,result_digest,result_json,ingress_count,received_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		core.NewID("ackevent"), ack.AckID, ack.PushID, ack.ClientID, ack.Version, disposition, reason,
		ack.ContentDigest, ack.ResultDigest, resultJSON, ack.IngressCount, ack.ReceivedAt)
	return err
}

func parseMap(raw string) map[string]any {
	if strings.TrimSpace(raw) == "" || raw == "{}" {
		return map[string]any{}
	}
	// Reuse the store's tolerant map decoder without exposing SQL details.
	return store.UnmarshalMap(raw)
}
