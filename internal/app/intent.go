package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"elastic-scale/internal/model"
	"elastic-scale/internal/store"
)

type existingIntent struct {
	Status       string
	RequestHash  string
	ResponseJSON sql.NullString
	ErrorCode    sql.NullString
	ErrorJSON    sql.NullString
}

func (a *App) beginIntent(ctx context.Context, commitID, kind string, baseVersion int64, payload any) (*model.CommitResponse, error) {
	if commitID == "" {
		commitID = kind + "-" + canonicalHash(kind, payload)
	}
	hash := canonicalHash(kind, payload)
	requestJSON, _ := json.Marshal(payload)
	var row existingIntent
	err := a.db.QueryRowContext(ctx, `SELECT status, request_hash, response_json, error_code, error_json FROM intents WHERE commit_id=?`, commitID).
		Scan(&row.Status, &row.RequestHash, &row.ResponseJSON, &row.ErrorCode, &row.ErrorJSON)
	if err == nil {
		if row.RequestHash != hash {
			return nil, model.NewError(model.CodeConflictRequest, "commit_id was reused with different request content", map[string]any{
				"commit_id": commitID, "existing_request_hash": row.RequestHash, "incoming_request_hash": hash,
			})
		}
		switch row.Status {
		case "committed":
			var resp model.CommitResponse
			if row.ResponseJSON.Valid && row.ResponseJSON.String != "" {
				if err := json.Unmarshal([]byte(row.ResponseJSON.String), &resp); err != nil {
					return nil, err
				}
			}
			resp.CommitID = commitID
			resp.IdempotentReplay = true
			return &resp, nil
		case "rejected":
			if row.ErrorJSON.Valid {
				var be model.BusinessError
				if err := json.Unmarshal([]byte(row.ErrorJSON.String), &be); err == nil {
					return nil, &be
				}
			}
			return nil, model.NewError(model.CodeInvalidRequest, "previously rejected request", map[string]any{"commit_id": commitID, "code": row.ErrorCode.String})
		case "aborted":
			return nil, model.NewError(model.CodeConflictRequest, "commit intent was aborted by crash recovery; use a new commit_id", map[string]any{
				"commit_id": commitID, "intent_status": "aborted",
			})
		default:
			return nil, model.NewError(model.CodeConflictRequest, "commit intent is already pending in this process", map[string]any{
				"commit_id": commitID, "intent_status": row.Status,
			})
		}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	now := store.NowMS()
	_, err = a.db.ExecContext(ctx, `INSERT INTO intents(commit_id,kind,request_hash,base_version,status,request_json,created_at_ms,updated_at_ms)
		VALUES(?,?,?,?,'pending',?,?,?)`, commitID, kind, hash, baseVersion, string(requestJSON), now, now)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (a *App) failIntent(ctx context.Context, commitID string, businessErr *model.BusinessError) error {
	if businessErr == nil {
		return nil
	}
	errJSON, _ := json.Marshal(businessErr)
	code := businessErr.Code
	_, err := a.db.ExecContext(ctx, `UPDATE intents SET status='rejected', error_code=?, error_json=?, response_json=NULL, updated_at_ms=? WHERE commit_id=?`,
		code, string(errJSON), store.NowMS(), commitID)
	return err
}

func finishIntentTx(ctx context.Context, tx *sql.Tx, commitID string, resp *model.CommitResponse) error {
	var responseJSON any
	if resp != nil {
		b, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		responseJSON = string(b)
	}
	_, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', response_json=?, error_code=NULL, error_json=NULL, updated_at_ms=? WHERE commit_id=?`,
		responseJSON, store.NowMS(), commitID)
	return err
}
