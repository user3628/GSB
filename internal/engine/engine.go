package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/pipeline"
	"local/configmiddleware/internal/store"
)

type Config struct {
	Concurrency        int
	QueueCapacity      int
	DefaultTimeout     time.Duration
	DefaultRoundBudget time.Duration
}

type Engine struct {
	store   *store.Store
	samples *pipeline.Document
	config  Config
	slots   chan struct{}

	flightMu sync.Mutex
	flights  map[string]*flight
	recovery core.RecoveryReport
}

type flight struct {
	done chan struct{}
	push core.Push
	err  error
}

type PushView struct {
	core.Push
	Reused bool      `json:"reused"`
	Ack    *core.Ack `json:"ack,omitempty"`
}

type TriggerRequest struct {
	Version       int64    `json:"version"`
	ClientIDs     []string `json:"client_ids,omitempty"`
	TimeoutMS     int64    `json:"timeout_ms,omitempty"`
	RoundBudgetMS int64    `json:"round_budget_ms,omitempty"`
	ContentDigest string   `json:"content_digest,omitempty"`
}

type TriggerResult struct {
	Batch  core.Batch       `json:"batch"`
	Pushes []PushView       `json:"pushes"`
	Errors []*core.BizError `json:"errors,omitempty"`
}

func New(st *store.Store, doc *pipeline.Document, cfg Config) *Engine {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 4
	}
	if cfg.QueueCapacity < cfg.Concurrency {
		cfg.QueueCapacity = cfg.Concurrency * 4
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 2 * time.Second
	}
	if cfg.DefaultRoundBudget <= 0 {
		cfg.DefaultRoundBudget = 5 * time.Second
	}
	return &Engine{store: st, samples: doc, config: cfg, slots: make(chan struct{}, cfg.Concurrency), flights: map[string]*flight{}}
}

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) LatestSnapshot(ctx context.Context) (core.Snapshot, error) {
	var snap core.Snapshot
	var raw string
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT version,generated_at,config_json,digest,created_at FROM snapshots ORDER BY version DESC LIMIT 1`).
			Scan(&snap.Version, &snap.GeneratedAt, &raw, &snap.Digest, &snap.CreatedAt)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return snap, core.NewBizError("E_SNAPSHOT_NOT_FOUND", "no committed snapshot", nil)
	}
	if err != nil {
		return snap, err
	}
	if err := json.Unmarshal([]byte(raw), &snap.Config); err != nil {
		return snap, err
	}
	return snap, nil
}

func (e *Engine) GetSnapshot(ctx context.Context, version int64) (core.Snapshot, error) {
	var snap core.Snapshot
	var raw string
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT version,generated_at,config_json,digest,created_at FROM snapshots WHERE version=?`, version).
			Scan(&snap.Version, &snap.GeneratedAt, &raw, &snap.Digest, &snap.CreatedAt)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return snap, core.NewBizError("E_SNAPSHOT_NOT_FOUND", "snapshot not found", map[string]any{"version": version})
	}
	if err != nil {
		return snap, err
	}
	if err := json.Unmarshal([]byte(raw), &snap.Config); err != nil {
		return snap, err
	}
	return snap, nil
}

func (e *Engine) SubmitSnapshot(ctx context.Context, snap core.Snapshot) (core.Snapshot, error) {
	if err := core.ValidateSnapshot(snap); err != nil {
		return core.Snapshot{}, err
	}
	var stored core.Snapshot
	_, err := e.store.IntentWrite(ctx, "SNAPSHOT_SUBMIT", fmt.Sprintf("snapshot:%d", snap.Version), snap, func(tx *sql.Tx) (bool, error) {
		var latest int64
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM snapshots`).Scan(&latest)
		if err != nil {
			return false, err
		}
		if snap.Version <= latest {
			return false, core.NewBizError("E_STALE_SNAPSHOT", "snapshot version must increase monotonically", map[string]any{
				"submitted_version": snap.Version, "committed_version": latest,
			})
		}
		now := core.Now()
		snap.CreatedAt = now
		configJSON, err := json.Marshal(snap.Config)
		if err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,generated_at,config_json,digest,created_at) VALUES(?,?,?,?,?)`,
			snap.Version, snap.GeneratedAt, string(configJSON), snap.Digest, now); err != nil {
			return false, err
		}
		stored = snap
		return true, nil
	})
	if err != nil {
		return core.Snapshot{}, err
	}
	return stored, nil
}

func (e *Engine) RegisterClient(ctx context.Context, client core.Client) (core.Client, error) {
	if err := validateClient(&client); err != nil {
		return core.Client{}, err
	}
	now := core.Now()
	client.CreatedAt = now
	client.UpdatedAt = now
	_, err := e.store.IntentWrite(ctx, "CLIENT_REGISTER", "client:"+client.ClientID, client, func(tx *sql.Tx) (bool, error) {
		existing, err := getClientTx(ctx, tx, client.ClientID)
		if err == nil {
			return false, core.NewBizError("E_CLIENT_EXISTS", "client already registered", map[string]any{"client_id": client.ClientID, "existing": existing})
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		return true, insertClientTx(ctx, tx, client)
	})
	if err != nil {
		return core.Client{}, err
	}
	return client, nil
}

func (e *Engine) UpdateClient(ctx context.Context, client core.Client) (core.Client, error) {
	if err := validateClient(&client); err != nil {
		return core.Client{}, err
	}
	client.UpdatedAt = core.Now()
	_, err := e.store.IntentWrite(ctx, "CLIENT_UPDATE", "client:"+client.ClientID, client, func(tx *sql.Tx) (bool, error) {
		existing, err := getClientTx(ctx, tx, client.ClientID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_CLIENT_NOT_FOUND", "client not found", map[string]any{"client_id": client.ClientID})
		}
		if err != nil {
			return false, err
		}
		client.CreatedAt = existing.CreatedAt
		_, err = tx.ExecContext(ctx, `UPDATE clients SET keys_json=?,endpoint=?,pickup_policy=?,pinned_version=?,updated_at=? WHERE client_id=?`,
			marshal(client.Keys), client.Endpoint, client.PickupPolicy, client.PinnedVersion, client.UpdatedAt, client.ClientID)
		return err == nil, err
	})
	if err != nil {
		return core.Client{}, err
	}
	return client, nil
}

func (e *Engine) ListClients(ctx context.Context) ([]core.Client, error) {
	var result []core.Client
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT client_id,keys_json,endpoint,pickup_policy,pinned_version,created_at,updated_at FROM clients ORDER BY client_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			client, err := scanClient(rows)
			if err != nil {
				return err
			}
			result = append(result, client)
		}
		return rows.Err()
	})
	return result, err
}

func validateClient(client *core.Client) error {
	if strings.TrimSpace(client.ClientID) == "" {
		return core.NewBizError("E_BAD_REQUEST", "client_id is required", nil)
	}
	if err := core.ValidateKeys(client.Keys); err != nil {
		return core.NewBizError("E_BAD_REQUEST", err.Error(), map[string]any{"client_id": client.ClientID})
	}
	u, err := url.Parse(client.Endpoint)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return core.NewBizError("E_BAD_REQUEST", "endpoint must be an http URL", map[string]any{"endpoint": client.Endpoint})
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	isLoopback := strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback())
	if !isLoopback {
		return core.NewBizError("E_ENDPOINT_NOT_ALLOWED", "only loopback client endpoints are allowed", map[string]any{"host": host})
	}
	if client.PickupPolicy == "" {
		client.PickupPolicy = core.PickupLatest
	}
	client.PickupPolicy = strings.ToUpper(client.PickupPolicy)
	if client.PickupPolicy != core.PickupLatest && client.PickupPolicy != core.PickupPinned {
		return core.NewBizError("E_BAD_REQUEST", "pickup_policy must be LATEST or PINNED", map[string]any{"pickup_policy": client.PickupPolicy})
	}
	if client.PickupPolicy == core.PickupPinned && client.PinnedVersion == nil {
		return core.NewBizError("E_BAD_REQUEST", "PINNED policy requires pinned_version", map[string]any{"client_id": client.ClientID})
	}
	sort.Strings(client.Keys)
	return nil
}

func getClientTx(ctx context.Context, tx *sql.Tx, clientID string) (core.Client, error) {
	row := tx.QueryRowContext(ctx, `SELECT client_id,keys_json,endpoint,pickup_policy,pinned_version,created_at,updated_at FROM clients WHERE client_id=?`, clientID)
	return scanClient(row)
}

type scanner interface{ Scan(dest ...any) error }

func scanClient(row scanner) (core.Client, error) {
	var c core.Client
	var keysRaw string
	var pinned sql.NullInt64
	if err := row.Scan(&c.ClientID, &keysRaw, &c.Endpoint, &c.PickupPolicy, &pinned, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return c, err
	}
	if err := json.Unmarshal([]byte(keysRaw), &c.Keys); err != nil {
		return c, err
	}
	if pinned.Valid {
		v := pinned.Int64
		c.PinnedVersion = &v
	}
	return c, nil
}

func insertClientTx(ctx context.Context, tx *sql.Tx, c core.Client) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO clients(client_id,keys_json,endpoint,pickup_policy,pinned_version,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`,
		c.ClientID, marshal(c.Keys), c.Endpoint, c.PickupPolicy, c.PinnedVersion, c.CreatedAt, c.UpdatedAt)
	return err
}

const pushColumns = `push_id,batch_id,client_id,version,status,content_digest,attempt_token,attempt_intent_id,error_code,error_message,error_context_json,created_at,updated_at`

func scanPush(row scanner) (core.Push, error) {
	var p core.Push
	var errContext string
	if err := row.Scan(&p.PushID, &p.BatchID, &p.ClientID, &p.Version, &p.Status, &p.ContentDigest, &p.AttemptToken,
		&p.AttemptIntentID, &p.ErrorCode, &p.ErrorMessage, &errContext, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return p, err
	}
	if strings.TrimSpace(errContext) != "" && errContext != "{}" {
		p.ErrorContext = store.UnmarshalMap(errContext)
	}
	return p, nil
}

func getPushTx(ctx context.Context, tx *sql.Tx, pushID string) (core.Push, error) {
	return scanPush(tx.QueryRowContext(ctx, `SELECT `+pushColumns+` FROM pushes WHERE push_id=?`, pushID))
}

func marshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

func resultID(report core.ResultReport) string {
	decision := "false"
	if report.Decision {
		decision = "true"
	}
	raw := fmt.Sprintf("result-id.v1\n%s\n%s\n%d\n%d\n%s\n%s\n%s\n%s\n%s\n%s",
		report.ClientID, report.PushID, report.PickedVersion, report.Seed, report.SampleID, report.SampleDigest,
		core.FloatString(report.WeightedSum), core.FloatString(report.Normalized), decision, report.ResultDigest)
	sum := sha256.Sum256([]byte(raw))
	return "res-" + hex.EncodeToString(sum[:12])
}

func copyConfig(config map[string]float64, keys []string) (map[string]float64, error) {
	result := make(map[string]float64, len(keys))
	missing := make([]string, 0)
	for _, key := range keys {
		value, ok := config[key]
		if !ok {
			missing = append(missing, key)
			continue
		}
		result[key] = value
	}
	if len(missing) > 0 {
		return nil, core.NewBizError("E_CONFIG_INVALID", "client subscribes to missing snapshot keys", map[string]any{"missing": missing})
	}
	return result, nil
}
