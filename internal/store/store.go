package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/graph"
	"protocolfuzzlab/internal/model"
)

const (
	SampleMarker = "FIXED_SAMPLE_2026-01-01T00:00:00Z"
	schemaSQL    = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS caseset_versions (
  version        INTEGER PRIMARY KEY CHECK (version > 0),
  parent_version INTEGER NOT NULL,
  digest         TEXT NOT NULL,
  content        BLOB NOT NULL,
  summary_json   BLOB NOT NULL,
  created_marker TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS caseset_head (
  id      INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_snapshots (
  version           INTEGER PRIMARY KEY CHECK (version > 0),
  parent_version    INTEGER NOT NULL,
  caseset_version   INTEGER NOT NULL,
  batch_id          TEXT NOT NULL,
  logical_batch     TEXT NOT NULL,
  replay_key        TEXT NOT NULL UNIQUE,
  digest            TEXT NOT NULL,
  content           BLOB NOT NULL,
  created_marker    TEXT NOT NULL,
  whole_replacement INTEGER NOT NULL DEFAULT 0,
  content_changed   INTEGER NOT NULL DEFAULT 0,
  previous_version  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_snapshot_logical_batch ON execution_snapshots(logical_batch, version);

CREATE TABLE IF NOT EXISTS batch_commits (
  replay_key    TEXT PRIMARY KEY,
  batch_id      TEXT NOT NULL,
  caseset_version INTEGER NOT NULL,
  snapshot_version INTEGER NOT NULL,
  digest        TEXT NOT NULL
);
`
)

type Store struct {
	db   *sql.DB
	Path string
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.ToSlash(abs) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	return &Store{db: db, Path: path}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

type CaseSetVersion struct {
	Version       int               `json:"version"`
	ParentVersion int               `json:"parent_version"`
	Digest        string            `json:"digest"`
	CaseSet       model.CaseSet     `json:"case_set"`
	Summary       model.DiffSummary `json:"summary"`
	CreatedMarker string            `json:"created_marker"`
}

type ApplyResult struct {
	Previous CaseSetVersion
	Current  CaseSetVersion
	Summary  model.DiffSummary
}

func (s *Store) Initialize(cs model.CaseSet, description string) (CaseSetVersion, bool, error) {
	cs.Normalize()
	if err := cs.ValidateShape(); err != nil {
		return CaseSetVersion{}, false, codes.New(codes.BadRequest, "initial case set: %v", err)
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CaseSetVersion{}, false, err
	}
	defer tx.Rollback()
	var head int
	err = tx.QueryRowContext(ctx, `SELECT version FROM caseset_head WHERE id=1`).Scan(&head)
	if err == nil {
		v, loadErr := loadCaseSetTx(ctx, tx, head)
		if loadErr != nil {
			return CaseSetVersion{}, false, loadErr
		}
		if err := tx.Commit(); err != nil {
			return CaseSetVersion{}, false, err
		}
		return v, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return CaseSetVersion{}, false, err
	}
	summary := model.DiffSummary{
		BeforeVersion:     0,
		AfterVersion:      1,
		Description:       description,
		Added:             entityRefsForCaseSet(cs),
		AffectedCases:     cs.CaseIDs(),
		AffectedOperators: cs.OperatorIDs(),
		Diagnostics:       graph.Analyze(cs, 1, nil).Diagnostics,
	}
	content, err := cs.JSON()
	if err != nil {
		return CaseSetVersion{}, false, err
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return CaseSetVersion{}, false, err
	}
	digest := model.DigestBytes(content)
	if _, err := tx.ExecContext(ctx, `INSERT INTO caseset_versions(version,parent_version,digest,content,summary_json,created_marker) VALUES(1,0,?,?,?,?)`, digest, content, summaryJSON, SampleMarker); err != nil {
		return CaseSetVersion{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO caseset_head(id,version) VALUES(1,1)`); err != nil {
		return CaseSetVersion{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return CaseSetVersion{}, false, err
	}
	v := CaseSetVersion{Version: 1, ParentVersion: 0, Digest: digest, CaseSet: cs, Summary: summary, CreatedMarker: SampleMarker}
	return v, true, nil
}

func (s *Store) Head() (CaseSetVersion, error) {
	ctx := context.Background()
	var version int
	if err := s.db.QueryRowContext(ctx, `SELECT version FROM caseset_head WHERE id=1`).Scan(&version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CaseSetVersion{}, codes.New(codes.CasesetVersion, "case set has not been initialized")
		}
		return CaseSetVersion{}, err
	}
	return s.LoadCaseSet(version)
}

func (s *Store) LoadCaseSet(version int) (CaseSetVersion, error) {
	return loadCaseSet(context.Background(), s.db, version)
}

func loadCaseSet(ctx context.Context, q queryer, version int) (CaseSetVersion, error) {
	var (
		v       CaseSetVersion
		content []byte
		summary []byte
	)
	err := q.QueryRowContext(ctx, `SELECT version,parent_version,digest,content,summary_json,created_marker FROM caseset_versions WHERE version=?`, version).
		Scan(&v.Version, &v.ParentVersion, &v.Digest, &content, &summary, &v.CreatedMarker)
	if errors.Is(err, sql.ErrNoRows) {
		return CaseSetVersion{}, codes.New(codes.CasesetVersion, "case set version %d does not exist", version)
	}
	if err != nil {
		return CaseSetVersion{}, err
	}
	if err := json.Unmarshal(content, &v.CaseSet); err != nil {
		return CaseSetVersion{}, fmt.Errorf("decode case set %d: %w", version, err)
	}
	if err := json.Unmarshal(summary, &v.Summary); err != nil {
		return CaseSetVersion{}, fmt.Errorf("decode case set summary %d: %w", version, err)
	}
	return v, nil
}

func loadCaseSetTx(ctx context.Context, tx *sql.Tx, version int) (CaseSetVersion, error) {
	return loadCaseSet(ctx, tx, version)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) CaseSetVersions() ([]CaseSetVersion, error) {
	rows, err := s.db.Query(`SELECT version FROM caseset_versions ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]CaseSetVersion, 0, len(versions))
	for _, v := range versions {
		cs, err := s.LoadCaseSet(v)
		if err != nil {
			return nil, err
		}
		out = append(out, cs)
	}
	return out, nil
}

func (s *Store) ApplyChangeSet(changes model.ChangeSet) (ApplyResult, error) {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplyResult{}, err
	}
	defer tx.Rollback()

	var headVersion int
	if err := tx.QueryRowContext(ctx, `SELECT version FROM caseset_head WHERE id=1`).Scan(&headVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ApplyResult{}, codes.New(codes.CasesetVersion, "case set has not been initialized")
		}
		return ApplyResult{}, err
	}
	before, err := loadCaseSetTx(ctx, tx, headVersion)
	if err != nil {
		return ApplyResult{}, err
	}
	after := before.CaseSet
	after.Normalize()
	if err := applyOperations(&after, changes.Operations); err != nil {
		return ApplyResult{}, err
	}
	after.Normalize()
	if err := after.ValidateShape(); err != nil {
		return ApplyResult{}, codes.New(codes.BadRequest, "invalid updated case set: %v", err)
	}
	if err := preflightAddedReferences(after, changes.Operations, headVersion+1); err != nil {
		return ApplyResult{}, err
	}

	analysis := graph.Analyze(after, headVersion+1, changes.Operations)
	after.Invalidations = mergeInvalidations(before.CaseSet.Invalidations, after, changes.Operations, analysis.Diagnostics)
	after.Normalize()
	affectedCases, affectedOperators := graph.Affected(before.CaseSet, after, changes.Operations)
	summary := model.DiffSummary{
		BeforeVersion:     headVersion,
		AfterVersion:      headVersion + 1,
		Description:       changes.Description,
		AffectedCases:     affectedCases,
		AffectedOperators: affectedOperators,
		Diagnostics:       analysis.Diagnostics,
	}
	for _, op := range changes.Operations {
		ref := model.EntityRef{Entity: op.Entity, ID: op.ID}
		switch op.Action {
		case model.ActionAdd:
			summary.Added = append(summary.Added, ref)
		case model.ActionModify:
			summary.Modified = append(summary.Modified, ref)
		case model.ActionDelete:
			summary.Deleted = append(summary.Deleted, ref)
		}
	}
	sortEntityRefs(summary.Added)
	sortEntityRefs(summary.Modified)
	sortEntityRefs(summary.Deleted)

	content, err := after.JSON()
	if err != nil {
		return ApplyResult{}, err
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return ApplyResult{}, err
	}
	digest := model.DigestBytes(content)
	next := headVersion + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO caseset_versions(version,parent_version,digest,content,summary_json,created_marker) VALUES(?,?,?,?,?,?)`, next, headVersion, digest, content, summaryJSON, SampleMarker); err != nil {
		return ApplyResult{}, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE caseset_head SET version=? WHERE id=1 AND version=?`, next, headVersion)
	if err != nil {
		return ApplyResult{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return ApplyResult{}, err
	}
	if n != 1 {
		return ApplyResult{}, codes.New(codes.CasesetVersion, "concurrent case set update detected")
	}
	if err := tx.Commit(); err != nil {
		return ApplyResult{}, err
	}
	current := CaseSetVersion{Version: next, ParentVersion: headVersion, Digest: digest, CaseSet: after, Summary: summary, CreatedMarker: SampleMarker}
	return ApplyResult{Previous: before, Current: current, Summary: summary}, nil
}

func mergeInvalidations(previous []model.Diagnostic, after model.CaseSet, ops []model.Change, fresh []model.Diagnostic) []model.Diagnostic {
	touched := map[string]bool{}
	for _, op := range ops {
		touched[op.Entity+":"+op.ID] = true
	}
	seen := map[string]bool{}
	var out []model.Diagnostic
	add := func(d model.Diagnostic) {
		if !entityExists(after, d.Entity, d.EntityID) {
			return
		}
		key := d.Entity + "\x00" + d.EntityID + "\x00" + d.Code + "\x00" + strings.Join(d.RootChain, ">")
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, d)
	}
	for _, d := range previous {
		if touched[d.Entity+":"+d.EntityID] {
			continue
		}
		add(d)
	}
	for _, d := range fresh {
		add(d)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Entity != b.Entity {
			return a.Entity < b.Entity
		}
		if a.EntityID != b.EntityID {
			return a.EntityID < b.EntityID
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return strings.Join(a.RootChain, ">") < strings.Join(b.RootChain, ">")
	})
	return out
}

func entityExists(cs model.CaseSet, entity, id string) bool {
	switch entity {
	case model.EntitySeed:
		_, ok := cs.Seed(id)
		return ok
	case model.EntityOperator:
		_, ok := cs.Operator(id)
		return ok
	case model.EntityAssertion:
		_, ok := cs.Assertion(id)
		return ok
	case model.EntityCase:
		_, ok := cs.Case(id)
		return ok
	default:
		return false
	}
}

func preflightAddedReferences(after model.CaseSet, ops []model.Change, version int) error {
	for _, op := range ops {
		if op.Action == model.ActionDelete {
			continue
		}
		switch op.Entity {
		case model.EntityOperator:
			o, ok := after.Operator(op.ID)
			if !ok {
				return codes.New(codes.BadRequest, "operator %q missing after update", op.ID)
			}
			if _, ok := after.Seed(o.SeedID); !ok {
				return &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("operator %q references missing seed %q", o.ID, o.SeedID), Chain: []string{"operator:" + o.ID, "seed:" + o.SeedID}}
			}
			for _, dep := range o.DependsOn {
				if _, ok := after.Operator(dep); !ok {
					return &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("operator %q depends on missing operator %q", o.ID, dep), Chain: []string{"operator:" + o.ID, "operator:" + dep}}
				}
			}
		case model.EntityAssertion:
			a, ok := after.Assertion(op.ID)
			if !ok {
				return codes.New(codes.BadRequest, "assertion %q missing after update", op.ID)
			}
			if _, ok := after.Case(a.CaseID); !ok {
				return &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("assertion %q references missing case %q", a.ID, a.CaseID), Chain: []string{"assertion:" + a.ID, "case:" + a.CaseID}}
			}
			if _, ok := after.Seed(a.TemplateID); !ok {
				return &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("assertion %q references missing seed %q", a.ID, a.TemplateID), Chain: []string{"assertion:" + a.ID, "seed:" + a.TemplateID}}
			}
		case model.EntityCase:
			c, ok := after.Case(op.ID)
			if !ok {
				return codes.New(codes.BadRequest, "case %q missing after update", op.ID)
			}
			for _, step := range c.Steps {
				if _, ok := after.Seed(step.SeedID); !ok {
					return &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("case %q references missing seed %q", c.ID, step.SeedID), Chain: []string{"case:" + c.ID, "seed:" + step.SeedID}}
				}
				for _, opID := range step.OperatorIDs {
					if _, ok := after.Operator(opID); !ok {
						return &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("case %q references missing operator %q", c.ID, opID), Chain: []string{"case:" + c.ID, "operator:" + opID}}
					}
				}
			}
		}
	}
	analysis := graph.Analyze(after, version, nil)
	cycleSeen := false
	var chain []string
	for _, d := range analysis.Diagnostics {
		if d.Code == codes.DepCycle {
			cycleSeen = true
			chain = d.RootChain
			break
		}
	}
	if cycleSeen {
		return &codes.Error{Code: codes.DepCycle, Message: "mutation operator dependency cycle", Chain: chain}
	}
	return nil
}

func applyOperations(cs *model.CaseSet, ops []model.Change) error {
	for _, op := range ops {
		if op.ID == "" {
			return codes.New(codes.BadRequest, "change operation id is empty")
		}
		switch op.Entity {
		case model.EntitySeed:
			var value model.Seed
			idx := seedIndex(*cs, op.ID)
			switch op.Action {
			case model.ActionAdd:
				if idx >= 0 {
					return codes.New(codes.BadRequest, "seed %q already exists", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Seeds = append(cs.Seeds, value)
			case model.ActionModify:
				if idx < 0 {
					return codes.New(codes.BadRequest, "seed %q does not exist", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Seeds[idx] = value
			case model.ActionDelete:
				if idx < 0 {
					return codes.New(codes.BadRequest, "seed %q does not exist", op.ID)
				}
				cs.Seeds = append(cs.Seeds[:idx], cs.Seeds[idx+1:]...)
			default:
				return codes.New(codes.BadRequest, "unknown action %q", op.Action)
			}
		case model.EntityOperator:
			var value model.MutationOperator
			idx := operatorIndex(*cs, op.ID)
			switch op.Action {
			case model.ActionAdd:
				if idx >= 0 {
					return codes.New(codes.BadRequest, "operator %q already exists", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Operators = append(cs.Operators, value)
			case model.ActionModify:
				if idx < 0 {
					return codes.New(codes.BadRequest, "operator %q does not exist", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Operators[idx] = value
			case model.ActionDelete:
				if idx < 0 {
					return codes.New(codes.BadRequest, "operator %q does not exist", op.ID)
				}
				cs.Operators = append(cs.Operators[:idx], cs.Operators[idx+1:]...)
			default:
				return codes.New(codes.BadRequest, "unknown action %q", op.Action)
			}
		case model.EntityAssertion:
			var value model.PathAssertion
			idx := assertionIndex(*cs, op.ID)
			switch op.Action {
			case model.ActionAdd:
				if idx >= 0 {
					return codes.New(codes.BadRequest, "assertion %q already exists", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Assertions = append(cs.Assertions, value)
			case model.ActionModify:
				if idx < 0 {
					return codes.New(codes.BadRequest, "assertion %q does not exist", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Assertions[idx] = value
			case model.ActionDelete:
				if idx < 0 {
					return codes.New(codes.BadRequest, "assertion %q does not exist", op.ID)
				}
				cs.Assertions = append(cs.Assertions[:idx], cs.Assertions[idx+1:]...)
			default:
				return codes.New(codes.BadRequest, "unknown action %q", op.Action)
			}
		case model.EntityCase:
			var value model.TestCase
			idx := caseIndex(*cs, op.ID)
			switch op.Action {
			case model.ActionAdd:
				if idx >= 0 {
					return codes.New(codes.BadRequest, "case %q already exists", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Cases = append(cs.Cases, value)
			case model.ActionModify:
				if idx < 0 {
					return codes.New(codes.BadRequest, "case %q does not exist", op.ID)
				}
				if err := decodeChangeValue(op, &value); err != nil {
					return err
				}
				value.ID = op.ID
				cs.Cases[idx] = value
			case model.ActionDelete:
				if idx < 0 {
					return codes.New(codes.BadRequest, "case %q does not exist", op.ID)
				}
				cs.Cases = append(cs.Cases[:idx], cs.Cases[idx+1:]...)
			default:
				return codes.New(codes.BadRequest, "unknown action %q", op.Action)
			}
		default:
			return codes.New(codes.BadRequest, "unknown entity %q", op.Entity)
		}
	}
	return nil
}

func decodeChangeValue(op model.Change, dst any) error {
	if len(op.Value) == 0 {
		return codes.New(codes.BadRequest, "%s %q is missing value", op.Entity, op.ID)
	}
	if err := json.Unmarshal(op.Value, dst); err != nil {
		return codes.New(codes.BadRequest, "decode %s %q: %v", op.Entity, op.ID, err)
	}
	return nil
}

func seedIndex(cs model.CaseSet, id string) int {
	for i := range cs.Seeds {
		if cs.Seeds[i].ID == id {
			return i
		}
	}
	return -1
}
func operatorIndex(cs model.CaseSet, id string) int {
	for i := range cs.Operators {
		if cs.Operators[i].ID == id {
			return i
		}
	}
	return -1
}
func assertionIndex(cs model.CaseSet, id string) int {
	for i := range cs.Assertions {
		if cs.Assertions[i].ID == id {
			return i
		}
	}
	return -1
}
func caseIndex(cs model.CaseSet, id string) int {
	for i := range cs.Cases {
		if cs.Cases[i].ID == id {
			return i
		}
	}
	return -1
}

func entityRefsForCaseSet(cs model.CaseSet) []model.EntityRef {
	var out []model.EntityRef
	for _, s := range cs.Seeds {
		out = append(out, model.EntityRef{Entity: model.EntitySeed, ID: s.ID})
	}
	for _, op := range cs.Operators {
		out = append(out, model.EntityRef{Entity: model.EntityOperator, ID: op.ID})
	}
	for _, a := range cs.Assertions {
		out = append(out, model.EntityRef{Entity: model.EntityAssertion, ID: a.ID})
	}
	for _, c := range cs.Cases {
		out = append(out, model.EntityRef{Entity: model.EntityCase, ID: c.ID})
	}
	sortEntityRefs(out)
	return out
}

func sortEntityRefs(refs []model.EntityRef) {
	rank := func(entity string) int {
		switch entity {
		case model.EntitySeed:
			return 0
		case model.EntityOperator:
			return 1
		case model.EntityAssertion:
			return 2
		case model.EntityCase:
			return 3
		default:
			return 4
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Entity != refs[j].Entity {
			return rank(refs[i].Entity) < rank(refs[j].Entity)
		}
		return refs[i].ID < refs[j].ID
	})
}

func (s *Store) SummaryText(v CaseSetVersion) string {
	var b strings.Builder
	fmt.Fprintf(&b, "case_set_version=%d digest=%s\n", v.Version, v.Digest)
	fmt.Fprintf(&b, "added=%s modified=%s deleted=%s\n", refsText(v.Summary.Added), refsText(v.Summary.Modified), refsText(v.Summary.Deleted))
	fmt.Fprintf(&b, "affected_cases=%s\n", listText(v.Summary.AffectedCases))
	fmt.Fprintf(&b, "affected_operators=%s\n", listText(v.Summary.AffectedOperators))
	for _, d := range v.Summary.Diagnostics {
		fmt.Fprintf(&b, "diagnostic=%s %s:%s chain=%s\n", d.Code, d.Entity, d.EntityID, strings.Join(d.RootChain, " -> "))
	}
	return b.String()
}

func refsText(refs []model.EntityRef) string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.Entity+":"+ref.ID)
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ",")
}

func listText(list []string) string {
	if len(list) == 0 {
		return "-"
	}
	return strings.Join(list, ",")
}
