package demo

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
)

func BaseCaseSet() model.CaseSet {
	seeds := []model.Seed{
		{ID: "seed-auth", Type: protocol.MsgAuth, Payload: []byte("AUTH-TOKEN"), Description: "authentication token"},
		{ID: "seed-close", Type: protocol.MsgClose, Payload: []byte("BYE"), Description: "graceful close"},
		{ID: "seed-data", Type: protocol.MsgData, Payload: []byte("PAYLOAD"), Description: "transfer payload"},
		{ID: "seed-hello", Type: protocol.MsgHandshake, Payload: []byte("HELLO"), Description: "opening handshake"},
		{ID: "seed-ping", Type: protocol.MsgPing, Payload: []byte("PING"), Description: "ready-state keepalive"},
	}
	operators := []model.MutationOperator{
		{ID: "op-auth-xor", Name: "xor authentication token", SeedID: "seed-auth", Kind: "xor", Arg: "5a"},
		{ID: "op-data-chain", Name: "derive chained data", SeedID: "seed-data", Kind: "xor", Arg: "aa", DependsOn: []string{"op-data-dup"}},
		{ID: "op-data-dup", Name: "duplicate payload", SeedID: "seed-data", Kind: "duplicate"},
		{ID: "op-hello-append", Name: "append version marker", SeedID: "seed-hello", Kind: "append", Arg: "-v1"},
		{ID: "op-ping-append", Name: "append probe marker", SeedID: "seed-ping", Kind: "append", Arg: "-probe"},
	}
	cases := []model.TestCase{
		{
			ID: "case-basic", Name: "handshake to close",
			Steps: []model.CaseStep{
				{SeedID: "seed-hello", OperatorIDs: []string{"op-hello-append"}},
				{SeedID: "seed-auth", OperatorIDs: []string{"op-auth-xor"}},
				{SeedID: "seed-data"},
				{SeedID: "seed-close"},
			},
		},
		{
			ID: "case-ping", Name: "ready keepalive then close",
			Steps: []model.CaseStep{
				{SeedID: "seed-hello", OperatorIDs: []string{"op-hello-append"}},
				{SeedID: "seed-auth", OperatorIDs: []string{"op-auth-xor"}},
				{SeedID: "seed-ping", OperatorIDs: []string{"op-ping-append"}},
				{SeedID: "seed-close"},
			},
		},
		{
			ID: "case-transfer", Name: "two transfer frames",
			Steps: []model.CaseStep{
				{SeedID: "seed-hello", OperatorIDs: []string{"op-hello-append"}},
				{SeedID: "seed-auth", OperatorIDs: []string{"op-auth-xor"}},
				{SeedID: "seed-data", OperatorIDs: []string{"op-data-dup"}},
				{SeedID: "seed-data", OperatorIDs: []string{"op-data-chain"}},
				{SeedID: "seed-close"},
			},
		},
	}
	assertions := []model.PathAssertion{
		assert("assert-basic-handshake", "case-basic", "seed-hello", protocol.StateHandshake, protocol.StateAuth),
		assert("assert-basic-auth", "case-basic", "seed-auth", protocol.StateAuth, protocol.StateReady),
		assert("assert-basic-transfer", "case-basic", "seed-data", protocol.StateReady, protocol.StateTransfer),
		assert("assert-basic-close", "case-basic", "seed-close", protocol.StateTransfer, protocol.StateClosed),

		assert("assert-ping-handshake", "case-ping", "seed-hello", protocol.StateHandshake, protocol.StateAuth),
		assert("assert-ping-auth", "case-ping", "seed-auth", protocol.StateAuth, protocol.StateReady),
		assert("assert-ping-ready", "case-ping", "seed-ping", protocol.StateReady, protocol.StateReady),
		assert("assert-ping-close", "case-ping", "seed-close", protocol.StateReady, protocol.StateClosed),

		assert("assert-transfer-handshake", "case-transfer", "seed-hello", protocol.StateHandshake, protocol.StateAuth),
		assert("assert-transfer-auth", "case-transfer", "seed-auth", protocol.StateAuth, protocol.StateReady),
		assert("assert-transfer-enter", "case-transfer", "seed-data", protocol.StateReady, protocol.StateTransfer),
		assert("assert-transfer-repeat", "case-transfer", "seed-data", protocol.StateTransfer, protocol.StateTransfer),
		assert("assert-transfer-close", "case-transfer", "seed-close", protocol.StateTransfer, protocol.StateClosed),
	}
	return model.NewCaseSet(seeds, operators, assertions, cases)
}

func IncrementalChange() model.ChangeSet {
	bulkSeed := model.Seed{ID: "seed-bulk", Type: protocol.MsgData, Payload: []byte("BULK-PAYLOAD"), Description: "bulk transfer template"}
	bulkAppend := model.MutationOperator{ID: "op-bulk-append", Name: "append bulk marker", SeedID: "seed-bulk", Kind: "append", Arg: "-bulk"}
	bulkRepeat := model.MutationOperator{ID: "op-bulk-repeat", Name: "repeat bulk payload", SeedID: "seed-bulk", Kind: "duplicate", DependsOn: []string{"op-bulk-append"}}
	bulkCase := model.TestCase{
		ID: "case-bulk", Name: "bulk transfer",
		Steps: []model.CaseStep{
			{SeedID: "seed-hello", OperatorIDs: []string{"op-hello-append"}},
			{SeedID: "seed-auth", OperatorIDs: []string{"op-auth-xor"}},
			{SeedID: "seed-bulk", OperatorIDs: []string{"op-bulk-append", "op-bulk-repeat"}},
			{SeedID: "seed-close"},
		},
	}
	assertions := []model.PathAssertion{
		assert("assert-bulk-handshake", "case-bulk", "seed-hello", protocol.StateHandshake, protocol.StateAuth),
		assert("assert-bulk-auth", "case-bulk", "seed-auth", protocol.StateAuth, protocol.StateReady),
		assert("assert-bulk-transfer", "case-bulk", "seed-bulk", protocol.StateReady, protocol.StateTransfer),
		assert("assert-bulk-close", "case-bulk", "seed-close", protocol.StateTransfer, protocol.StateClosed),
	}
	return model.ChangeSet{
		Description: "incremental bulk-transfer seed, operator, case, and assertions",
		Operations: []model.Change{
			change(model.EntitySeed, model.ActionAdd, bulkSeed.ID, bulkSeed),
			change(model.EntityOperator, model.ActionAdd, bulkAppend.ID, bulkAppend),
			change(model.EntityOperator, model.ActionAdd, bulkRepeat.ID, bulkRepeat),
			change(model.EntityCase, model.ActionAdd, bulkCase.ID, bulkCase),
			change(model.EntityAssertion, model.ActionAdd, assertions[0].ID, assertions[0]),
			change(model.EntityAssertion, model.ActionAdd, assertions[1].ID, assertions[1]),
			change(model.EntityAssertion, model.ActionAdd, assertions[2].ID, assertions[2]),
			change(model.EntityAssertion, model.ActionAdd, assertions[3].ID, assertions[3]),
		},
	}
}

func BreakChange() model.ChangeSet {
	return model.ChangeSet{
		Description: "delete keepalive seed; its operator, case, and assertion dependents become invalid",
		Operations: []model.Change{
			change(model.EntitySeed, model.ActionDelete, "seed-ping", nil),
		},
	}
}

func MissingSeedChange() model.ChangeSet {
	op := model.MutationOperator{ID: "op-injected-missing", Name: "bad reference", SeedID: "seed-does-not-exist", Kind: "append", Arg: "-bad"}
	return model.ChangeSet{
		Description: "external injection: operator references a missing seed",
		Operations:  []model.Change{change(model.EntityOperator, model.ActionAdd, op.ID, op)},
	}
}

func OperatorCycleChange() model.ChangeSet {
	opA := model.MutationOperator{ID: "op-cycle-a", Name: "cycle a", SeedID: "seed-data", Kind: "append", Arg: "-a", DependsOn: []string{"op-cycle-b"}}
	opB := model.MutationOperator{ID: "op-cycle-b", Name: "cycle b", SeedID: "seed-data", Kind: "append", Arg: "-b", DependsOn: []string{"op-cycle-a"}}
	return model.ChangeSet{
		Description: "external injection: mutation operator dependency cycle",
		Operations: []model.Change{
			change(model.EntityOperator, model.ActionAdd, opA.ID, opA),
			change(model.EntityOperator, model.ActionAdd, opB.ID, opB),
		},
	}
}

func ReplaceAllCasesChange() model.ChangeSet {
	// Remove all three baseline cases and add a disjoint case set. This is used
	// to verify replacement detection according to content identity rather than
	// a version-number-only comparison.
	base := BaseCaseSet()
	var ops []model.Change
	for _, tc := range base.Cases {
		ops = append(ops, change(model.EntityCase, model.ActionDelete, tc.ID, nil))
	}
	newCase := model.TestCase{
		ID: "case-replaced", Name: "replacement path",
		Steps: []model.CaseStep{
			{SeedID: "seed-hello"},
			{SeedID: "seed-auth"},
			{SeedID: "seed-data"},
			{SeedID: "seed-close"},
		},
	}
	ops = append(ops, change(model.EntityCase, model.ActionAdd, newCase.ID, newCase))
	ops = append(ops, change(model.EntityAssertion, model.ActionAdd, "assert-replaced-handshake", model.PathAssertion{
		ID: "assert-replaced-handshake", CaseID: "case-replaced", TemplateID: "seed-hello", From: protocol.StateHandshake, To: protocol.StateAuth,
	}))
	ops = append(ops, change(model.EntityAssertion, model.ActionAdd, "assert-replaced-transfer", model.PathAssertion{
		ID: "assert-replaced-transfer", CaseID: "case-replaced", TemplateID: "seed-data", From: protocol.StateReady, To: protocol.StateTransfer,
	}))
	return model.ChangeSet{Description: "replace all case identities while retaining protocol templates", Operations: ops}
}

func change(entity, action, id string, value any) model.Change {
	op := model.Change{Entity: entity, Action: action, ID: id}
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		op.Value = b
	}
	return op
}

func assert(id, caseID, templateID string, from, to protocol.State) model.PathAssertion {
	return model.PathAssertion{ID: id, CaseID: caseID, TemplateID: templateID, From: from, To: to}
}

// WriteSamples creates all external-parameter sample files from the built-in
// generator. Existing files are overwritten only when their content changes.
func WriteSamples(dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	files := map[string]any{
		"change-01-incremental.json":       IncrementalChange(),
		"change-02-break-dependency.json":  BreakChange(),
		"inject-missing-seed.json":         MissingSeedChange(),
		"inject-operator-cycle.json":       OperatorCycleChange(),
		"change-03-replace-all-cases.json": ReplaceAllCasesChange(),
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := json.MarshalIndent(files[name], "", "  ")
		if err != nil {
			return nil, err
		}
		b = append(b, '\n')
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return nil, err
		}
	}
	return names, nil
}
