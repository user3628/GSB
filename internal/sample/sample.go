package sample

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"secverify/internal/config"
	"secverify/internal/model"
)

const Namespace = "lab-a"

type RoundSample struct {
	Name      string
	Changeset []byte
}

type PathSet struct {
	Baseline    string
	RoundOne    string
	RoundTwo    string
	RoundThree  string
	RoundFour   string
	MissingDep  string
	Cycle       string
	ProbeEscape string
	Contention  string
}

func floatPtr(v float64) *float64 { return &v }

func Baseline() config.Baseline {
	return config.Baseline{
		Namespace: Namespace,
		Items: []model.Item{
			{
				ID:   "acct.base.min_length",
				Kind: model.KindAccount,
				Account: &model.AccountPolicy{
					Name: "minimum_password_length", Value: floatPtr(12), Unit: "characters",
					Min: floatPtr(8), Max: floatPtr(64), Objective: "at_least",
				},
			},
			{
				ID:   "acct.child.min_length",
				Kind: model.KindAccount,
				Account: &model.AccountPolicy{
					Name: "inherited_password_length", Unit: "characters", Min: floatPtr(10),
					Max: floatPtr(64), InheritFrom: "acct.base.min_length", Objective: "at_least",
				},
			},
			{
				ID:   "audit.logon",
				Kind: model.KindAudit,
				Refs: []string{"acct.child.min_length"},
				Audit: &model.AuditPolicy{
					Name: "logon-audit", Category: "logon", Path: "audit/logon", Mode: "0750",
					Enabled: true, RetentionValue: floatPtr(30), RetentionUnit: "days",
				},
			},
			{
				ID:   "audit.retention",
				Kind: model.KindAudit,
				Refs: []string{"audit.logon"},
				Audit: &model.AuditPolicy{
					Name: "retention-audit", Category: "object_access", Path: "audit/retention", Mode: "0700",
					Enabled: true, RetentionValue: floatPtr(12), RetentionUnit: "hours",
				},
			},
			{
				ID:   "svc.telemetry",
				Kind: model.KindService,
				Refs: []string{"audit.logon"},
				Service: &model.ServicePolicy{
					Name: "TelemetrySvc", StartType: "automatic", ImagePath: `C:\Lab\Telemetry.exe`,
				},
			},
			{
				ID:   "svc.defender",
				Kind: model.KindService,
				Refs: []string{"audit.retention"},
				Service: &model.ServicePolicy{
					Name: "DefenderSvc", StartType: "manual", ImagePath: `C:\Lab\Defender.exe`,
				},
			},
			{
				ID:   "reg.hklm.software.lab",
				Kind: model.KindRegistry,
				Refs: []string{"svc.telemetry"},
				Registry: &model.RegistryPolicy{
					Key: `HKLM\SOFTWARE\Lab`, Path: "registry/hklm/software/lab", Mode: "0750", Hive: "HKLM",
					ACL: []model.ACLEntry{{Principal: "SYSTEM", Access: "full_control"}, {Principal: "Users", Access: "read"}},
				},
			},
			{
				ID:   "reg.hklm.defender",
				Kind: model.KindRegistry,
				Refs: []string{"svc.defender"},
				Registry: &model.RegistryPolicy{
					Key: `HKLM\SOFTWARE\Defender`, Path: "registry/hklm/software/defender", Mode: "0700", Hive: "HKLM",
					ACL: []model.ACLEntry{{Principal: "SYSTEM", Access: "full_control"}},
				},
			},
		},
	}
}

func BaselineJSON() []byte {
	b := Baseline()
	data, _ := json.MarshalIndent(b, "", "  ")
	return append(data, '\n')
}

type operationPayload struct {
	Op   string      `json:"op"`
	ID   string      `json:"id"`
	Item *model.Item `json:"item,omitempty"`
}

func encodeOps(ops ...operationPayload) []byte {
	payloads := make([][]byte, 0, len(ops))
	for _, op := range ops {
		payloads = append(payloads, config.MustPayload(op))
	}
	return config.EncodeFrames(payloads)
}

func itemByID(b config.Baseline, id string) model.Item {
	for _, item := range b.Items {
		if item.ID == id {
			return model.CloneItem(item)
		}
	}
	panic("sample item missing: " + id)
}

func RoundOne() RoundSample {
	b := Baseline()
	item := itemByID(b, "acct.base.min_length")
	item.Account.Value = floatPtr(14)
	return RoundSample{Name: "tighten-account", Changeset: encodeOps(operationPayload{Op: "modify", ID: item.ID, Item: &item})}
}

func RoundTwo() RoundSample {
	b := Baseline()
	item := itemByID(b, "audit.logon")
	item.Audit.Mode = "0700"
	item.Audit.RetentionValue = floatPtr(60)
	return RoundSample{Name: "tighten-audit", Changeset: encodeOps(operationPayload{Op: "modify", ID: item.ID, Item: &item})}
}

func RoundThree() RoundSample {
	b := Baseline()
	svc := itemByID(b, "svc.telemetry")
	svc.Service.StartType = "disabled"
	newItem := model.Item{
		ID:   "acct.history.length",
		Kind: model.KindAccount,
		Account: &model.AccountPolicy{
			Name: "password_history_length", Value: floatPtr(24), Unit: "count",
			Min: floatPtr(16), Max: floatPtr(64), Objective: "at_least",
		},
	}
	return RoundSample{Name: "add-and-disable", Changeset: encodeOps(
		operationPayload{Op: "add", ID: newItem.ID, Item: &newItem},
		operationPayload{Op: "modify", ID: svc.ID, Item: &svc},
	)}
}

func RoundFour() RoundSample {
	b := Baseline()
	// The dependent retargets in the same atomic change set, while the closure
	// still records its historical edge to the deleted service.
	reg := itemByID(b, "reg.hklm.defender")
	reg.Refs = nil
	return RoundSample{Name: "delete-with-retarget", Changeset: encodeOps(
		operationPayload{Op: "modify", ID: reg.ID, Item: &reg},
		operationPayload{Op: "delete", ID: "svc.defender"},
	)}
}

func ValidRounds() []RoundSample {
	return []RoundSample{RoundOne(), RoundTwo(), RoundThree(), RoundFour()}
}

func MissingDependencyChange() []byte {
	item := model.Item{
		ID:   "acct.inject.missing_ref",
		Kind: model.KindAccount,
		Refs: []string{"item.does.not.exist"},
		Account: &model.AccountPolicy{
			Name: "injected_missing", Value: floatPtr(20), Unit: "characters", Min: floatPtr(8), Max: floatPtr(64), Objective: "at_least",
		},
	}
	return encodeOps(operationPayload{Op: "add", ID: item.ID, Item: &item})
}

func DependencyCycleChange() []byte {
	b := Baseline()
	base := itemByID(b, "acct.base.min_length")
	base.Refs = []string{"acct.child.min_length"}
	return encodeOps(operationPayload{Op: "modify", ID: base.ID, Item: &base})
}

func ProbeEscapeChange() []byte {
	b := Baseline()
	item := itemByID(b, "audit.logon")
	item.Audit.Path = "../escape-canary"
	return encodeOps(operationPayload{Op: "modify", ID: item.ID, Item: &item})
}

func ContentionChange() []byte {
	b := Baseline()
	item := itemByID(b, "svc.telemetry")
	item.Service.StartType = "manual"
	return encodeOps(operationPayload{Op: "modify", ID: item.ID, Item: &item})
}

func WriteFiles(dir string) (PathSet, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return PathSet{}, err
	}
	set := PathSet{
		Baseline:    filepath.Join(dir, "baseline.json"),
		RoundOne:    filepath.Join(dir, "round-01-tighten-account.changes"),
		RoundTwo:    filepath.Join(dir, "round-02-tighten-audit.changes"),
		RoundThree:  filepath.Join(dir, "round-03-add-and-disable.changes"),
		RoundFour:   filepath.Join(dir, "round-04-delete-with-retarget.changes"),
		MissingDep:  filepath.Join(dir, "inject-missing-dependency.changes"),
		Cycle:       filepath.Join(dir, "inject-dependency-cycle.changes"),
		ProbeEscape: filepath.Join(dir, "inject-probe-escape.changes"),
		Contention:  filepath.Join(dir, "inject-contention.changes"),
	}
	rounds := ValidRounds()
	files := []struct {
		path string
		data []byte
	}{
		{set.Baseline, BaselineJSON()},
		{set.RoundOne, rounds[0].Changeset},
		{set.RoundTwo, rounds[1].Changeset},
		{set.RoundThree, rounds[2].Changeset},
		{set.RoundFour, rounds[3].Changeset},
		{set.MissingDep, MissingDependencyChange()},
		{set.Cycle, DependencyCycleChange()},
		{set.ProbeEscape, ProbeEscapeChange()},
		{set.Contention, ContentionChange()},
	}
	for _, file := range files {
		if err := os.WriteFile(file.path, file.data, 0o600); err != nil {
			return PathSet{}, fmt.Errorf("write %s: %w", filepath.Base(file.path), err)
		}
	}
	return set, nil
}
