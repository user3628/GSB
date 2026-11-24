package sample

import (
	"bytes"
	"encoding/json"
	"fmt"

	"privescworkbench/internal/domain"
)

// Scenario selects a deterministic generator profile.
type Scenario string

const (
	ScenarioBaseline    Scenario = "baseline"
	ScenarioMaskBad     Scenario = "mask_out_of_range"
	ScenarioOverflow    Scenario = "multiply_overflow"
	ScenarioHandleAbuse Scenario = "handle_abuse"
	ScenarioOrphan      Scenario = "orphan"
	ScenarioGap         Scenario = "snapshot_gap"
)

// Generated is one deterministic sample batch plus its rendered wire bytes.
type Generated struct {
	Bundle         *domain.Bundle
	ManifestBytes  []byte
	StreamBytes    []byte
	ExpectCodes    map[int64]domain.BizCode
	ExpectParseErr domain.BizCode
}

// Generate renders a deterministic batch for a scenario. The same seed always
// produces byte-identical manifest and event stream output.
func Generate(seed int64, scenario Scenario) *Generated {
	g := &Generated{
		ExpectCodes: map[int64]domain.BizCode{},
	}
	switch scenario {
	case ScenarioMaskBad:
		g.buildMaskBad()
	case ScenarioOverflow:
		g.buildOverflow()
	case ScenarioHandleAbuse:
		g.buildHandleAbuse()
	case ScenarioOrphan:
		g.buildOrphan()
	case ScenarioGap:
		g.buildBaseline(seed)
	default:
		g.buildBaseline(seed)
	}
	g.render()
	return g
}

func (g *Generated) buildBaseline(seed int64) {
	version := fmt.Sprintf("sample-baseline-%d", seed)
	procs := []domain.ProcessDecl{
		{PID: 1001, ParentPID: 900, ExePath: `C:\Windows\System32\services.exe`, Account: "LocalSystem",
			InheritedFrom: 0, InheritedMask: 0x0000, ServiceBin: `C:\svc\core.exe`, ServiceWrite: 3,
			HandleIDs: []string{"h1"}},
		{PID: 1002, ParentPID: 1001, ExePath: `C:\Program Files\app\worker.exe`, Account: "Service",
			InheritedFrom: 1001, InheritedMask: 0x000F, ServiceBin: `C:\svc\worker.exe`, ServiceWrite: 2,
			HandleIDs: []string{"h2"}},
		{PID: 1003, ParentPID: 1002, ExePath: `C:\Users\ops\tool.exe`, Account: "User",
			InheritedFrom: 1002, InheritedMask: 0x0001, ServiceBin: `C:\svc\tool.exe`, ServiceWrite: 0,
			HandleIDs: []string{"h3"}},
		{PID: 1004, ParentPID: 1003, ExePath: `C:\Users\ops\quiet.exe`, Account: "Anonymous",
			InheritedFrom: 0, InheritedMask: 0, ServiceBin: "", ServiceWrite: 0},
	}
	events := []domain.Event{
		{Seq: 1, SubjectPID: 1001, TargetPID: 1002, TargetObject: "process:1002", Op: "SERVICE_WRITE", Mask: 0x00FF, OffsetNS: 1_000_500},
		{Seq: 2, SubjectPID: 1002, TargetPID: 1001, TargetObject: "process:1001", Op: "PROCESS_INJECT", Mask: 0x0007, OffsetNS: 1_000_999},
		{Seq: 3, SubjectPID: 1003, TargetPID: 1002, TargetObject: "process:1002", Op: "HANDLE_DUP", Mask: 0x0001, OffsetNS: 12_000_000},
		{Seq: 4, SubjectPID: 1004, TargetPID: 1003, TargetObject: "file:cfg", Op: "READ", Mask: 0x0000, OffsetNS: 20_000_000},
	}
	g.Bundle = &domain.Bundle{Version: version, Processes: procs, Events: events}
	// expectations are asserted in tests against actual rendered scores
}

func (g *Generated) buildMaskBad() {
	g.buildBaseline(1)
	g.Bundle.Events[0].Mask = 0x10000 // illegal: exceeds 16-bit range
	g.ExpectParseErr = domain.CodeSampleParse
}

func (g *Generated) buildOverflow() {
	version := "sample-overflow"
	procs := []domain.ProcessDecl{
		{PID: 2001, ParentPID: 900, ExePath: `C:\svc\huge.exe`, Account: "LocalSystem",
			InheritedMask: 0xFFFF, ServiceBin: `C:\svc\huge.exe`, ServiceWrite: 3},
	}
	events := []domain.Event{
		// max mask at bucket zero: factors are bounded; overflow is driven by
		// the injected multiplicand in tests via config, so keep events valid
		{Seq: 1, SubjectPID: 2001, TargetPID: 2001, TargetObject: "process:self", Op: "SERVICE_WRITE", Mask: 0xFFFF, OffsetNS: 1_000},
	}
	g.Bundle = &domain.Bundle{Version: version, Processes: procs, Events: events}
}

func (g *Generated) buildHandleAbuse() {
	version := "sample-handle-abuse"
	procs := []domain.ProcessDecl{
		{PID: 3001, ParentPID: 900, ExePath: `C:\Windows\System32\lsass.exe`, Account: "LocalSystem",
			InheritedMask: 0, ServiceBin: `C:\svc\auth.exe`, ServiceWrite: 1},
		{PID: 3002, ParentPID: 3001, ExePath: `C:\tmp\injector.exe`, Account: "User",
			InheritedFrom: 3001, InheritedMask: 0x00FF, ServiceBin: `C:\svc\agent.exe`, ServiceWrite: 2},
	}
	events := []domain.Event{
		{Seq: 1, SubjectPID: 3002, TargetPID: 3001, TargetObject: "process:3001", Op: "PROCESS_INJECT", Mask: 0x00FF, OffsetNS: 2_500_777},
		{Seq: 2, SubjectPID: 3002, TargetPID: 3001, TargetObject: "process:3001", Op: "WRITE_DAC", Mask: 0x0F0F, OffsetNS: 3_000_000},
	}
	g.Bundle = &domain.Bundle{Version: version, Processes: procs, Events: events}
	g.ExpectCodes[3002] = domain.CodePrivEscalation
}

func (g *Generated) buildOrphan() {
	version := "sample-orphan"
	procs := []domain.ProcessDecl{
		{PID: 4001, ParentPID: 999, ExePath: `C:\unknown\ghost.exe`, Account: "Admin",
			InheritedFrom: 888, InheritedMask: 0x0003, ServiceBin: `C:\svc\ghost.exe`, ServiceWrite: 1},
		{PID: 4002, ParentPID: 4001, ExePath: `C:\tmp\child.exe`, Account: "User",
			InheritedFrom: 4001, InheritedMask: 0x0001, ServiceBin: "", ServiceWrite: 0},
	}
	events := []domain.Event{
		{Seq: 1, SubjectPID: 4002, TargetPID: 4001, TargetObject: "process:4001", Op: "HANDLE_DUP", Mask: 0x0001, OffsetNS: 500_000},
	}
	g.Bundle = &domain.Bundle{Version: version, Processes: procs, Events: events}
	g.ExpectCodes[4001] = domain.CodeOrphanRef
}

func (g *Generated) render() {
	var mb bytes.Buffer
	enc := json.NewEncoder(&mb)
	enc.SetEscapeHTML(false)
	for _, p := range g.Bundle.Processes {
		_ = enc.Encode(p)
	}
	g.ManifestBytes = mb.Bytes()

	var sb bytes.Buffer
	for _, ev := range g.Bundle.Events {
		raw, _ := json.Marshal(ev)
		fmt.Fprintf(&sb, "LEN %d\n%s\n", len(raw), raw)
	}
	g.StreamBytes = sb.Bytes()
}
