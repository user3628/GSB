package capver

import "testing"

func decl(id string, caps ...Capability) ProcessDecl {
	return ProcessDecl{ID: id, Command: []string{"@self", "probe"}, Capabilities: caps}
}

func TestEvaluateBaselineOK(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapFileRead, CapFileWrite), []ProbeResult{
		{Token: CapFileRead, Target: "<WORK>/seed.txt", Allowed: true, Expected: true},
		{Token: CapFileWrite, Target: "<WORK>/out.txt", Allowed: true, Expected: true},
	}, false)
	if rec.Code != CodeOK || rec.Terminated {
		t.Fatalf("baseline should be OK, got %+v", rec)
	}
}

func TestEvaluateTimeout(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapFileRead), nil, true)
	if rec.Code != CodeProcessTimeout || !rec.TimedOut || !rec.Terminated {
		t.Fatalf("timeout mapping wrong: %+v", rec)
	}
}

func TestEvaluateCapLeak(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapFileRead), []ProbeResult{
		{Token: CapFileRead, Target: "<WORK>/seed.txt", Allowed: true, Expected: true},
		{Token: CapRegistryWrite, Target: `HKCU\Software\capver-probe`, Scenario: ScenarioCapLeak, Allowed: true, Expected: false},
	}, false)
	if rec.Code != CodeCapLeak || !rec.Terminated {
		t.Fatalf("cap leak mapping wrong: %+v", rec)
	}
}

func TestEvaluateUnauthorizedWriteDenied(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapFileRead, CapFileWrite), []ProbeResult{
		{Token: CapFileWrite, Target: "<OUTSIDE>/escape.txt", Scenario: ScenarioUnauthorizedWrite,
			Allowed: false, Expected: false, DeniedBy: "component", RawError: "denied-by-policy"},
	}, false)
	if rec.Code != CodeCapDenied || !rec.Terminated {
		t.Fatalf("unauthorized write mapping wrong: %+v", rec)
	}
}

func TestEvaluateChildEscapeDenied(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapChildCreate), []ProbeResult{
		{Token: CapChildCreate, Target: "capver-not-whitelisted.exe", Scenario: ScenarioChildEscape,
			Allowed: false, Expected: false, DeniedBy: "component"},
	}, false)
	if rec.Code != CodeCapDenied {
		t.Fatalf("child escape mapping wrong: %+v", rec)
	}
}

func TestEvaluateHandleEscape(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapFileRead), []ProbeResult{
		{Token: CapFileWrite, Target: "inherited-handle:231", Scenario: ScenarioHandleEscape,
			Allowed: false, Expected: false, DeniedBy: "kernel", RawError: "invalid handle"},
	}, false)
	if rec.Code != CodeHandleEscape || !rec.Terminated {
		t.Fatalf("handle escape mapping wrong: %+v", rec)
	}
}

func TestEvaluateDeclaredOpDenied(t *testing.T) {
	rec := EvaluateRecord(decl("p", CapFileRead), []ProbeResult{
		{Token: CapFileRead, Target: "<WORK>/seed.txt", Allowed: false, Expected: true, DeniedBy: "kernel"},
	}, false)
	if rec.Code != CodeCapDenied {
		t.Fatalf("declared-op-denied mapping wrong: %+v", rec)
	}
}

func TestMediationPolicyCheck(t *testing.T) {
	pol := &mediationPolicy{workDir: "/tmp/work", exePath: "/bin/capver", whitelist: []string{"@self"}}
	if !pol.check(CapFileWrite, "/tmp/work/out.txt") {
		t.Fatal("in-scope write should be allowed")
	}
	if pol.check(CapFileWrite, "/tmp/other/escape.txt") {
		t.Fatal("out-of-scope write should be denied")
	}
	if pol.check(CapFileRead, "/tmp/work") {
		// 目录本身视为在范围内
	} else {
		t.Fatal("workdir itself should be in scope")
	}
	if !pol.check(CapChildCreate, "/bin/capver") {
		t.Fatal("whitelisted @self child should be allowed")
	}
	if pol.check(CapChildCreate, "/bin/sh") {
		t.Fatal("non-whitelisted child should be denied")
	}
}
