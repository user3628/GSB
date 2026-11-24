package resolver_test

import (
	"context"
	"path/filepath"
	"testing"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/inject"
	"pathcourt/internal/reparse"
	"pathcourt/internal/resolver"
	"pathcourt/internal/run"
	"pathcourt/internal/sample"
)

// buildWorkspace materializes a fresh fixture tree for one scenario.
func buildWorkspace(t *testing.T, scenario sample.Scenario, maxJumps int) (*resolver.Resolver, string) {
	t.Helper()
	dir := t.TempDir()
	out, err := run.Execute(context.Background(), run.Options{
		WorkDir: dir,
		Config:  inject.Config{Scenario: scenario, MaxJumps: maxJumps, MaxConcurrent: 2, MaxPathsPerSession: 256},
	})
	if err != nil {
		t.Fatalf("execute %s: %v", scenario, err)
	}
	t.Cleanup(func() { _ = out.Store.Close() })
	r := resolver.New(resolver.Config{Root: filepath.Join(dir, "restricted-root"), MaxJumps: maxJumps}, reparse.NewInspector())
	return r, dir
}

func TestResolverBaselineContainment(t *testing.T) {
	r, _ := buildWorkspace(t, sample.ScenarioBaseline, resolver.DefaultMaxJumps)
	cases := map[string]bizcode.Code{
		`app/bin/tool.exe`:               bizcode.OK,
		`app/../data/records/r1.txt`:     bizcode.OK,
		`links/app-link/bin/tool.exe`:    bizcode.OK,
		`links/data-junction/records/r1.txt`: bizcode.OK,
		`links/rel/r1-link`:              bizcode.OK,
		`./plain.txt`:                    bizcode.OK,
		`data/records/../../plain.txt`:   bizcode.OK,
		`APP/BIN/TOOL.EXE`:               bizcode.OK,
	}
	for input, want := range cases {
		res, err := r.Resolve("t", 0, input)
		if err != nil {
			t.Fatalf("resolve %q: %v", input, err)
		}
		if res.Code != want {
			t.Errorf("resolve %q = %s, want %s", input, res.Code, want)
		}
	}
}

func TestResolverEscapeAndFirstComponent(t *testing.T) {
	r, _ := buildWorkspace(t, sample.ScenarioEscape, resolver.DefaultMaxJumps)
	res, err := r.Resolve("t", 0, `links/escape-link`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != bizcode.E_PATH_ESCAPE {
		t.Fatalf("code = %s, want E_PATH_ESCAPE", res.Code)
	}
	if res.FirstEscape == nil || res.FirstEscape.ViaHop != 1 {
		t.Fatalf("first escape not traced via hop: %+v", res.FirstEscape)
	}
	if len(res.Hops) != 1 {
		t.Fatalf("hops = %d, want 1", len(res.Hops))
	}

	junction, err := r.Resolve("t", 1, `links/escape-junction/secret.txt`)
	if err != nil {
		t.Fatal(err)
	}
	if junction.Code != bizcode.E_PATH_ESCAPE {
		t.Fatalf("junction code = %s", junction.Code)
	}
	if junction.Hops[0].Kind != reparse.KindJunction {
		t.Fatalf("hop kind = %s, want junction", junction.Hops[0].Kind)
	}
}

func TestResolverCycle(t *testing.T) {
	r, _ := buildWorkspace(t, sample.ScenarioCycle, resolver.DefaultMaxJumps)
	res, err := r.Resolve("t", 0, `links/cycle-a`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != bizcode.E_LINK_CYCLE {
		t.Fatalf("code = %s, want E_LINK_CYCLE", res.Code)
	}
	if res.Cycle == nil || res.Cycle.FirstHopSeq != 1 || res.Cycle.RepeatHopSeq != 3 {
		t.Fatalf("cycle info wrong: %+v", res.Cycle)
	}
	if len(res.Hops) < 3 {
		t.Fatalf("hop chain truncated: %d", len(res.Hops))
	}
}

func TestResolverDepthCap(t *testing.T) {
	r, _ := buildWorkspace(t, sample.ScenarioDepth, 4)
	res, err := r.Resolve("t", 0, `deep/d0`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != bizcode.E_LINK_DEPTH {
		t.Fatalf("code = %s, want E_LINK_DEPTH", res.Code)
	}
	if res.DepthCutoff != 4 {
		t.Fatalf("cutoff = %d, want 4", res.DepthCutoff)
	}
	if len(res.Hops) != 5 {
		t.Fatalf("hops = %d, want 5 (truncation point recorded)", len(res.Hops))
	}
}

func TestResolverMissingAndNotDirectory(t *testing.T) {
	r, _ := buildWorkspace(t, sample.ScenarioDangling, resolver.DefaultMaxJumps)
	cases := []struct {
		in   string
		want bizcode.Code
	}{
		{`data/records/missing.txt`, bizcode.E_TARGET_MISSING},
		{`app/bin/tool.exe/extra`, bizcode.E_NOT_DIRECTORY},
		{`links/dangling-inside`, bizcode.E_TARGET_MISSING},
		{`links/dangling-outside`, bizcode.E_ESCAPE_DANGLING},
	}
	for _, tc := range cases {
		res, err := r.Resolve("t", 0, tc.in)
		if err != nil {
			t.Fatalf("resolve %q: %v", tc.in, err)
		}
		if res.Code != tc.want {
			t.Errorf("resolve %q = %s, want %s", tc.in, res.Code, tc.want)
		}
		if tc.want == bizcode.E_ESCAPE_DANGLING && res.FirstEscape == nil {
			t.Errorf("dangling escape %q missing first-escape trace", tc.in)
		}
	}
}
