package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func writeJSON(path string, b []byte) error {
	if path == "" || path == "-" {
		_, err := os.Stdout.Write(b)
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	samples := fs.String("samples", "", "samples output directory")
	config := fs.String("config", "", "optional plan.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *samples == "" {
		return fmt.Errorf("--samples is required")
	}
	var plan *Plan
	if *config != "" {
		raw, err := os.ReadFile(*config)
		if err != nil {
			return err
		}
		var p Plan
		if err := json.Unmarshal(raw, &p); err != nil {
			return fmt.Errorf("invalid plan: %v", err)
		}
		plan = &p
	}
	if err := GenerateSamples(*samples, plan); err != nil {
		return err
	}
	fmt.Println("samples generated under", *samples)
	return nil
}

func cmdInspect(args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	repo := fs.String("repo", "", "repo name")
	samples := fs.String("samples", "", "samples directory")
	state := fs.String("state", "", "state directory")
	budget := fs.Int("budget", 0, "max generations per session (0 = unlimited)")
	crash := fs.Bool("crash-before-commit", false, "simulate hard kill before snapshot commit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repo == "" || *samples == "" || *state == "" {
		return fmt.Errorf("--repo, --samples and --state are required")
	}
	res, err := InspectRepo(*samples, *repo, *budget)
	if err != nil {
		return err
	}
	store, err := OpenStore(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	version, err := store.CommitSnapshot(*repo, res, *crash)
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(map[string]any{
		"repo": *repo, "snapshot_version": version, "result": res,
	}, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdBatch(args []string) error {
	fs := flag.NewFlagSet("batch", flag.ContinueOnError)
	samples := fs.String("samples", "", "samples directory")
	state := fs.String("state", "", "state directory")
	concurrency := fs.Int("concurrency", 2, "max concurrent repo sessions")
	budget := fs.Int("budget", 0, "max generations per session (0 = unlimited)")
	report := fs.String("report", "", "optional report output file")
	injectGap := fs.String("inject-snapshot-gap", "", "repo to receive an injected snapshot version gap")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *samples == "" || *state == "" {
		return fmt.Errorf("--samples and --state are required")
	}
	br, store, err := RunBatch(*samples, *state, *concurrency, *budget)
	if err != nil {
		return err
	}
	defer store.Close()
	if *injectGap != "" {
		gapVersion, err := store.InjectSnapshotGap(*injectGap)
		if err != nil {
			return err
		}
		fmt.Printf("injected snapshot gap for %s at version %d\n", *injectGap, gapVersion)
	}
	out, _ := json.MarshalIndent(br, "", "  ")
	fmt.Println(string(out))
	if *report != "" {
		rep, err := BuildReport(store)
		if err != nil {
			return err
		}
		b, err := MarshalReport(rep)
		if err != nil {
			return err
		}
		if err := writeJSON(*report, b); err != nil {
			return err
		}
	}
	return nil
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	repo := fs.String("repo", "", "repo name")
	state := fs.String("state", "", "state directory")
	version := fs.Int("version", 0, "snapshot version to replay")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repo == "" || *state == "" || *version < 1 {
		return fmt.Errorf("--repo, --state and --version >= 1 are required")
	}
	store, err := OpenStore(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	snaps, code, err := store.LoadSnapshots(*repo)
	if err != nil {
		return err
	}
	for _, sn := range snaps {
		if sn.Version == *version {
			var pretty json.RawMessage = []byte(sn.Payload)
			out, _ := json.MarshalIndent(map[string]any{
				"repo": sn.Repo, "version": sn.Version, "digest": sn.Digest,
				"snapshot_code": code, "timeline": pretty,
			}, "", "  ")
			fmt.Println(string(out))
			return nil
		}
	}
	return fmt.Errorf("no contiguous snapshot version %d for repo %q (snapshot_code=%s)", *version, *repo, code)
}

func cmdRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	repo := fs.String("repo", "", "repo name")
	state := fs.String("state", "", "state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *repo == "" || *state == "" {
		return fmt.Errorf("--repo and --state are required")
	}
	store, err := OpenStore(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	res, version, code, err := store.LatestResult(*repo)
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(map[string]any{
		"repo": *repo, "snapshot_version": version, "snapshot_code": code,
		"restore_generation": res.RestoreGen, "restore_files": res.RestoreFiles,
		"excluded": res.Excluded, "codes": res.Codes,
	}, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	state := fs.String("state", "", "state directory")
	out := fs.String("out", "", "output file (default stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *state == "" {
		return fmt.Errorf("--state is required")
	}
	store, err := OpenStore(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	rep, err := BuildReport(store)
	if err != nil {
		return err
	}
	b, err := MarshalReport(rep)
	if err != nil {
		return err
	}
	return writeJSON(*out, b)
}

func cmdStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	state := fs.String("state", "", "state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *state == "" {
		return fmt.Errorf("--state is required")
	}
	store, err := OpenStore(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	st, err := store.Stats()
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(st, "", "  ")
	fmt.Println(string(out))
	return nil
}
