package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/pipeline"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "digest":
		digestCommand(os.Args[2:])
	case "gen-samples":
		genSamplesCommand(os.Args[2:])
	case "compute":
		computeCommand(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  configctl digest <snapshot.json>
  configctl gen-samples [-seed 424242] [-count 64] [-out samples/samples.json]
  configctl compute -snapshot <snapshot.json> -samples <samples.json> [-client c1]`)
	os.Exit(2)
}

func digestCommand(args []string) {
	if len(args) != 1 {
		usage()
	}
	b, err := os.ReadFile(args[0])
	must(err)
	var snap core.Snapshot
	must(json.Unmarshal(b, &snap))
	fmt.Println(core.SnapshotDigest(snap))
}

func genSamplesCommand(args []string) {
	fs := flag.NewFlagSet("gen-samples", flag.ExitOnError)
	seed := fs.Int64("seed", 424242, "fixed random seed")
	count := fs.Int("count", 64, "vector count")
	out := fs.String("out", "samples/samples.json", "output path")
	_ = fs.Parse(args)
	vectors := pipeline.GenerateVectors(*seed, *count)
	doc := pipeline.Document{
		Seed: *seed, SampleID: fmt.Sprintf("tensor-seed-%d-%d", *seed, *count),
		Generator: "configctl weighted LCG v1: state=state*6364136223846793005+1442695040888963407; x/y rounded to 1e-6",
		Vectors:   vectors,
	}
	doc.Digest = pipeline.DocumentDigest(doc.Seed, doc.SampleID, doc.Vectors)
	b, err := json.MarshalIndent(doc, "", "  ")
	must(err)
	b = append(b, '\n')
	must(os.WriteFile(*out, b, 0o644))
	fmt.Printf("wrote %s vectors=%d digest=%s\n", *out, len(vectors), doc.Digest)
}

func computeCommand(args []string) {
	fs := flag.NewFlagSet("compute", flag.ExitOnError)
	snapshotPath := fs.String("snapshot", "", "snapshot JSON")
	samplesPath := fs.String("samples", "samples/samples.json", "samples JSON")
	clientID := fs.String("client", "sample-client", "client ID")
	_ = fs.Parse(args)
	if *snapshotPath == "" {
		usage()
	}
	b, err := os.ReadFile(*snapshotPath)
	must(err)
	var snap core.Snapshot
	must(json.Unmarshal(b, &snap))
	doc, err := pipeline.Load(*samplesPath)
	must(err)
	summary, err := pipeline.Compute(*clientID, snap.Version, snap.Config, doc)
	must(err)
	out, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(out))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
