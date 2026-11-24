package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"ledgerworkbench/sample"
)

func main() {
	dir := flag.String("dir", "", "output directory (required; tests/demo use a temp directory)")
	seed := flag.Int64("seed", sample.FixedSeed, "fixed random seed")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "--dir is required")
		os.Exit(2)
	}
	manifest, err := sample.Generate(*dir, *seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate samples: %v\n", err)
		os.Exit(1)
	}
	buf, _ := json.MarshalIndent(manifest, "", "  ")
	fmt.Println(string(buf))
}
