// fwsample materializes the deterministic multi-format forensic corpus into a
// local directory. It performs no network access and depends on no accounts;
// identical invocations produce byte-identical sample files.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"forensicworkbench/internal/samplegen"
)

func main() {
	dir := flag.String("out", filepath.Join(".", "fwbench-samples"), "output directory for the generated corpus")
	flag.Parse()
	abs, err := filepath.Abs(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fwsample: %v\n", err)
		os.Exit(1)
	}
	bundle, err := samplegen.Generate(abs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fwsample: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("generated %d deterministic source files in %s\n", len(bundle.Files), abs)
	for _, f := range bundle.Files {
		fmt.Println(" ", filepath.Join(abs, filepath.Base(f.Path)), "("+f.Format+")")
	}
}
