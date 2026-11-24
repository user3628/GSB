package main

import (
	"flag"
	"fmt"
	"os"

	"replicationlag/internal/samplegen"
)

func main() {
	out := flag.String("out", "samples", "sample output directory")
	flag.Parse()
	if err := samplegen.Generate(*out); err != nil {
		fmt.Fprintln(os.Stderr, "sample generation failed:", err)
		os.Exit(1)
	}
	fmt.Println("generated deterministic samples in", *out)
}
