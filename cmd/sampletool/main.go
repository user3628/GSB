package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"

	"recoveryengine/internal/spec"
)

func main() {
	file := flag.String("file", "samples/full-snapshot.json", "snapshot JSON file")
	write := flag.Bool("write", false, "replace checksum_sha256 in the file")
	flag.Parse()
	data, err := os.ReadFile(*file)
	if err != nil {
		fatal(err)
	}
	raw, business := spec.DecodeSnapshot(data)
	if business != nil {
		fatal(business)
	}
	normalized, business := spec.NormalizeSnapshot(raw, false)
	if business != nil {
		fatal(business)
	}
	checksum := normalized.Snapshot.ChecksumSHA256
	fmt.Println(checksum)
	if *write {
		re := regexp.MustCompile(`"checksum_sha256"\s*:\s*"[^"]*"`)
		updated := re.ReplaceAllString(string(data), `"checksum_sha256": "`+checksum+`"`)
		if err := os.WriteFile(*file, []byte(updated), 0o644); err != nil {
			fatal(err)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
