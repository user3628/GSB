package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"example.com/gsb/dnscache/internal/store"
)

func main() {
	in := flag.String("file", "", "snapshot or delta JSON file")
	out := flag.String("out", "", "output file (default stdout)")
	flag.Parse()
	if *in == "" {
		flag.Usage()
		os.Exit(2)
	}
	b, err := os.ReadFile(*in)
	if err != nil {
		fatal(err)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil {
		fatal(err)
	}
	var result []byte
	if _, ok := probe["changes"]; ok {
		var d store.ZoneDelta
		if err := json.Unmarshal(b, &d); err != nil {
			fatal(err)
		}
		d.Checksum = store.ComputeDeltaChecksum(d)
		result, err = json.MarshalIndent(d, "", "  ")
		if err == nil {
			result = append(result, '\n')
		}
	} else {
		var s store.ZoneSnapshot
		if err := json.Unmarshal(b, &s); err != nil {
			fatal(err)
		}
		s.Checksum = store.ComputeSnapshotChecksum(s)
		result, err = json.MarshalIndent(s, "", "  ")
		if err == nil {
			result = append(result, '\n')
		}
	}
	if err != nil {
		fatal(err)
	}
	if *out == "" {
		_, _ = os.Stdout.Write(result)
		return
	}
	if err := os.WriteFile(*out, result, 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
