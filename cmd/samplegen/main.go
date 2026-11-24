package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"example.com/replicabudget/internal/policy"
)

func main() {
	in := flag.String("jsonl", "samples/new.jsonl", "source JSONL path")
	out := flag.String("out", "samples/new.bin", "destination binary path")
	bigEndian := flag.Bool("big-endian", false, "encode big endian")
	flag.Parse()
	data, err := os.ReadFile(*in)
	if err != nil {
		panic(err)
	}
	strategies, _, err := policy.DecodeJSONL(data)
	if err != nil {
		panic(err)
	}
	order := binary.ByteOrder(binary.LittleEndian)
	if *bigEndian {
		order = binary.BigEndian
	}
	encoded, err := policy.EncodeBinary(strategies, order)
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*out, encoded, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("wrote %s (%d bytes, %d records)\n", *out, len(encoded), len(strategies))
}
