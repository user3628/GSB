package main

import (
	"reflect"
	"testing"
)

func TestResolveArgsPreservesSubcommandFlags(t *testing.T) {
	cmd, args, err := resolveArgs([]string{"run", "--shard", "short", "--data", "tmp"})
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "run" {
		t.Fatalf("cmd=%q", cmd)
	}
	want := []string{"--shard", "short", "--data", "tmp"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args=%v want %v", args, want)
	}
}

func TestResolveArgsMovesLeadingCommonFlags(t *testing.T) {
	cmd, args, err := resolveArgs([]string{"--data=data-dir", "--reports", "report-dir", "run", "--shard", "extreme"})
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "run" {
		t.Fatalf("cmd=%q", cmd)
	}
	want := []string{"--shard", "extreme", "--data", "data-dir", "--reports", "report-dir"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args=%v want %v", args, want)
	}
}

func TestResolveArgsSubcommandDataWins(t *testing.T) {
	_, args, err := resolveArgs([]string{"--data=outer", "run", "--data", "inner", "--shard", "s"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(args, []string{"--data", "inner", "--shard", "s"}) {
		t.Fatalf("args=%v", args)
	}
}
