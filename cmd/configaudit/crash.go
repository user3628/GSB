package main

import (
	"os"
	"os/exec"
)

func newChildCommand(dir, db string) *exec.Cmd {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return exec.Command(exe, "crash-demo", "-dir", dir, "-db", db)
}
