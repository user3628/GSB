package main

import (
	"errors"
	"fmt"
	"os"

	"preprocesspipeline/internal/cli"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "verify" {
		if err := cli.Verify(args[1:]); err != nil {
			if errors.Is(err, cli.ErrVerificationFailed) {
				os.Exit(2)
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(args) > 0 && (args[0] == "server" || args[0] == "serve") {
		args = args[1:]
	}
	if err := cli.Server(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
