package main

import (
	"errors"
	"fmt"
	"os"

	"preprocesspipeline/internal/cli"
)

func main() {
	if err := cli.Verify(nil); err != nil {
		if errors.Is(err, cli.ErrVerificationFailed) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "verify failed:", err)
		os.Exit(1)
	}
}
