package main

import (
	"log"

	"preprocesspipeline/internal/cli"
)

func main() {
	if err := cli.Server(nil); err != nil {
		log.Fatal(err)
	}
}
