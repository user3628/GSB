//go:build !unix

package main

import "os"

func killSelfForFaultInjection() {
	os.Exit(137)
}
