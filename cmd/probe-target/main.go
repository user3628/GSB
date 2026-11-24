package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"ledgerworkbench/probetarget"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "listen address")
	portFile := flag.String("port-file", "", "optional file receiving the actual base URL")
	printRefused := flag.Bool("print-refused", false, "print a freshly closed loopback endpoint and exit")
	flag.Parse()

	if *printRefused {
		endpoint, err := probetarget.RefusedEndpointString()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(endpoint)
		return
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", *addr, err)
		os.Exit(1)
	}
	baseURL := "http://" + listener.Addr().String()
	if *portFile != "" {
		if err := os.WriteFile(*portFile, []byte(baseURL+"\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write port file: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println(baseURL)
	if err := http.Serve(listener, probetarget.Handler()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
