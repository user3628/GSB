package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"local/configmiddleware/internal/pipeline"
	"local/configmiddleware/internal/sim"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:18081", "loopback listen address")
	samplesPath := flag.String("samples", "samples/samples.json", "deterministic samples JSON")
	flag.Parse()
	doc, err := pipeline.Load(*samplesPath)
	if err != nil {
		log.Fatalf("load samples: %v", err)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	handler := sim.New(doc)
	server := &http.Server{Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	fmt.Printf("client-sim listening on http://%s sample=%s digest=%s\n", listener.Addr(), doc.SampleID, doc.Digest)
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}
