package main

import (
	"flag"
	"log"
	"net/http"

	"nonceguard/internal/peer"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18091", "loopback proxy peer listen address")
	instance := flag.String("instance", "loopback-peer-1", "peer instance name")
	flag.Parse()
	srv := peer.NewServer(*instance)
	log.Printf("loopback proxy peer listening on http://%s", *addr)
	if err := http.ListenAndServe(*addr, srv.Mux); err != nil {
		log.Fatal(err)
	}
}
