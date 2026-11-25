// Command blockinspectd runs the local inspection HTTP API backed by a
// SQLite intent store.
package main

import (
	"flag"
	"log"
	"net/http"

	"blockinspect/internal/api"
	"blockinspect/internal/service"
	"blockinspect/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "listen address")
	db := flag.String("db", "inspect.db", "SQLite database path")
	sampleDir := flag.String("sample-dir", "samples/fixed-seed-demo", "directory for generated samples")
	flag.Parse()

	st, err := store.Open(*db)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()
	// startup recovery: abort unfinished intents from a previous hard kill
	rep, err := st.Recover()
	if err != nil {
		log.Fatalf("recovery: %v", err)
	}
	log.Printf("recovery: %d aborted intents, %d committed checkpoints",
		len(rep.AbortedIntents), len(rep.LastCheckpoints))

	svc := service.New(st)
	srv := api.NewServer(svc, *sampleDir)
	log.Printf("listening on %s (db=%s)", *addr, *db)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
