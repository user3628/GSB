package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18090", "loopback listen address")
	output := flag.String("out", "", "optional JSONL event output file")
	flag.Parse()
	if !strings.HasPrefix(*addr, "127.0.0.1:") && !strings.HasPrefix(*addr, "localhost:") {
		log.Fatalf("rollback webhook must bind to loopback, got %q", *addr)
	}
	var file *os.File
	if *output != "" {
		var err error
		file, err = os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"endpoint":"rollback-touch"}`))
	})
	mux.HandleFunc("POST /rollback", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		event := map[string]any{
			"received_at_ms": time.Now().UnixMilli(), "remote_addr": r.RemoteAddr,
			"event": json.RawMessage(body),
		}
		raw, _ := json.Marshal(event)
		log.Printf("rollback touch: %s", string(raw))
		if file != nil {
			if _, err := file.Write(append(raw, '\n')); err != nil {
				log.Printf("write event: %v", err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	})
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("ROLLBACK_WEBHOOK_LISTENING http://%s/rollback\n", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
