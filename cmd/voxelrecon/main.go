package main

import (
	"flag"
	"log"
	"net/http"

	"voxelrecon/internal/pipeline"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8787", "HTTP listen address")
	workDir := flag.String("workdir", "./workspace", "workspace directory for tiles, db and artifacts")
	flag.Parse()

	srv, err := pipeline.NewServer(*workDir)
	if err != nil {
		log.Fatalf("server init: %v", err)
	}
	defer srv.Close()

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	log.Printf("voxel reconstruction service listening on %s (workdir=%s)", *addr, *workDir)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}
