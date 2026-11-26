package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/gsb/dnscache/internal/authserver"
)

func main() {
	zoneFile := flag.String("zone", "", "zone snapshot JSON file")
	udpAddr := flag.String("udp", "127.0.0.1:15354", "UDP listen address (empty disables)")
	tcpAddr := flag.String("tcp", "127.0.0.1:15354", "TCP listen address (empty disables)")
	delay := flag.Duration("delay", 0, "artificial response delay")
	behavior := flag.String("behavior", "normal", "normal|drop|bad-id|malformed|refused")
	flag.Parse()
	if *zoneFile == "" {
		flag.Usage()
		os.Exit(2)
	}
	logger := log.New(os.Stdout, "authserver ", log.LstdFlags|log.Lmicroseconds)
	snap, err := authserver.LoadSnapshotFile(*zoneFile)
	if err != nil {
		logger.Fatalf("load zone: %v", err)
	}
	srv, err := authserver.New(snap, authserver.Config{
		UDPAddr: *udpAddr, TCPAddr: *tcpAddr, Delay: *delay, Behavior: authserver.Behavior(*behavior),
	}, logger)
	if err != nil {
		logger.Fatalf("create authoritative server: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := srv.Start(ctx); err != nil {
		logger.Fatalf("start authoritative server: %v", err)
	}
	logger.Printf("ready: zone=%s version=%d udp=%s tcp=%s behavior=%s delay=%s", snap.Zone, snap.Version, srv.UDPAddr(), srv.TCPAddr(), *behavior, *delay)
	<-ctx.Done()
	_ = srv.Close()
	if *delay > 0 {
		time.Sleep(10 * time.Millisecond)
	}
}
