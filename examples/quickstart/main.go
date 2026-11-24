// Command quickstart demonstrates embedding the truststore package in a host
// PKI service. It uses only local deterministic certificates and a temporary
// SQLite database.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"pkitrust/truststore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "quickstart:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	workdir, err := os.MkdirTemp("", "pkitrust-quickstart-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workdir)

	samples, err := truststore.GenerateSamples()
	if err != nil {
		return err
	}
	content, err := samples.InitialContent()
	if err != nil {
		return err
	}
	store, err := truststore.Open(ctx, truststore.StoreOptions{
		DBPath:      filepath.Join(workdir, "state.db"),
		MaxInflight: 2,
	})
	if err != nil {
		return err
	}
	defer store.Close()

	initial, err := store.Bootstrap(ctx, "pki-demo", content)
	if err != nil {
		return err
	}
	fmt.Printf("bootstrap version=%d chains=%d\n", initial.Version, len(initial.Content.HostChains))

	before, err := store.Verify(ctx, "pki-demo", "chain-team")
	if err != nil {
		return err
	}
	fmt.Printf("before chain-team=%s path=%v\n", before.Code, before.PathNames)

	result, err := store.Apply(ctx, "pki-demo", truststore.Changeset{Operations: []truststore.Operation{{
		Type:   truststore.OperationRevoke,
		CertID: "dept-ca",
		Reason: "intermediate key compromise",
	}}}, truststore.ApplyOptions{RequestID: "quickstart-revoke"})
	if err != nil {
		return err
	}
	fmt.Printf("apply version=%d replacement=%t affected=%v\n", result.SnapshotAfter.Version, result.Replacement, result.Closure.AffectedChains)

	report, err := store.ExportReportJSON(ctx, "pki-demo")
	if err != nil {
		return err
	}
	reportPath := filepath.Join(workdir, "report.json")
	if err := os.WriteFile(reportPath, report, 0o600); err != nil {
		return err
	}
	fmt.Printf("report bytes=%d path=%s\n", len(report), reportPath)
	return nil
}
