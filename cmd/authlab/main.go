package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"example.com/authlab"
	"example.com/authlab/experiment"
	"example.com/authlab/sample"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "authlab:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return errors.New("a subcommand is required")
	}
	if strings.HasPrefix(args[0], "-") && args[0] != "-h" && args[0] != "--help" {
		return runExperiment(args)
	}
	switch args[0] {
	case "experiment", "verify", "inject":
		return runExperiment(args[1:])
	case "serve":
		return runServer(args[1:])
	case "sample", "samples":
		return runSample(args[1:])
	case "export":
		return runExport(args[1:])
	case "help", "-h", "--help":
		printUsage(os.Stdout)
		return nil
	default:
		printUsage(os.Stderr)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `authlab - local deterministic multi-tenant auth/session isolation lab

Usage:
  authlab experiment [flags]   Run five anomaly injections and write deterministic evidence
  authlab serve [flags]        Run local HTTP login/refresh/session endpoints
  authlab sample [flags]       Print deterministic tenants, users, hashes, and keys metadata
  authlab export [flags]       Export a tenant snapshot from a local SQLite database

Experiment flags:
  -injection all|expired|cross-tenant|session-fixation|privilege-escalation|refresh-race
  -out artifacts/run
  -concurrency 8
  -contention-wait 40ms

Serve flags:
  -addr 127.0.0.1:8080
  -db authlab.sqlite`)
}

func runExperiment(args []string) error {
	flags := flag.NewFlagSet("experiment", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	injection := flags.String("injection", "all", "anomaly injection to run")
	injectAlias := flags.String("inject", "", "alias for -injection")
	caseAlias := flags.String("case", "", "alias for -injection")
	outDir := flags.String("out", filepath.Join("artifacts", "run"), "deterministic output directory")
	concurrency := flags.Int("concurrency", 8, "concurrent refresh request count")
	contentionWait := flags.Duration("contention-wait", 40*time.Millisecond, "maximum wait before E_AUTH_CONTENTION")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *injectAlias != "" {
		*injection = *injectAlias
	}
	if *caseAlias != "" {
		*injection = *caseAlias
	}
	if *concurrency < 2 {
		return errors.New("concurrency must be at least 2")
	}
	if *contentionWait <= 0 {
		return errors.New("contention-wait must be positive")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	report, err := experiment.Run(context.Background(), experiment.Options{
		Injection: *injection, WorkDir: filepath.Join(*outDir, ".work"),
		Concurrency: *concurrency, ContentionWait: *contentionWait,
	})
	if err != nil {
		return err
	}
	payload, err := experiment.Marshal(report)
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	reportPath := filepath.Join(*outDir, "report.json")
	if err := writeFile(reportPath, payload); err != nil {
		return err
	}
	stateDir := filepath.Join(*outDir, "states")
	if err := os.RemoveAll(stateDir); err != nil {
		return fmt.Errorf("reset state output: %w", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("create state output: %w", err)
	}
	for _, caseReport := range report.Cases {
		for _, tenant := range caseReport.Tenants {
			statePayload, err := json.MarshalIndent(tenant, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal tenant state: %w", err)
			}
			statePayload = append(statePayload, '\n')
			name := sanitize(caseReport.Name) + "--" + sanitize(tenant.Scope) + "--" + sanitize(tenant.TenantID) + ".json"
			if err := writeFile(filepath.Join(stateDir, name), statePayload); err != nil {
				return err
			}
		}
	}
	sum := sha256.Sum256(payload)
	if err := writeFile(filepath.Join(*outDir, "report.sha256"), []byte(hex.EncodeToString(sum[:])+"  report.json\n")); err != nil {
		return err
	}
	fmt.Printf("cases=%d steps=%d failed_steps=%d passed=%t\n", report.Summary.CaseCount, report.Summary.StepCount, report.Summary.FailedSteps, report.Summary.Passed)
	fmt.Printf("report=%s\n", reportPath)
	if !report.Summary.Passed {
		for _, caseReport := range report.Cases {
			if caseReport.Passed {
				continue
			}
			for _, step := range caseReport.Steps {
				if !step.Passed {
					fmt.Printf("failed case=%s step=%s expected=%s actual=%s\n", caseReport.Name, step.Name, step.ExpectedCode, step.ActualCode)
				}
			}
			for _, item := range caseReport.Assertions {
				if !item.Passed {
					fmt.Printf("failed case=%s assertion=%s expected=%s actual=%s\n", caseReport.Name, item.Name, item.Expected, item.Actual)
				}
			}
		}
		return errors.New("deterministic experiment assertions failed")
	}
	return nil
}

func runSample(args []string) error {
	flags := flag.NewFlagSet("sample", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	out := flags.String("out", "", "optional JSON file path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	payload := struct {
		sample.FixtureSummary
		Credentials []sample.Credential `json:"credentials"`
	}{
		FixtureSummary: sample.Summary(),
		Credentials:    sample.Credentials,
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if *out != "" {
		if err := writeFile(*out, encoded); err != nil {
			return err
		}
		fmt.Printf("sample=%s\n", *out)
		return nil
	}
	_, err = os.Stdout.Write(encoded)
	return err
}

func runExport(args []string) error {
	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	dbPath := flags.String("db", "authlab.sqlite", "SQLite database path")
	tenantID := flags.String("tenant", sample.TenantA, "tenant id")
	out := flags.String("out", "", "optional output path (stdout when empty)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	fixture := sample.Fixture()
	store, err := authlab.OpenSQLiteStore(ctx, *dbPath, time.Now)
	if err != nil {
		return err
	}
	engine, err := authlab.NewEngine(store, fixture, authlab.Options{})
	if err != nil {
		_ = store.Close()
		return err
	}
	defer engine.Close()
	snapshot, err := engine.CurrentSnapshot(ctx, *tenantID)
	if err != nil {
		return err
	}
	payload, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if *out == "" {
		_, err = os.Stdout.Write(payload)
		return err
	}
	if err := writeFile(*out, payload); err != nil {
		return err
	}
	fmt.Printf("state=%s\n", *out)
	return nil
}

func runServer(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	addr := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
	dbPath := flags.String("db", "authlab.sqlite", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	engine, err := authlab.OpenSQLiteEngine(ctx, *dbPath, sample.Fixture(), authlab.Options{})
	if err != nil {
		return err
	}
	defer engine.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/login", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			TenantID        string `json:"tenant_id"`
			Username        string `json:"username"`
			Password        string `json:"password"`
			PriorSessionID  string `json:"prior_session_id"`
			ClientSessionID string `json:"session_id"`
		}
		if err := decodeJSON(r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "E_REQUEST_INVALID")
			return
		}
		result, err := engine.Login(r.Context(), authlab.LoginRequest{
			TenantID: request.TenantID, Username: request.Username, Password: request.Password,
			PriorSessionID: request.PriorSessionID, ClientSessionID: request.ClientSessionID,
		})
		if err != nil {
			authlab.WriteAuthError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	mux.HandleFunc("POST /v1/refresh", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := decodeJSON(r, &request); err != nil {
			writeError(w, http.StatusBadRequest, "E_REQUEST_INVALID")
			return
		}
		var result *authlab.RotationResult
		var err error
		if expectedTenant := strings.TrimSpace(r.Header.Get(authlab.TenantHeader)); expectedTenant != "" {
			result, err = engine.RefreshForTenant(r.Context(), expectedTenant, request.RefreshToken)
		} else {
			result, err = engine.Refresh(r.Context(), request.RefreshToken)
		}
		if err != nil {
			authlab.WriteAuthError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	protected := engine.AuthenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, _ := authlab.ClaimsFromContext(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{
			"tenant_id":   claims.TenantID,
			"user_id":     claims.UserID,
			"session_id":  claims.SessionID,
			"roles":       claims.Roles,
			"permissions": claims.Permissions,
		})
	}))
	mux.Handle("GET /v1/session", protected)
	mux.HandleFunc("POST /v1/logout", func(w http.ResponseWriter, r *http.Request) {
		token, ok := authlab.BearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, string(authlab.CodeTokenInvalid))
			return
		}
		if expectedTenant := strings.TrimSpace(r.Header.Get(authlab.TenantHeader)); expectedTenant != "" {
			if _, err := engine.ValidateAccessForTenant(r.Context(), expectedTenant, token); err != nil {
				authlab.WriteAuthError(w, err)
				return
			}
		}
		snapshot, err := engine.RevokeSession(r.Context(), token)
		if err != nil {
			authlab.WriteAuthError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"revoked": true, "snapshot_version": snapshot.Version})
	})

	server := &http.Server{
		Addr:              *addr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("authlab listening on http://%s db=%s", *addr, *dbPath)
		errCh <- server.ListenAndServe()
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case sig := <-signals:
		log.Printf("received %s, shutting down", sig)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("multiple JSON values")
	}
	return nil
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func writeFile(path string, payload []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".authlab-tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write output: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync output: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return fmt.Errorf("chmod output: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("replace existing output: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace output: %w", err)
	}
	return nil
}

func sanitize(value string) string {
	value = strings.ToLower(value)
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	if builder.Len() == 0 {
		return "unnamed"
	}
	return builder.String()
}
