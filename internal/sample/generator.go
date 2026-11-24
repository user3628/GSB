package sample

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"keylife/internal/model"
	"keylife/internal/service"
	"keylife/internal/store"
)

type Options struct {
	Dir            string
	NowTick        int64
	Threshold      int
	Scale          int
	MaxConcurrency int
	SubjectLimit   int
}

type Workspace struct {
	Root           string
	DBPath         string
	Store          *store.Store
	Service        *service.Service
	Clock          *model.VirtualClock
	Threshold      int
	NowTick        int64
	KeyIDs         []string
	MaxConcurrency int
	SubjectLimit   int
}

func Build(ctx context.Context, opts Options) (*Workspace, error) {
	if opts.Threshold < 1 {
		opts.Threshold = 4
	}
	if opts.NowTick <= 0 {
		opts.NowTick = 420
	}
	if opts.Scale < 0 {
		opts.Scale = 0
	}
	if opts.MaxConcurrency < 1 {
		opts.MaxConcurrency = 8
	}
	if opts.SubjectLimit < 1 {
		opts.SubjectLimit = 64
	}
	root := opts.Dir
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "keylife-sample-")
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(root, "keylife.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dbPath + suffix)
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	clock := model.NewVirtualClock(0)
	svc := service.New(st, clock, service.Config{MaxConcurrency: opts.MaxConcurrency, DefaultSubjectLimit: opts.SubjectLimit}, service.Hooks{})
	ws := &Workspace{
		Root: root, DBPath: dbPath, Store: st, Service: svc, Clock: clock, Threshold: opts.Threshold,
		NowTick: opts.NowTick, MaxConcurrency: opts.MaxConcurrency, SubjectLimit: opts.SubjectLimit,
	}
	cleanup := func(err error) (*Workspace, error) {
		st.Close()
		return nil, err
	}

	policies := []model.SubjectPolicy{
		{Subject: "alpha-payments", MinStrengthBits: 256, RotationInterval: model.D(300), DefaultLifetime: model.D(1200), MaxPendingRequests: opts.SubjectLimit},
		{Subject: "beta-lab", MinStrengthBits: 128, RotationInterval: model.D(600), DefaultLifetime: model.D(1800), MaxPendingRequests: opts.SubjectLimit},
		{Subject: "gamma-ops", MinStrengthBits: 192, RotationInterval: model.D(450), DefaultLifetime: model.D(900), MaxPendingRequests: opts.SubjectLimit},
	}
	for _, policy := range policies {
		if err := st.UpsertSubject(ctx, policy); err != nil {
			return cleanup(err)
		}
	}

	type keySpec struct {
		id        string
		subject   string
		algorithm string
		created   int64
		activated int64
		lifetime  int64
		activate  bool
	}
	specs := []keySpec{
		{id: "key-alpha-001", subject: "alpha-payments", algorithm: "aes-256-gcm", created: 100, activated: 120, lifetime: 1100, activate: true},
		{id: "key-alpha-002", subject: "alpha-payments", algorithm: "chacha20-poly1305", created: 140, activated: 160, lifetime: 1200, activate: true},
		{id: "key-beta-001", subject: "beta-lab", algorithm: "aes-128-gcm", created: 80, activated: 90, lifetime: 1600, activate: true},
		{id: "key-gamma-001", subject: "gamma-ops", algorithm: "ecdsa-p256-sha256", created: 60, activated: 70, lifetime: 900, activate: true},
		{id: "key-gamma-002", subject: "gamma-ops", algorithm: "ecdsa-p256-sha256", created: 65, lifetime: 900},
	}
	for i := 0; i < opts.Scale; i++ {
		specs = append(specs, keySpec{
			id: fmt.Sprintf("key-scale-%03d", i+1), subject: "alpha-payments", algorithm: "aes-256-gcm",
			created: int64(180 + i*3), activated: int64(185 + i*3), lifetime: 1400, activate: true,
		})
	}
	for _, spec := range specs {
		clock.Set(spec.created)
		result, err := svc.Generate(ctx, service.GenerateRequest{
			KeyID: spec.id, Actor: spec.subject, Subject: spec.subject, Algorithm: spec.algorithm,
			Lifetime: model.D(spec.lifetime),
		})
		if err != nil {
			return cleanup(fmt.Errorf("generate %s: %w", spec.id, err))
		}
		ws.KeyIDs = append(ws.KeyIDs, spec.id)
		if spec.activate {
			clock.Set(spec.activated)
			if _, err := svc.Activate(ctx, service.TransitionRequest{Actor: spec.subject, KeyID: spec.id}); err != nil {
				return cleanup(fmt.Errorf("activate %s: %w (snapshot %d)", spec.id, err, result.SnapshotVersion))
			}
		}
	}

	// Give one key a complete generated -> active -> rotated -> archived ->
	// destroyed trajectory, including a successor produced by rotation.
	clock.Set(260)
	rotation, err := svc.Rotate(ctx, service.TransitionRequest{Actor: "alpha-payments", KeyID: "key-alpha-001"})
	if err != nil {
		return cleanup(fmt.Errorf("rotate key-alpha-001: %w", err))
	}
	if rotation.Successor != nil {
		ws.KeyIDs = append(ws.KeyIDs, rotation.Successor.ID)
		clock.Set(280)
		if _, err := svc.Activate(ctx, service.TransitionRequest{Actor: "alpha-payments", KeyID: rotation.Successor.ID}); err != nil {
			return cleanup(fmt.Errorf("activate rotated successor: %w", err))
		}
	}
	clock.Set(300)
	if _, err := svc.Archive(ctx, service.TransitionRequest{Actor: "alpha-payments", KeyID: "key-alpha-001"}); err != nil {
		return cleanup(fmt.Errorf("archive key-alpha-001: %w", err))
	}
	clock.Set(320)
	if _, err := svc.Destroy(ctx, service.TransitionRequest{Actor: "alpha-payments", KeyID: "key-alpha-001"}); err != nil {
		return cleanup(fmt.Errorf("destroy key-alpha-001: %w", err))
	}

	clock.Set(opts.NowTick)
	return ws, nil
}

func (w *Workspace) Close() error {
	if w == nil || w.Store == nil {
		return nil
	}
	return w.Store.Close()
}

func (w *Workspace) Requests(subject string, includeGenerated bool) ([]model.DecisionRequest, error) {
	keys, err := w.Store.ListKeys(context.Background(), subject)
	if err != nil {
		return nil, err
	}
	var requests []model.DecisionRequest
	for _, key := range keys {
		if key.Phase != model.PhaseActive && !(includeGenerated && key.Phase == model.PhaseGenerated) {
			continue
		}
		requests = append(requests, model.DecisionRequest{
			RequestID: "req-" + key.ID, Subject: key.Subject, KeyID: key.ID,
			Actor: key.Subject, Operation: "use",
		})
	}
	return requests, nil
}
