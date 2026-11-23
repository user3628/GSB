package fixture

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/safepath"
)

const (
	SchemaDescriptor = "ntfsacl/descriptor/v1"
	SchemaPolicy     = "ntfsacl/policy/v1"
	FixtureID        = "ntfsacl-enterprise-migration-v1"
	StateDir         = ".ntfsacl"
	TreeDir          = "tree"
	ACLDir           = "acl"
	PolicyFile       = "policy.json"
	DBFile           = "engine.db"

	SIDIntern  = "S-1-5-21-424242-1001"
	SIDFinance = "S-1-5-21-424242-1002"
	SIDDocs    = "S-1-5-21-424242-1003"
	SIDOrphan  = "S-1-5-21-424242-9999"
)

type Options struct {
	Malformed string // "", "flags", "empty", "mask"
}

type Spec struct {
	RelPath    string
	Kind       string
	Descriptor domain.Descriptor
}

func Descriptor(aces []domain.ACE, intents []domain.Intent, protected bool) domain.Descriptor {
	return domain.Descriptor{Schema: SchemaDescriptor, DACLPresent: true, Protected: protected, ACEs: aces, Intents: intents}
}

func ace(index int, typ, sid string, mask uint32, flags domain.InheritFlags) domain.ACE {
	return domain.ACE{Index: index, Type: typ, SID: sid, AccessMask: mask, Flags: flags}
}

func Specs(opts Options) []Spec {
	base := []Spec{
		{
			RelPath: ".", Kind: "DIRECTORY",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileAllAccess, domain.InheritFlags{OI: true, CI: true}),
				ace(1, domain.ACETypeAllow, SIDFinance, domain.FileReadAccess, domain.InheritFlags{OI: true, CI: true}),
				ace(2, domain.ACETypeAllow, SIDDocs, domain.FileReadAccess, domain.InheritFlags{OI: true, CI: true}),
			}, []domain.Intent{
				{SID: SIDIntern, Access: "full", Expected: domain.DecisionAllow},
			}, false),
		},
		{
			RelPath: "finance", Kind: "DIRECTORY",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDFinance, domain.FileWriteAccess, domain.InheritFlags{OI: true, CI: true}),
			}, []domain.Intent{{SID: SIDFinance, Access: "write", Expected: domain.DecisionAllow}}, false),
		},
		{
			RelPath: "finance/budget.txt", Kind: "FILE",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDFinance, domain.FileReadAccess, domain.InheritFlags{}),
				ace(1, domain.ACETypeDeny, SIDFinance, domain.FileReadAccess, domain.InheritFlags{}),
			}, []domain.Intent{{SID: SIDFinance, Access: "read", Expected: domain.DecisionAllow}}, false),
		},
		{
			RelPath: "docs", Kind: "DIRECTORY",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileWriteAccess, domain.InheritFlags{}),
			}, nil, true),
		},
		{
			RelPath: "docs/plan.txt", Kind: "FILE",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileReadAccess, domain.InheritFlags{}),
			}, []domain.Intent{{SID: SIDDocs, Access: "read", Expected: domain.DecisionAllow, ExpectedSource: ".:2"}}, false),
		},
		{
			RelPath: "docs/archive", Kind: "DIRECTORY",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileReadAccess, domain.InheritFlags{}),
			}, nil, false),
		},
		{
			RelPath: "docs/archive/old.txt", Kind: "FILE",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileReadAccess, domain.InheritFlags{}),
			}, nil, false),
		},
		{
			RelPath: "legacy", Kind: "DIRECTORY",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileExecute, domain.InheritFlags{OI: true, NP: true}),
			}, nil, false),
		},
		{
			RelPath: "legacy/orphan.txt", Kind: "FILE",
			Descriptor: Descriptor([]domain.ACE{
				ace(0, domain.ACETypeAllow, SIDIntern, domain.FileExecute, domain.InheritFlags{OI: true}),
				ace(1, domain.ACETypeAllow, SIDOrphan, domain.FileReadAccess, domain.InheritFlags{}),
			}, []domain.Intent{
				{SID: SIDIntern, Access: "execute", Expected: domain.DecisionAllow},
				{SID: SIDOrphan, Access: "read", Expected: domain.DecisionNotGranted},
			}, false),
		},
	}
	if opts.Malformed == "" {
		return base
	}
	for i := range base {
		if base[i].RelPath != "finance/budget.txt" {
			continue
		}
		switch opts.Malformed {
		case "flags":
			base[i].Descriptor.ACEs[0].Flags = domain.InheritFlags{IO: true}
		case "empty":
			base[i].Descriptor.ACEs = nil
		case "mask":
			base[i].Descriptor.ACEs[0].AccessMask = 0x80000001
		}
	}
	return base
}

// Generate creates a real directory tree and deterministic placeholder ACL
// metadata. Existing generated markers are overwritten; user files outside the
// generated paths are left untouched.
func Generate(workdir string, opts Options) (string, error) {
	root, err := safepath.Root(workdir)
	if err != nil {
		return "", err
	}
	tree, err := safepath.Join(root, TreeDir)
	if err != nil {
		return "", err
	}
	state, err := safepath.Join(root, StateDir)
	if err != nil {
		return "", err
	}
	if err := safepath.EnsureNoSymlink(root, tree); err != nil {
		return "", err
	}
	if err := safepath.EnsureNoSymlink(root, state); err != nil {
		return "", err
	}
	if err := os.MkdirAll(tree, 0o755); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(state, ACLDir), 0o755); err != nil {
		return "", err
	}
	specs := Specs(opts)
	sort.Slice(specs, func(i, j int) bool { return specs[i].RelPath < specs[j].RelPath })
	for _, spec := range specs {
		target, err := safepath.Join(tree, spec.RelPath)
		if err != nil {
			return "", err
		}
		if spec.Kind == "DIRECTORY" {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return "", err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(target, []byte("deterministic fixture: "+spec.RelPath+"\n"), 0o644); err != nil {
				return "", err
			}
		}
		if err := writeDescriptor(state, spec.RelPath, spec.Descriptor); err != nil {
			return "", err
		}
	}
	policy := domain.Policy{Schema: SchemaPolicy, FixtureID: FixtureID, KnownSIDs: []string{SIDIntern, SIDFinance, SIDDocs}}
	payload, err := domain.CanonicalJSON(policy)
	if err != nil {
		return "", err
	}
	if err := writeAtomic(filepath.Join(state, PolicyFile), payload); err != nil {
		return "", err
	}
	return tree, nil
}

func writeDescriptor(state, rel string, d domain.Descriptor) error {
	payload, err := domain.CanonicalJSON(d)
	if err != nil {
		return err
	}
	name := descriptorName(rel)
	target := filepath.Join(state, ACLDir, name)
	return writeAtomic(target, payload)
}

func writeAtomic(target string, payload []byte) error {
	dir := filepath.Dir(target)
	f, err := os.CreateTemp(dir, ".ntfsacl-atomic-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	if err := f.Chmod(0o644); err != nil {
		cleanup()
		return err
	}
	if _, err := f.Write(payload); err != nil {
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func descriptorName(rel string) string {
	if rel == "." || rel == "" {
		return "ROOT.json"
	}
	return base64.RawURLEncoding.EncodeToString([]byte(filepath.ToSlash(rel))) + ".json"
}

type Store struct {
	Root      string
	TreeRoot  string
	StateRoot string
}

func Open(workdir string) (*Store, error) {
	root, err := safepath.Root(workdir)
	if err != nil {
		return nil, err
	}
	tree, _ := safepath.Join(root, TreeDir)
	state, _ := safepath.Join(root, StateDir)
	if _, err := os.Stat(tree); err != nil {
		return nil, fmt.Errorf("fixture tree missing: %w", err)
	}
	if err := safepath.EnsureNoSymlink(root, tree); err != nil {
		return nil, err
	}
	if err := safepath.EnsureNoSymlink(root, state); err != nil {
		return nil, err
	}
	for _, rel := range []string{
		filepath.Join(StateDir, ACLDir),
		filepath.Join(StateDir, PolicyFile),
		filepath.Join(StateDir, DBFile),
	} {
		target, err := safepath.Join(root, rel)
		if err != nil {
			return nil, err
		}
		if err := safepath.EnsureNoSymlink(root, target); err != nil {
			return nil, err
		}
	}
	return &Store{Root: root, TreeRoot: tree, StateRoot: state}, nil
}

func (s *Store) Policy() (domain.Policy, error) {
	var p domain.Policy
	path := filepath.Join(s.StateRoot, PolicyFile)
	if err := safepath.EnsureNoSymlink(s.Root, path); err != nil {
		return p, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err := domain.DecodeJSON(strings.NewReader(string(b)), &p); err != nil {
		return p, err
	}
	if p.Schema != SchemaPolicy || p.FixtureID == "" || len(p.KnownSIDs) == 0 {
		return p, errors.New("invalid fixture policy")
	}
	sort.Strings(p.KnownSIDs)
	return p, nil
}

func (s *Store) LoadDescriptor(rel string) (domain.Descriptor, string, error) {
	var d domain.Descriptor
	rel, err := safepath.CleanRel(rel)
	if err != nil {
		return d, "", err
	}
	path := filepath.Join(s.StateRoot, ACLDir, descriptorName(rel))
	if err := safepath.EnsureNoSymlink(s.Root, path); err != nil {
		return d, "", err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return d, "", err
	}
	if err := domain.DecodeJSON(strings.NewReader(string(b)), &d); err != nil {
		return d, "", err
	}
	return d, domain.HashBytes(b), nil
}

func (s *Store) LoadDescriptorBytes(rel string) ([]byte, error) {
	rel, err := safepath.CleanRel(rel)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(s.StateRoot, ACLDir, descriptorName(rel))
	if err := safepath.EnsureNoSymlink(s.Root, path); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Store) SaveDescriptor(rel string, d domain.Descriptor) error {
	rel, err := safepath.CleanRel(rel)
	if err != nil {
		return err
	}
	// Enforce that the logical node still exists in the real tree.
	target, err := safepath.Join(s.TreeRoot, rel)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(target); err != nil {
		return err
	}
	if err := safepath.EnsureNoSymlink(s.TreeRoot, target); err != nil {
		return err
	}
	return writeDescriptor(s.StateRoot, rel, d)
}

func (s *Store) RestoreDescriptor(rel string, raw []byte) error {
	rel, err := safepath.CleanRel(rel)
	if err != nil {
		return err
	}
	target := filepath.Join(s.StateRoot, ACLDir, descriptorName(rel))
	if err := safepath.EnsureNoSymlink(s.Root, target); err != nil {
		return err
	}
	return writeAtomic(target, raw)
}

func (s *Store) DBPath() string { return filepath.Join(s.StateRoot, DBFile) }
