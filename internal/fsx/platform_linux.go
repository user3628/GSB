//go:build linux

package fsx

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/model"
)

const (
	aclVersion  = 2
	aclUserObj  = 0x01
	aclUser     = 0x02
	aclGroupObj = 0x04
	aclMask     = 0x10
	aclOther    = 0x20

	permExecute = 1
	permWrite   = 2
	permRead    = 4
	aclUndefID  = 0xffffffff
)

type posixEntry struct {
	Tag  uint16
	Perm uint16
	ID   uint32
}

func platformName() string { return "linux-posix-acl" }

func subjectUID(id string) uint32 {
	sum := sha256.Sum256([]byte("hostacl-workbench/uid/" + id))
	return 60000 + binary.LittleEndian.Uint32(sum[:4])%1_000_000
}

func subjectGID(id string) uint32 {
	sum := sha256.Sum256([]byte("hostacl-workbench/gid/" + id))
	return 200000 + binary.LittleEndian.Uint32(sum[:4])%1_000_000
}

func platformVerificationAvailable() error {
	if os.Geteuid() != 0 {
		return apperr.With(apperr.IdentityUnavailable, "Linux kernel identity verification requires root so each named principal can be opened through a deterministic UID", map[string]any{
			"effective_uid": os.Geteuid(), "required_uid": 0,
		})
	}
	return nil
}

func applyPlatformACL(tree string, policy *model.Policy, expected map[string]map[string]model.AccessBits) (map[string]string, error) {
	subjects := make([]string, 0, len(policy.Subjects))
	for _, subject := range policy.Subjects {
		subjects = append(subjects, subject.ID)
	}
	sort.Strings(subjects)
	kind := map[string]string{}
	for _, managed := range policy.Paths {
		kind[managed.Path] = managed.Kind
	}
	digests := make(map[string]string, len(policy.Paths))
	for _, managed := range policy.Paths {
		target := tree + "/" + managed.Path
		entries := make([]posixEntry, 0, len(subjects)+4)
		ownerPerm := uint16(permRead | permWrite)
		if kind[managed.Path] == "directory" {
			ownerPerm |= permExecute
		}
		entries = append(entries, posixEntry{Tag: aclUserObj, Perm: ownerPerm, ID: aclUndefID})
		for _, subjectID := range subjects {
			bits := expected[managed.Path][subjectID]
			perm := uint16(0)
			if bits.Read {
				perm |= permRead
			}
			if bits.Write {
				perm |= permWrite
			}
			if kind[managed.Path] == "directory" {
				perm |= permExecute
			}
			entries = append(entries, posixEntry{Tag: aclUser, Perm: perm, ID: subjectUID(subjectID)})
		}
		entries = append(entries, posixEntry{Tag: aclGroupObj, Perm: 0, ID: aclUndefID})
		maskPerm := uint16(0)
		for _, entry := range entries {
			if entry.Tag == aclUser {
				maskPerm |= entry.Perm
			}
		}
		entries = append(entries, posixEntry{Tag: aclMask, Perm: maskPerm, ID: aclUndefID})
		entries = append(entries, posixEntry{Tag: aclOther, Perm: 0, ID: aclUndefID})
		raw, err := encodePOSIXACL(entries)
		if err != nil {
			return nil, err
		}
		if err := unix.Setxattr(target, "system.posix_acl_access", raw, 0); err != nil {
			return nil, apperr.With(apperr.ApplyMismatch, "failed to write POSIX ACL", map[string]any{
				"path": managed.Path, "error": err.Error(),
			})
		}
		buf := make([]byte, 64*1024)
		n, err := unix.Getxattr(target, "system.posix_acl_access", buf)
		if err != nil {
			return nil, apperr.With(apperr.ApplyMismatch, "failed to read back POSIX ACL", map[string]any{
				"path": managed.Path, "error": err.Error(),
			})
		}
		actual, err := decodePOSIXACL(buf[:n])
		if err != nil {
			return nil, err
		}
		if !equalPOSIXACL(entries, actual) {
			return nil, applyMismatch(managed.Path, map[string]any{
				"expected": entries, "actual": actual,
			})
		}
		sum := sha256.Sum256(buf[:n])
		digests[managed.Path] = "sha256:" + hex.EncodeToString(sum[:])
	}
	return digests, nil
}

func encodePOSIXACL(entries []posixEntry) ([]byte, error) {
	if len(entries) > 65535 {
		return nil, fmt.Errorf("too many POSIX ACL entries")
	}
	buf := make([]byte, 4+len(entries)*8)
	binary.LittleEndian.PutUint32(buf[:4], aclVersion)
	for i, entry := range entries {
		offset := 4 + i*8
		binary.LittleEndian.PutUint16(buf[offset:offset+2], entry.Tag)
		binary.LittleEndian.PutUint16(buf[offset+2:offset+4], entry.Perm)
		binary.LittleEndian.PutUint32(buf[offset+4:offset+8], entry.ID)
	}
	return buf, nil
}

func decodePOSIXACL(raw []byte) ([]posixEntry, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("short POSIX ACL")
	}
	if binary.LittleEndian.Uint32(raw[:4]) != aclVersion {
		return nil, fmt.Errorf("unsupported POSIX ACL version")
	}
	if (len(raw)-4)%8 != 0 {
		return nil, fmt.Errorf("malformed POSIX ACL")
	}
	entries := make([]posixEntry, 0, (len(raw)-4)/8)
	for offset := 4; offset < len(raw); offset += 8 {
		entries = append(entries, posixEntry{
			Tag:  binary.LittleEndian.Uint16(raw[offset : offset+2]),
			Perm: binary.LittleEndian.Uint16(raw[offset+2 : offset+4]),
			ID:   binary.LittleEndian.Uint32(raw[offset+4 : offset+8]),
		})
	}
	return entries, nil
}

func equalPOSIXACL(a, b []posixEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func openAsPlatformIdentity(ctx context.Context, tree, target, kind, subjectID, mode string) (string, bool, string, string, error) {
	if err := platformVerificationAvailable(); err != nil {
		return "error", false, platformName(), "", err
	}
	uid, gid := subjectUID(subjectID), subjectGID(subjectID)
	script := ""
	switch {
	case mode == "read":
		script = `out=$(dd if="$1" of=/dev/null bs=1 count=0 status=none 2>&1); rc=$?; if [ "$rc" -eq 0 ]; then exit 0; fi; printf '%s\n' "$out" >&2; case "$out" in *"Permission denied"*) exit 13;; *) exit 2;; esac`
	case mode == "write" && kind == "file":
		script = `out=$(dd if=/dev/null of="$1" bs=1 count=0 conv=notrunc status=none 2>&1); rc=$?; if [ "$rc" -eq 0 ]; then exit 0; fi; printf '%s\n' "$out" >&2; case "$out" in *"Permission denied"*) exit 13;; *) exit 2;; esac`
	case mode == "write" && kind == "directory":
		script = `probe="$1/.workbench-write-probe-$$"; out=$(dd if=/dev/null of="$probe" bs=1 count=0 status=none 2>&1); rc=$?; if [ "$rc" -eq 0 ]; then rm -f -- "$probe"; exit 0; fi; printf '%s\n' "$out" >&2; case "$out" in *"Permission denied"*) exit 13;; *) exit 2;; esac`
	default:
		return "error", false, platformName(), fmt.Sprintf("uid=%d", uid), fmt.Errorf("unsupported open mode %q for %s", mode, kind)
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", script, "workbench-open", target)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid, Groups: []uint32{}}}
	output, err := cmd.CombinedOutput()
	identity := fmt.Sprintf("uid=%d,gid=%d", uid, gid)
	if err == nil {
		return "allowed", false, "linux-setuid-open", identity, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 13 {
		return "denied", true, "linux-setuid-open", identity, nil
	}
	msg := strings.TrimSpace(string(output))
	if msg == "" {
		msg = err.Error()
	}
	return "error", false, "linux-setuid-open", identity, fmt.Errorf("kernel open failed: %s", msg)
}
