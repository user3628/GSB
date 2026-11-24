//go:build windows

package fsx

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/model"
)

var (
	advapi32                      = windows.NewLazySystemDLL("advapi32.dll")
	procGetSecurityDescriptorDacl = advapi32.NewProc("GetSecurityDescriptorDacl")
	procCreateRestrictedToken     = advapi32.NewProc("CreateRestrictedToken")
	procImpersonateLoggedOnUser   = advapi32.NewProc("ImpersonateLoggedOnUser")
)

type aclHeaderView struct {
	Revision byte
	Sbz1     byte
	Size     uint16
	AceCount uint16
	Sbz2     uint16
}

type aceRecord struct {
	Type  uint8
	Flags uint8
	Mask  uint32
	SID   string
}

func platformName() string { return "windows-security-descriptor" }

func syntheticSID(subjectID string) (*windows.SID, error) {
	sum := sha256.Sum256([]byte("hostacl-workbench/sid/" + subjectID))
	a := binary.LittleEndian.Uint32(sum[0:4])
	b := binary.LittleEndian.Uint32(sum[4:8])
	c := binary.LittleEndian.Uint32(sum[8:12])
	rid := 1000 + binary.LittleEndian.Uint32(sum[12:16])%1_000_000_000
	return windows.StringToSid(fmt.Sprintf("S-1-5-21-%d-%d-%d-%d", a, b, c, rid))
}

func platformVerificationAvailable() error { return nil }

func readDenyMask() uint32  { return uint32(windows.FILE_GENERIC_READ) }
func writeDenyMask() uint32 { return uint32(windows.FILE_GENERIC_WRITE) }
func openAllowMask() uint32 {
	return uint32(windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE | windows.FILE_GENERIC_EXECUTE)
}

func applyPlatformACL(tree string, policy *model.Policy, expected map[string]map[string]model.AccessBits) (map[string]string, error) {
	baselineSID, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil {
		return nil, err
	}
	systemSID, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return nil, err
	}
	subjectIDs := make([]string, 0, len(policy.Subjects))
	sids := map[string]*windows.SID{}
	for _, subject := range policy.Subjects {
		subjectIDs = append(subjectIDs, subject.ID)
		sid, err := syntheticSID(subject.ID)
		if err != nil {
			return nil, err
		}
		sids[subject.ID] = sid
	}
	sort.Strings(subjectIDs)
	digests := make(map[string]string, len(policy.Paths))
	for _, managed := range policy.Paths {
		entries := make([]windows.EXPLICIT_ACCESS, 0, len(subjectIDs)+2)
		expectedACEs := map[string]aceRecord{}
		for _, subjectID := range subjectIDs {
			bits := expected[managed.Path][subjectID]
			mask := uint32(0)
			if !bits.Read {
				mask |= readDenyMask()
			}
			if !bits.Write {
				mask |= writeDenyMask()
			}
			if mask == 0 {
				continue
			}
			entries = append(entries, explicitEntry(sids[subjectID], mask, windows.DENY_ACCESS))
			expectedACEs[aceKey(windows.ACCESS_DENIED_ACE_TYPE, sids[subjectID].String())] = aceRecord{
				Type: windows.ACCESS_DENIED_ACE_TYPE, Mask: mask, SID: sids[subjectID].String(),
			}
		}
		entries = append(entries, explicitEntry(baselineSID, openAllowMask(), windows.GRANT_ACCESS))
		expectedACEs[aceKey(windows.ACCESS_ALLOWED_ACE_TYPE, baselineSID.String())] = aceRecord{
			Type: windows.ACCESS_ALLOWED_ACE_TYPE, Mask: openAllowMask(), SID: baselineSID.String(),
		}
		entries = append(entries, explicitEntry(systemSID, uint32(windows.GENERIC_ALL), windows.GRANT_ACCESS))
		expectedACEs[aceKey(windows.ACCESS_ALLOWED_ACE_TYPE, systemSID.String())] = aceRecord{
			Type: windows.ACCESS_ALLOWED_ACE_TYPE, Mask: uint32(windows.GENERIC_ALL), SID: systemSID.String(),
		}
		acl, err := windows.ACLFromEntries(entries, nil)
		if err != nil {
			return nil, err
		}
		target := tree + `\` + strings.ReplaceAll(managed.Path, "/", `\`)
		info := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION)
		if err := windows.SetNamedSecurityInfo(target, windows.SE_FILE_OBJECT, info, nil, nil, acl, nil); err != nil {
			return nil, apperr.With(apperr.ApplyMismatch, "failed to write Windows security descriptor", map[string]any{
				"path": managed.Path, "error": err.Error(),
			})
		}
		sd, err := windows.GetNamedSecurityInfo(target, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			return nil, apperr.With(apperr.ApplyMismatch, "failed to read back Windows security descriptor", map[string]any{
				"path": managed.Path, "error": err.Error(),
			})
		}
		actualACEs, err := readDACL(sd)
		if err != nil {
			return nil, apperr.With(apperr.ApplyMismatch, "failed to parse Windows DACL", map[string]any{
				"path": managed.Path, "error": err.Error(),
			})
		}
		if differences := compareACEs(expectedACEs, actualACEs); len(differences) > 0 {
			return nil, applyMismatch(managed.Path, differences)
		}
		sum := sha256.Sum256([]byte(sd.String()))
		digests[managed.Path] = "sha256:" + hex.EncodeToString(sum[:])
	}
	return digests, nil
}

func explicitEntry(sid *windows.SID, mask uint32, mode windows.ACCESS_MODE) windows.EXPLICIT_ACCESS {
	return windows.EXPLICIT_ACCESS{
		AccessPermissions: windows.ACCESS_MASK(mask),
		AccessMode:        mode,
		Inheritance:       windows.NO_INHERITANCE,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_UNKNOWN,
			TrusteeValue: windows.TrusteeValueFromSID(sid),
		},
	}
}

func aceKey(aceType uint8, sid string) string { return fmt.Sprintf("%d|%s", aceType, sid) }

func readDACL(sd *windows.SECURITY_DESCRIPTOR) ([]aceRecord, error) {
	var present int32
	var defaulted int32
	var acl *windows.ACL
	ok, _, callErr := procGetSecurityDescriptorDacl.Call(
		uintptr(unsafe.Pointer(sd)),
		uintptr(unsafe.Pointer(&present)),
		uintptr(unsafe.Pointer(&acl)),
		uintptr(unsafe.Pointer(&defaulted)),
	)
	if ok == 0 {
		return nil, fmt.Errorf("GetSecurityDescriptorDacl failed: %v", callErr)
	}
	if present == 0 || acl == nil {
		return nil, nil
	}
	header := (*aclHeaderView)(unsafe.Pointer(acl))
	out := make([]aceRecord, 0, header.AceCount)
	for i := uint16(0); i < header.AceCount; i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, uint32(i), &ace); err != nil {
			return nil, err
		}
		if ace == nil {
			return nil, fmt.Errorf("GetAce returned nil")
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		out = append(out, aceRecord{
			Type: ace.Header.AceType, Flags: ace.Header.AceFlags,
			Mask: uint32(ace.Mask), SID: sid.String(),
		})
	}
	return out, nil
}

func compareACEs(expected map[string]aceRecord, actual []aceRecord) []string {
	actualMap := map[string]aceRecord{}
	for _, ace := range actual {
		actualMap[aceKey(ace.Type, ace.SID)] = ace
	}
	var differences []string
	for key, want := range expected {
		got, ok := actualMap[key]
		if !ok {
			differences = append(differences, "missing ACE "+key)
			continue
		}
		if got.Mask != want.Mask {
			differences = append(differences, fmt.Sprintf("ACE %s mask want 0x%x got 0x%x", key, want.Mask, got.Mask))
		}
		if got.Flags != 0 {
			differences = append(differences, fmt.Sprintf("ACE %s inherited flags want 0 got 0x%x", key, got.Flags))
		}
		delete(actualMap, key)
	}
	for key := range actualMap {
		differences = append(differences, "unexpected ACE "+key)
	}
	sort.Strings(differences)
	return differences
}

func openAsPlatformIdentity(ctx context.Context, tree, target, kind, subjectID, mode string) (string, bool, string, string, error) {
	select {
	case <-ctx.Done():
		return "error", false, platformName(), "", ctx.Err()
	default:
	}
	subjectSID, err := syntheticSID(subjectID)
	if err != nil {
		return "error", false, platformName(), "", err
	}
	baselineSID, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil {
		return "error", false, platformName(), "", err
	}
	restrictedSID, err := windows.CreateWellKnownSid(windows.WinRestrictedCodeSid)
	if err != nil {
		return "error", false, platformName(), "", err
	}
	access := uint32(windows.FILE_GENERIC_READ)
	if mode == "write" {
		access = uint32(windows.FILE_GENERIC_WRITE)
	} else if mode != "read" {
		return "error", false, platformName(), subjectSID.String(), fmt.Errorf("unsupported open mode %q", mode)
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var base windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), 0x0002|0x0008, &base); err != nil {
		return "error", false, platformName(), subjectSID.String(), err
	}
	defer windows.CloseHandle(windows.Handle(base))
	restrictSIDs := []windows.SIDAndAttributes{
		{Sid: baselineSID, Attributes: 0},
		{Sid: restrictedSID, Attributes: 0},
		{Sid: subjectSID, Attributes: 0},
	}
	var restricted windows.Token
	ok, _, callErr := procCreateRestrictedToken.Call(
		uintptr(base), 0x1, 0, 0, 0, 0,
		uintptr(len(restrictSIDs)), uintptr(unsafe.Pointer(&restrictSIDs[0])),
		uintptr(unsafe.Pointer(&restricted)),
	)
	runtime.KeepAlive(restrictSIDs)
	if ok == 0 {
		return "error", false, platformName(), subjectSID.String(), fmt.Errorf("CreateRestrictedToken failed: %v", callErr)
	}
	defer windows.CloseHandle(windows.Handle(restricted))
	ok, _, callErr = procImpersonateLoggedOnUser.Call(uintptr(restricted))
	if ok == 0 {
		return "error", false, platformName(), subjectSID.String(), fmt.Errorf("ImpersonateLoggedOnUser failed: %v", callErr)
	}
	defer windows.RevertToSelf()

	pathp, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return "error", false, platformName(), subjectSID.String(), err
	}
	handle, openErr := windows.CreateFile(pathp, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if openErr == nil {
		_ = windows.CloseHandle(handle)
		return "allowed", false, "windows-restricted-token-open", subjectSID.String(), nil
	}
	if openErr == windows.ERROR_ACCESS_DENIED {
		return "denied", true, "windows-restricted-token-open", subjectSID.String(), nil
	}
	return "error", false, "windows-restricted-token-open", subjectSID.String(), fmt.Errorf("CreateFile failed: %w", openErr)
}
