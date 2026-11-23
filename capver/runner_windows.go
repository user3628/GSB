//go:build windows

package capver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows 真实隔离运行器：作业对象 + 受限令牌 + 显式句柄列表。

var (
	advapi32                   = windows.NewLazySystemDLL("advapi32.dll")
	procCreateRestrictedToken  = advapi32.NewProc("CreateRestrictedToken")
	procCreateProcessWithToken = advapi32.NewProc("CreateProcessWithTokenW")
)

const (
	disableMaxPrivilege     = 0x08
	logonWithProfile        = 0x00000001
	jobActiveProcessLimit   = 8
	waitTimeoutMilliseconds = 100
)

type windowsRunner struct{}

func newPlatformRunner() Runner { return windowsRunner{} }

// createRestrictedToken 由当前进程令牌创建受限令牌（禁用全部特权）。
func createRestrictedToken() (windows.Token, error) {
	var self windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(),
		windows.TOKEN_DUPLICATE|windows.TOKEN_QUERY, &self); err != nil {
		return 0, fmt.Errorf("open process token: %w", err)
	}
	defer self.Close()
	var restricted windows.Token
	r1, _, err := procCreateRestrictedToken.Call(
		uintptr(self),
		uintptr(disableMaxPrivilege),
		0, 0, 0, 0, 0, 0,
		uintptr(unsafe.Pointer(&restricted)),
	)
	if r1 == 0 {
		return 0, fmt.Errorf("CreateRestrictedToken: %w", err)
	}
	return restricted, nil
}

// jobConfig 封装作业对象的基本限制。
func setupJobObject() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, fmt.Errorf("CreateJobObject: %w", err)
	}
	var limit windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	limit.BasicLimitInformation.LimitFlags =
		windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	limit.BasicLimitInformation.ActiveProcessLimit = jobActiveProcessLimit
	if _, err := windows.SetInformationJobObject(job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limit)),
		uint32(unsafe.Sizeof(limit))); err != nil {
		windows.CloseHandle(job)
		return 0, fmt.Errorf("SetInformationJobObject(limits): %w", err)
	}
	ui := windows.JOBOBJECT_BASIC_UI_RESTRICTIONS{
		UIRestrictionsClass: windows.JOB_OBJECT_UILIMIT_HANDLES |
			windows.JOB_OBJECT_UILIMIT_READCLIPBOARD |
			windows.JOB_OBJECT_UILIMIT_WRITECLIPBOARD |
			windows.JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS |
			windows.JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
			windows.JOB_OBJECT_UILIMIT_GLOBALATOMS |
			windows.JOB_OBJECT_UILIMIT_DESKTOP |
			windows.JOB_OBJECT_UILIMIT_EXITWINDOWS,
	}
	if _, err := windows.SetInformationJobObject(job,
		windows.JobObjectBasicUIRestrictions,
		uintptr(unsafe.Pointer(&ui)),
		uint32(unsafe.Sizeof(ui))); err != nil {
		windows.CloseHandle(job)
		return 0, fmt.Errorf("SetInformationJobObject(ui): %w", err)
	}
	return job, nil
}

// inheritablePipe 创建一端可继承、一端不可继承的管道。
func inheritablePipe() (readH, writeH windows.Handle, err error) {
	sa := &windows.SecurityAttributes{InheritHandle: 1}
	if err := windows.CreatePipe(&readH, &writeH, sa, 0); err != nil {
		return 0, 0, err
	}
	return readH, writeH, nil
}

func makeNonInheritable(h windows.Handle) {
	_ = windows.SetHandleInformation(h, windows.HANDLE_FLAG_INHERIT, 0)
}

func (windowsRunner) Run(ctx context.Context, req RunRequest) (ProcessRecord, error) {
	// 1. 准备工作目录与限制目录外目标。
	if err := os.MkdirAll(req.WorkDir, 0o700); err != nil {
		return ProcessRecord{}, err
	}
	if err := os.MkdirAll(req.OutsideDir, 0o700); err != nil {
		return ProcessRecord{}, err
	}
	if err := os.WriteFile(filepath.Join(req.WorkDir, "seed.txt"), []byte("capver-seed\n"), 0o600); err != nil {
		return ProcessRecord{}, err
	}

	// 2. 句柄继承逃逸场景：父进程故意持有指向限制目录之外文件的可继承句柄。
	var escapeHandle windows.Handle
	if req.Inject == InjectHandleEscape {
		target := filepath.Join(req.OutsideDir, "escape-target.txt")
		tp, err := windows.UTF16PtrFromString(target)
		if err != nil {
			return ProcessRecord{}, err
		}
		sa := &windows.SecurityAttributes{InheritHandle: 1}
		h, err := windows.CreateFile(tp,
			windows.GENERIC_READ|windows.GENERIC_WRITE, 0, sa,
			windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, 0)
		if err != nil {
			return ProcessRecord{}, fmt.Errorf("create escape handle: %w", err)
		}
		escapeHandle = h
		defer windows.CloseHandle(escapeHandle)
	}

	// 3. 创建作业对象并设置基本限制。
	job, err := setupJobObject()
	if err != nil {
		return ProcessRecord{}, err
	}
	defer windows.CloseHandle(job) // KILL_ON_JOB_CLOSE：关闭即终止整棵进程树

	// 4. 创建受限令牌。
	token, err := createRestrictedToken()
	if err != nil {
		return ProcessRecord{}, err
	}
	defer token.Close()

	// 5. 准备标准 IO 管道（仅管道句柄进入子进程句柄表）。
	inR, inW, err := inheritablePipe()
	if err != nil {
		return ProcessRecord{}, err
	}
	defer windows.CloseHandle(inR)
	defer windows.CloseHandle(inW)
	outR, outW, err := inheritablePipe()
	if err != nil {
		return ProcessRecord{}, err
	}
	defer windows.CloseHandle(outR)
	defer windows.CloseHandle(outW)
	// 父进程端显式关闭可继承标志。
	makeNonInheritable(inW)
	makeNonInheritable(outR)

	// 6. 显式句柄列表：子进程仅继承管道句柄，escapeHandle 被排除。
	attrList, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return ProcessRecord{}, err
	}
	defer attrList.Delete()
	inherit := []windows.Handle{inR, outW}
	if err := attrList.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
		unsafe.Pointer(&inherit[0]),
		uintptr(len(inherit))*unsafe.Sizeof(inherit[0])); err != nil {
		return ProcessRecord{}, err
	}

	// 7. 构造探针命令行。
	args := []string{req.ExePath, "probe"}
	for _, a := range req.Decl.Command[2:] { // 去掉 "@self probe"
		args = append(args, a)
	}
	caps := make([]string, 0, len(req.Decl.Capabilities))
	for _, c := range req.Decl.Capabilities {
		caps = append(caps, string(c))
	}
	args = append(args,
		"-workdir", req.WorkDir,
		"-outside", req.OutsideDir,
		"-caps", strings.Join(caps, ","),
		"-whitelist", strings.Join(req.Decl.ChildWhitelist, ","),
	)
	if req.Inject == InjectHandleEscape {
		args = append(args, "-inherit-handle", fmt.Sprintf("%d", uintptr(escapeHandle)))
	}
	if req.SlowMS > 0 {
		args = append(args, "-sleep-ms", fmt.Sprintf("%d", req.SlowMS))
	}
	cmdLine, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(args))
	if err != nil {
		return ProcessRecord{}, err
	}
	exePtr, err := windows.UTF16PtrFromString(req.ExePath)
	if err != nil {
		return ProcessRecord{}, err
	}
	dirPtr, err := windows.UTF16PtrFromString(req.WorkDir)
	if err != nil {
		return ProcessRecord{}, err
	}

	si := &windows.StartupInfoEx{
		StartupInfo: windows.StartupInfo{
			Cb:        uint32(unsafe.Sizeof(windows.StartupInfoEx{})),
			Flags:     windows.STARTF_USESTDHANDLES,
			StdInput:  inR,
			StdOutput: outW,
			StdErr:    outW,
		},
		ProcThreadAttributeList: attrList.List(),
	}
	var pi windows.ProcessInformation
	r1, _, callErr := procCreateProcessWithToken.Call(
		uintptr(token),
		uintptr(logonWithProfile),
		uintptr(unsafe.Pointer(exePtr)),
		uintptr(unsafe.Pointer(cmdLine)),
		uintptr(windows.CREATE_SUSPENDED|windows.CREATE_UNICODE_ENVIRONMENT|windows.EXTENDED_STARTUPINFO_PRESENT),
		0,
		uintptr(unsafe.Pointer(dirPtr)),
		uintptr(unsafe.Pointer(si)),
		uintptr(unsafe.Pointer(&pi)),
	)
	if r1 == 0 {
		return ProcessRecord{}, fmt.Errorf("CreateProcessWithTokenW: %w", callErr)
	}
	defer windows.CloseHandle(pi.Process)
	defer windows.CloseHandle(pi.Thread)

	// 8. 关联作业对象后恢复主线程。
	if err := windows.AssignProcessToJobObject(job, pi.Process); err != nil {
		windows.TerminateProcess(pi.Process, 1)
		return ProcessRecord{}, fmt.Errorf("AssignProcessToJobObject: %w", err)
	}
	if _, err := windows.ResumeThread(pi.Thread); err != nil {
		windows.TerminateProcess(pi.Process, 1)
		return ProcessRecord{}, fmt.Errorf("ResumeThread: %w", err)
	}

	// 9. 中介循环（goroutine）+ 超时监控。
	type medResult struct {
		probes []ProbeResult
		err    error
	}
	medCh := make(chan medResult, 1)
	pol := &mediationPolicy{
		workDir:   req.WorkDir,
		exePath:   req.ExePath,
		whitelist: req.Decl.ChildWhitelist,
	}
	go func() {
		probes, err := mediateSession(ctx, os.NewFile(uintptr(inW), "stdin"), os.NewFile(uintptr(outR), "stdout"), pol, req.Norm)
		medCh <- medResult{probes, err}
	}()

	deadline := time.Now().Add(req.Timeout)
	timedOut := false
	for {
		ev, err := windows.WaitForSingleObject(pi.Process, waitTimeoutMilliseconds)
		if err != nil {
			return ProcessRecord{}, fmt.Errorf("WaitForSingleObject: %w", err)
		}
		if ev == windows.WAIT_OBJECT_0 {
			break
		}
		if time.Now().After(deadline) {
			timedOut = true
			_ = windows.TerminateJobObject(job, 1) // 通过作业对象终止整棵进程树
			break
		}
		select {
		case <-ctx.Done():
			timedOut = true
			_ = windows.TerminateJobObject(job, 1)
		default:
		}
		if timedOut {
			break
		}
	}

	var med medResult
	select {
	case med = <-medCh:
	case <-time.After(5 * time.Second):
		med = medResult{err: fmt.Errorf("mediation did not finish")}
	}
	if med.err != nil && !timedOut {
		return ProcessRecord{}, newError(CodeStore, "mediation failed", med.err)
	}

	// 10. 判定；违规进程通过作业对象确保终止。
	rec := EvaluateRecord(req.Decl, med.probes, timedOut)
	if rec.Code != CodeOK && !timedOut {
		_ = windows.TerminateJobObject(job, 1)
	}
	return rec, nil
}
