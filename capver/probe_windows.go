//go:build windows

package capver

import (
	"os"

	"golang.org/x/sys/windows/registry"
)

// probeRegistryWrite 在真实内核边界上探测注册表写能力：
// 写 HKCU\Software\capver-probe（用户 hive，声明内应成功）。
func (p *probe) probeRegistryWrite(scenario string) {
	expected := p.declaredHas(CapRegistryWrite)
	const path = `Software\capver-probe`
	k, _, err := registry.CreateKey(registry.CURRENT_USER, path, registry.SET_VALUE)
	if err != nil {
		p.record(ProbeResult{Token: CapRegistryWrite, Target: `HKCU\` + path, Scenario: scenario,
			Allowed: false, Expected: expected, DeniedBy: "kernel", RawError: err.Error()})
		return
	}
	defer k.Close()
	if err := k.SetDWordValue("probe", 1); err != nil {
		p.record(ProbeResult{Token: CapRegistryWrite, Target: `HKCU\` + path, Scenario: scenario,
			Allowed: false, Expected: expected, DeniedBy: "kernel", RawError: err.Error()})
		return
	}
	p.record(ProbeResult{Token: CapRegistryWrite, Target: `HKCU\` + path, Scenario: scenario,
		Allowed: true, Expected: expected})
}

// probeInheritedHandle 基于真实句柄表状态探测句柄继承逃逸：
// 父进程故意持有指向限制目录之外文件的可继承句柄，
// 但启动时通过 PROC_THREAD_ATTRIBUTE_HANDLE_LIST 将其排除，
// 子进程使用该句柄值应得到 ERROR_INVALID_HANDLE。
func (p *probe) probeInheritedHandle() {
	target := "inherited-handle:" + uintToStr(p.inheritHandle)
	if p.inheritHandle == 0 {
		p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: ScenarioHandleEscape,
			Allowed: false, Expected: false, DeniedBy: "component", RawError: "no-handle-passed"})
		return
	}
	f := os.NewFile(uintptr(p.inheritHandle), "capver-inherited")
	if f == nil {
		p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: ScenarioHandleEscape,
			Allowed: false, Expected: false, DeniedBy: "kernel", RawError: "invalid-handle"})
		return
	}
	defer f.Close()
	if _, err := f.Write([]byte("escape\n")); err != nil {
		p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: ScenarioHandleEscape,
			Allowed: false, Expected: false, DeniedBy: "kernel", RawError: err.Error()})
		return
	}
	// 句柄可用：逃逸成功（隔离失效）。
	p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: ScenarioHandleEscape,
		Allowed: true, Expected: false})
}

func uintToStr(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
