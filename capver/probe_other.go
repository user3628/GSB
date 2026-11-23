//go:build !windows

package capver

// 非 Windows 平台：注册表与句柄继承探测不可用。
// 运行器在该平台直接返回 E_UNSUPPORTED，不会执行到此处；仅为编译完整。

func (p *probe) probeRegistryWrite(scenario string) {
	p.record(ProbeResult{Token: CapRegistryWrite, Target: `HKCU\Software\capver-probe`, Scenario: scenario,
		Allowed: false, Expected: p.declaredHas(CapRegistryWrite), DeniedBy: "component", RawError: "unsupported-platform"})
}

func (p *probe) probeInheritedHandle() {
	p.record(ProbeResult{Token: CapFileWrite, Target: "inherited-handle", Scenario: ScenarioHandleEscape,
		Allowed: false, Expected: false, DeniedBy: "component", RawError: "unsupported-platform"})
}
