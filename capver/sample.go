package capver

// SampleSeed 固定样本种子，保证样本确定性。
const SampleSeed = "capver-sample-v1"

// probeCommand 返回以当前组件二进制自身作为探针子进程的启动命令。
// exe 由调用方在运行时解析（os.Executable），样本中只保留逻辑占位符，
// 以保证样本内容跨机器、跨目录字节级一致。
func probeCommand(args ...string) []string {
	return append([]string{"@self", "probe"}, args...)
}

// GenerateSamplePolicy 生成内置确定性样本策略。
// 不依赖外部数据文件、账号、网络或真实凭据。
func GenerateSamplePolicy() *Policy {
	return &Policy{
		Name: SampleSeed,
		Processes: []ProcessDecl{
			{
				ID:             "proc-reader",
				Command:        probeCommand("-plan", "baseline"),
				Capabilities:   []Capability{CapFileRead},
				RestrictedDir:  "self",
				ChildWhitelist: nil,
			},
			{
				ID:             "proc-writer",
				Command:        probeCommand("-plan", "baseline"),
				Capabilities:   []Capability{CapFileRead, CapFileWrite},
				RestrictedDir:  "self",
				ChildWhitelist: nil,
			},
			{
				ID:             "proc-spawner",
				Command:        probeCommand("-plan", "baseline"),
				Capabilities:   []Capability{CapFileRead, CapChildCreate},
				RestrictedDir:  "self",
				ChildWhitelist: []string{"@self"},
			},
			{
				ID:             "proc-registry",
				Command:        probeCommand("-plan", "baseline"),
				Capabilities:   []Capability{CapFileRead, CapRegistryWrite},
				RestrictedDir:  "self",
				ChildWhitelist: nil,
			},
		},
	}
}

// InjectKind 异常注入类型。
type InjectKind string

const (
	InjectNone              InjectKind = ""
	InjectUnauthorizedWrite InjectKind = "unauthorized-write" // 越权文件写
	InjectChildEscape       InjectKind = "child-escape"       // 白名单外子进程
	InjectHandleEscape      InjectKind = "handle-escape"      // 可继承句柄逃逸
	InjectCapLeak           InjectKind = "cap-leak"           // 能力泄漏
	InjectSnapshotGap       InjectKind = "snapshot-gap"       // 快照版本跳号
)

// AllInjectKinds 返回全部异常注入类型（稳定顺序）。
func AllInjectKinds() []InjectKind {
	return []InjectKind{
		InjectUnauthorizedWrite,
		InjectChildEscape,
		InjectHandleEscape,
		InjectCapLeak,
		InjectSnapshotGap,
	}
}

// ExpectedCode 返回每种注入应产生的稳定业务码。
func (k InjectKind) ExpectedCode() Code {
	switch k {
	case InjectUnauthorizedWrite, InjectChildEscape:
		return CodeCapDenied
	case InjectHandleEscape:
		return CodeHandleEscape
	case InjectCapLeak:
		return CodeCapLeak
	case InjectSnapshotGap:
		return CodeSnapshotVersion
	default:
		return CodeOK
	}
}

// Valid 校验注入类型是否合法。
func (k InjectKind) Valid() bool {
	if k == InjectNone {
		return true
	}
	for _, k2 := range AllInjectKinds() {
		if k == k2 {
			return true
		}
	}
	return false
}

// ApplyInjection 按注入类型在样本策略上追加一个触发异常的受测进程声明。
// 快照跳号注入不修改策略（它作用于存储层），返回原策略。
// 注入仅通过输入参数触发，不修改源码。
func ApplyInjection(p *Policy, kind InjectKind) *Policy {
	out := *p
	out.Processes = append([]ProcessDecl(nil), p.Processes...)
	switch kind {
	case InjectUnauthorizedWrite:
		out.Processes = append(out.Processes, ProcessDecl{
			ID:             "proc-inject-write",
			Command:        probeCommand("-plan", "inject-unauthorized-write"),
			Capabilities:   []Capability{CapFileRead, CapFileWrite},
			RestrictedDir:  "self",
			ChildWhitelist: nil,
		})
	case InjectChildEscape:
		out.Processes = append(out.Processes, ProcessDecl{
			ID:             "proc-inject-child",
			Command:        probeCommand("-plan", "inject-child-escape"),
			Capabilities:   []Capability{CapFileRead, CapChildCreate},
			RestrictedDir:  "self",
			ChildWhitelist: []string{"@self"},
		})
	case InjectHandleEscape:
		out.Processes = append(out.Processes, ProcessDecl{
			ID:             "proc-inject-handle",
			Command:        probeCommand("-plan", "inject-handle-escape"),
			Capabilities:   []Capability{CapFileRead},
			RestrictedDir:  "self",
			ChildWhitelist: nil,
		})
	case InjectCapLeak:
		// 声明不含 registry.write，但启动时被额外授予（由运行器注入），
		// 探针将发现实际能力超出声明。
		out.Processes = append(out.Processes, ProcessDecl{
			ID:             "proc-inject-leak",
			Command:        probeCommand("-plan", "inject-cap-leak"),
			Capabilities:   []Capability{CapFileRead},
			RestrictedDir:  "self",
			ChildWhitelist: nil,
		})
	case InjectSnapshotGap:
		// 存储层注入，策略不变。
	}
	return &out
}

// InjectedProcID 返回注入进程的稳定标识（用于报告与断言）。
func (k InjectKind) InjectedProcID() string {
	switch k {
	case InjectUnauthorizedWrite:
		return "proc-inject-write"
	case InjectChildEscape:
		return "proc-inject-child"
	case InjectHandleEscape:
		return "proc-inject-handle"
	case InjectCapLeak:
		return "proc-inject-leak"
	default:
		return ""
	}
}

// String 实现 fmt.Stringer。
func (k InjectKind) String() string { return string(k) }
