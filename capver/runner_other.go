//go:build !windows

package capver

import "context"

// 非 Windows 平台：无法提供作业对象与受限令牌的真实隔离，
// 明确返回 E_UNSUPPORTED，不以软判定代替真实隔离。
type unsupportedRunner struct{}

func newPlatformRunner() Runner { return unsupportedRunner{} }

func (unsupportedRunner) Run(ctx context.Context, req RunRequest) (ProcessRecord, error) {
	rec := ProcessRecord{
		ProcID:   req.Decl.ID,
		Declared: sortedCaps(req.Decl.Capabilities),
		Code:     CodeUnsupported,
		Probes:   []ProbeResult{},
	}
	if rec.Declared == nil {
		rec.Declared = []Capability{}
	}
	return rec, nil
}
