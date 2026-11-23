package pin

import (
	"fmt"
	"sort"

	"crypto/x509"

	"mitmpin/internal/certutil"
	"mitmpin/internal/sample"
)

// PinHit 记录命中的固定项，用于报告逐条复核。
type PinHit struct {
	PinID       string `json:"pin_id"`
	CertSubject string `json:"cert_subject"`
	Algo        string `json:"algo,omitempty"`
	Matched     bool   `json:"matched"`
}

// Finding 是固定校验事件的单项检查结论。
type Finding struct {
	Check  string `json:"check"`
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

// Verdict 是一次 pin_check 事件的完整结论。
type Verdict struct {
	Code            string    `json:"code"` // 主业务码
	PolicyID        string    `json:"policy_id"`
	PolicyVersion   int       `json:"policy_version"`
	Mode            string    `json:"mode"`
	LeafFingerprint string    `json:"leaf_fingerprint"`
	Hits            []PinHit  `json:"hits"`
	Findings        []Finding `json:"findings"`
}

func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

// Evaluate 在 pin_check 事件处按当前生效策略重放固定校验。
// chain 为已解析的观测证书链（叶在前）。
func Evaluate(obs sample.Observation, chain []*x509.Certificate, policy sample.Policy, version int) Verdict {
	leaf := chain[0]
	verdict := Verdict{
		Code:            sample.E_OK,
		PolicyID:        policy.PolicyID,
		PolicyVersion:   version,
		Mode:            policy.Mode,
		LeafFingerprint: certutil.LeafFingerprint(leaf),
		Hits:            []PinHit{},
		Findings:        []Finding{},
	}

	// 1) 协议版本闸门：独立业务码 E_PROTOCOL_NOT_ALLOWED。
	if !contains(policy.AllowedVersions, obs.Protocol) {
		verdict.Code = sample.E_PROTOCOL_NOT_ALLOWED
		verdict.Findings = append(verdict.Findings, Finding{
			Check: "protocol", Code: sample.E_PROTOCOL_NOT_ALLOWED,
			Detail: fmt.Sprintf("协议 %s 不在允许集合 %v", obs.Protocol, policy.AllowedVersions),
		})
	}

	// 2) SNI 与证书 SAN 匹配：独立业务码 E_SNI_MISMATCH。
	if !certutil.SANMatches(leaf, obs.SNI) {
		verdict.Findings = append(verdict.Findings, Finding{
			Check: "sni", Code: sample.E_SNI_MISMATCH,
			Detail: fmt.Sprintf("SNI %s 与证书 SAN %v 不匹配", obs.SNI, leaf.DNSNames),
		})
		if verdict.Code == sample.E_OK {
			verdict.Code = sample.E_SNI_MISMATCH
		}
	}

	// 3) 链结构完整性公共闸门：签名关系不连续（中间 CA 缺失等）直接判链断裂。
	if structErr := certutil.CheckChainStructure(chain); structErr != nil {
		verdict.Code = sample.E_CHAIN_BROKEN
		verdict.Findings = append(verdict.Findings, Finding{
			Check: "chain_structure", Code: sample.E_CHAIN_BROKEN, Detail: structErr.Error(),
		})
		return verdict
	}

	// 4) 固定主体：SPKI 集合比对或受信根路径构建。
	switch policy.Mode {
	case sample.PinModeSPKI:
		evaluateSPKI(chain, policy, &verdict)
	case sample.PinModeRoot:
		evaluateRoots(obs, chain, policy, &verdict)
	}

	// 固定主体失败（MITM）优先于协议/SNI 之外的展示，但保持各 Finding 独立可追溯。
	return verdict
}

// evaluateSPKI 对叶与中间证书计算 SPKI 摘要并与固定集合比对。
// 规则：至少一个固定项命中链中任一证书即通过；全部未命中判 E_MITM_SUSPECTED。
func evaluateSPKI(chain []*x509.Certificate, policy sample.Policy, verdict *Verdict) {
	// 固定项按 ID 排序，保证报告稳定。
	pins := append([]sample.PinItem(nil), policy.Pins...)
	sort.Slice(pins, func(i, j int) bool { return pins[i].ID < pins[j].ID })

	anyMatch := false
	for _, pinItem := range pins {
		hit := PinHit{PinID: pinItem.ID, Algo: pinItem.Algo, Matched: false}
		for _, cert := range chain {
			digest, err := certutil.SPKIDigest(cert, pinItem.Algo)
			if err != nil {
				verdict.Findings = append(verdict.Findings, Finding{
					Check: "spki", Code: sample.E_HASH_ALGO_UNSUPPORTED, Detail: err.Error(),
				})
				continue
			}
			if digest == pinItem.Digest {
				hit.Matched = true
				hit.CertSubject = cert.Subject.CommonName
				anyMatch = true
			}
		}
		verdict.Hits = append(verdict.Hits, hit)
	}

	if !anyMatch {
		verdict.Code = sample.E_MITM_SUSPECTED
		verdict.Findings = append(verdict.Findings, Finding{
			Check: "spki_pins", Code: sample.E_MITM_SUSPECTED,
			Detail: "叶与中间证书的 SPKI 摘要均未命中任何固定项",
		})
	} else {
		verdict.Findings = append(verdict.Findings, Finding{
			Check: "spki_pins", Code: sample.E_OK, Detail: "至少一个固定项命中证书链",
		})
	}
}

// evaluateRoots 构建并验证到受信根的路径；失败判 E_MITM_SUSPECTED。
func evaluateRoots(obs sample.Observation, chain []*x509.Certificate, policy sample.Policy, verdict *Verdict) {
	roots := make([]*x509.Certificate, 0, len(policy.Pins))
	for _, pinItem := range policy.Pins {
		root, err := certutil.ParseCert(pinItem.RootPEM)
		if err != nil {
			verdict.Findings = append(verdict.Findings, Finding{
				Check: "trusted_roots", Code: sample.E_CHAIN_BROKEN, Detail: err.Error(),
			})
			continue
		}
		roots = append(roots, root)
	}

	var intermediates []*x509.Certificate
	if len(chain) > 1 {
		intermediates = chain[1:]
	}
	count, err := certutil.VerifyChain(chain[0], intermediates, roots, "")
	if err != nil {
		verdict.Code = sample.E_MITM_SUSPECTED
		verdict.Findings = append(verdict.Findings, Finding{
			Check: "trusted_roots", Code: sample.E_MITM_SUSPECTED,
			Detail: "无法构建到受信根的有效路径: " + err.Error(),
		})
		return
	}
	for _, root := range roots {
		verdict.Hits = append(verdict.Hits, PinHit{
			PinID: root.Subject.CommonName, CertSubject: root.Subject.CommonName, Matched: true,
		})
	}
	sort.Slice(verdict.Hits, func(i, j int) bool { return verdict.Hits[i].PinID < verdict.Hits[j].PinID })
	verdict.Findings = append(verdict.Findings, Finding{
		Check: "trusted_roots", Code: sample.E_OK,
		Detail: fmt.Sprintf("成功构建 %d 条受信路径", count),
	})
}
