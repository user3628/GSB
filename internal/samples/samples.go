package samples

import (
	"crypto/sha256"
	"encoding/binary"

	"depverify/internal/manifest"
	"depverify/internal/model"
)

// Kind 标识样本场景。
type Kind string

const (
	KindValid          Kind = "valid"
	KindTruncated      Kind = "truncated"
	KindBadMagic       Kind = "bad_magic"
	KindLengthOverflow Kind = "length_overflow"
	KindDangling       Kind = "dangling_edge"
	KindDowngrade      Kind = "version_downgrade"
	KindCycle          Kind = "cycle"
	KindConflict       Kind = "constraint_conflict"
	KindSnapshotGap    Kind = "snapshot_gap"
)

// Bundle 是一份自包含的确定性样本。
type Bundle struct {
	Kind        Kind
	Manifest    []byte
	Edges       []byte
	Roots       []string
	ExpectCodes []string
	Description string
}

// ExpectedCode 返回样本应触发的主业务码。
func (b Bundle) ExpectedCode() string {
	if len(b.ExpectCodes) == 0 {
		return ""
	}
	return b.ExpectCodes[0]
}

func digestOf(id string, version []int) []byte {
	buf := make([]byte, 0, len(id)+len(version)*8)
	buf = append(buf, id...)
	for _, v := range version {
		buf = append(buf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
	d := sha256.Sum256(buf)
	return d[:]
}

func comp(id string, version ...int) model.Component {
	return model.Component{
		ID: id, Version: version, DigestAlgo: "SHA256",
		Digest: digestOf(id, version), RegisteredDigest: digestOf(id, version),
	}
}

// baseComponents 是固定的正常组件集，摘要与登记值一致。
func baseComponents() []model.Component {
	return []model.Component{
		comp("acme/app", 2, 4, 1),
		comp("acme/core", 1, 7, 0),
		comp("acme/net", 3, 0, 2),
		comp("acme/log", 1, 2, 9),
		comp("acme/util", 4, 5, 0),
	}
}

// baseEdges 是与基础组件匹配的健康依赖边。
func baseEdges() string {
	return `{"parent":"acme/app","child":"acme/core","constraint":[1,7,0]}
{"parent":"acme/app","child":"acme/net","constraint":[3,0,0]}
{"parent":"acme/core","child":"acme/log","constraint":[1,2,0]}
{"parent":"acme/net","child":"acme/util","constraint":[4,0,0]}
{"parent":"acme/log","child":"acme/util","constraint":[4,0,0]}
`
}

// All 返回全部确定性样本（含正常样本与六类畸形样本）。
func All() []Bundle {
	return []Bundle{
		validBundle(),
		truncatedBundle(),
		badMagicBundle(),
		lengthOverflowBundle(),
		danglingBundle(),
		downgradeBundle(),
		cycleBundle(),
		conflictBundle(),
		snapshotGapBundle(),
	}
}

func validBundle() Bundle {
	return Bundle{
		Kind:        KindValid,
		Manifest:    manifest.Encode(baseComponents()),
		Edges:       []byte(baseEdges()),
		Roots:       []string{"acme/app"},
		ExpectCodes: nil,
		Description: "healthy closure, all digests match",
	}
}

func truncatedBundle() Bundle {
	full := manifest.Encode(baseComponents())
	// 截掉最后一条记录的尾部字节，制造记录负载截断。
	return Bundle{
		Kind:        KindTruncated,
		Manifest:    full[:len(full)-6],
		Edges:       []byte(baseEdges()),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.ETruncated},
		Description: "last manifest record payload truncated",
	}
}

func badMagicBundle() Bundle {
	full := manifest.Encode(baseComponents())
	broken := append([]byte(nil), full...)
	broken[0] = 'X'
	return Bundle{
		Kind:        KindBadMagic,
		Manifest:    broken,
		Edges:       []byte(baseEdges()),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.EBadMagic},
		Description: "magic header replaced",
	}
}

func lengthOverflowBundle() Bundle {
	full := manifest.Encode(baseComponents())
	// 魔数后写入一个 10 字节全 0x80/0x81 序列：超出 MaxVarintLen64 的合法编码，
	// 长度字段发生回绕/溢出。
	evil := make([]byte, 0, len(full)+10)
	evil = append(evil, full[:len(manifest.Magic)]...)
	evil = append(evil, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x81)
	evil = append(evil, full[len(manifest.Magic):]...)
	return Bundle{
		Kind:        KindLengthOverflow,
		Manifest:    evil,
		Edges:       []byte(baseEdges()),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.ELengthOverflow},
		Description: "length varint overflows 64 bits",
	}
}

func danglingBundle() Bundle {
	edges := baseEdges() +
		`{"parent":"acme/app","child":"acme/ghost","constraint":[1,0,0]}` + "\n"
	return Bundle{
		Kind:        KindDangling,
		Manifest:    manifest.Encode(baseComponents()),
		Edges:       []byte(edges),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.EEdgeDangling},
		Description: "edge points to component absent from manifest",
	}
}

func downgradeBundle() Bundle {
	// 要求 acme/net >= 3.1.0，但实际登记为 3.0.2，构成降级。
	edges := `{"parent":"acme/app","child":"acme/core","constraint":[1,7,0]}
{"parent":"acme/app","child":"acme/net","constraint":[3,1,0]}
{"parent":"acme/core","child":"acme/log","constraint":[1,2,0]}
{"parent":"acme/net","child":"acme/util","constraint":[4,0,0]}
{"parent":"acme/log","child":"acme/util","constraint":[4,0,0]}
`
	return Bundle{
		Kind:        KindDowngrade,
		Manifest:    manifest.Encode(baseComponents()),
		Edges:       []byte(edges),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.EVersionDowngrade},
		Description: "acme/net actual 3.0.2 below constraint 3.1.0",
	}
}

func cycleBundle() Bundle {
	// acme/core -> acme/log -> acme/util -> acme/core 构成环。
	edges := `{"parent":"acme/app","child":"acme/core","constraint":[1,7,0]}
{"parent":"acme/app","child":"acme/net","constraint":[3,0,0]}
{"parent":"acme/core","child":"acme/log","constraint":[1,2,0]}
{"parent":"acme/log","child":"acme/util","constraint":[4,0,0]}
{"parent":"acme/util","child":"acme/core","constraint":[1,7,0]}
{"parent":"acme/net","child":"acme/util","constraint":[4,0,0]}
`
	return Bundle{
		Kind:        KindCycle,
		Manifest:    manifest.Encode(baseComponents()),
		Edges:       []byte(edges),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.ECycle},
		Description: "core/log/util form a directed cycle",
	}
}

func conflictBundle() Bundle {
	// 同一子组件 acme/util 被两个父组件以互斥约束引用。
	edges := `{"parent":"acme/app","child":"acme/core","constraint":[1,7,0]}
{"parent":"acme/app","child":"acme/net","constraint":[3,0,0]}
{"parent":"acme/core","child":"acme/log","constraint":[1,2,0]}
{"parent":"acme/net","child":"acme/util","constraint":[4,5,0]}
{"parent":"acme/log","child":"acme/util","constraint":[4,0,0]}
`
	return Bundle{
		Kind:        KindConflict,
		Manifest:    manifest.Encode(baseComponents()),
		Edges:       []byte(edges),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.EConstraintConflict},
		Description: "two mutually exclusive constraints target acme/util",
	}
}

func snapshotGapBundle() Bundle {
	// 该样本本身是健康输入；跳号由存储层在测试中通过 InjectGap 制造。
	return Bundle{
		Kind:        KindSnapshotGap,
		Manifest:    manifest.Encode(baseComponents()),
		Edges:       []byte(baseEdges()),
		Roots:       []string{"acme/app"},
		ExpectCodes: []string{model.ESnapshotVersion},
		Description: "healthy input; snapshot chain gap injected at storage layer",
	}
}

// Large 生成规模可控的链式组件样本，用于双路径阈值切换验证。
func Large(n int) Bundle {
	comps := make([]model.Component, 0, n)
	edgeBuf := make([]byte, 0, n*80)
	for i := 0; i < n; i++ {
		id := componentID(i)
		comps = append(comps, comp(id, 1, 0, 0))
		if i > 0 {
			parent := componentID(i - 1)
			line := `{"parent":"` + parent + `","child":"` + id + `","constraint":[1,0,0]}` + "\n"
			edgeBuf = append(edgeBuf, line...)
		}
	}
	return Bundle{
		Kind:     KindValid,
		Manifest: manifest.Encode(comps),
		Edges:    edgeBuf,
		Roots:    []string{componentID(0)},
	}
}

func componentID(i int) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return "acme/c" + hex8(b[:])
}

func hex8(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexd[v>>4]
		out[i*2+1] = hexd[v&0x0f]
	}
	return string(out)
}
