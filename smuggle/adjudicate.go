package smuggle

import "bytes"

// Verdict 为走私面裁决的四类结论。
type Verdict string

const (
	VerdictConsistent        Verdict = "CONSISTENT"
	VerdictCountMismatch     Verdict = "COUNT_MISMATCH"
	VerdictBoundaryMismatch  Verdict = "BOUNDARY_MISMATCH"
	VerdictEncodingAmbiguity Verdict = "ENCODING_AMBIGUITY"
	// VerdictNone 表示连接未进入裁决（如批处理竞争被拒）。
	VerdictNone Verdict = "NONE"
)

// 算法路径标识。
const (
	PathPrecise = "precise"
	PathStream  = "stream"
)

// Adjudication 是单个连接的裁决结论。
type Adjudication struct {
	Verdict      Verdict
	Code         Code
	Offset       int
	Field        string
	ProxyCount   int
	BackendCount int
	ProxyCode    Code
	BackendCode  Code
}

// comparer 增量比较两个视角的边界序列，逐请求精算与流式扫描共用，
// 以保证两条路径产生逐字节一致的裁决结论与业务码序列。
type comparer struct {
	hasP, hasB bool
	pendP      Boundary
	pendB      Boundary
	divFound   bool
	firstDiv   int
	extraFound bool
	extraStart int
}

func (c *comparer) FeedProxy(b Boundary) {
	if c.hasP && !c.extraFound {
		c.extraFound = true
		c.extraStart = c.pendP.Start
	}
	c.pendP = b
	c.hasP = true
	c.pair()
}

func (c *comparer) FeedBackend(b Boundary) {
	if c.hasB && !c.extraFound {
		c.extraFound = true
		c.extraStart = c.pendB.Start
	}
	c.pendB = b
	c.hasB = true
	c.pair()
}

func (c *comparer) pair() {
	if !c.hasP || !c.hasB {
		return
	}
	p, q := c.pendP, c.pendB
	c.hasP, c.hasB = false, false
	if c.divFound {
		return
	}
	if p.Start != q.Start {
		c.divFound = true
		c.firstDiv = p.Start
		if q.Start < p.Start {
			c.firstDiv = q.Start
		}
		return
	}
	if p.End != q.End {
		c.divFound = true
		c.firstDiv = p.End
		if q.End < p.End {
			c.firstDiv = q.End
		}
	}
}

func (c *comparer) finish(pv, bv View) Adjudication {
	adj := Adjudication{
		Verdict:      VerdictConsistent,
		Code:         CodeOK,
		Offset:       -1,
		ProxyCount:   pv.Requests,
		BackendCount: bv.Requests,
		ProxyCode:    pv.Code,
		BackendCode:  bv.Code,
	}
	if c.hasP && !c.extraFound {
		c.extraFound = true
		c.extraStart = c.pendP.Start
	}
	if c.hasB && !c.extraFound {
		c.extraFound = true
		c.extraStart = c.pendB.Start
	}

	pAmb := pv.Code != CodeOK && pv.Code != CodeRequestBudget
	bAmb := bv.Code != CodeOK && bv.Code != CodeRequestBudget
	switch {
	case pAmb || bAmb:
		adj.Verdict = VerdictEncodingAmbiguity
		adj.Code = CodeSmuggleAmbiguity
		if pAmb {
			adj.Offset = pv.CodeOffset
			adj.Field = pv.CodeField
		} else {
			adj.Offset = bv.CodeOffset
			adj.Field = bv.CodeField
		}
	case pv.Requests != bv.Requests:
		adj.Verdict = VerdictCountMismatch
		adj.Code = CodeSmuggleAmbiguity
		switch {
		case c.divFound:
			adj.Offset = c.firstDiv
		case c.extraFound:
			adj.Offset = c.extraStart
		}
	case c.divFound:
		adj.Verdict = VerdictBoundaryMismatch
		adj.Code = CodeSmuggleAmbiguity
		adj.Offset = c.firstDiv
	}
	return adj
}

// SelectPath 依据单连接估计请求数与阈值确定性地选择算法路径。
// 估计值取两视角头部终止符数量的较大者，不依赖随机或墙钟因素。
func SelectPath(proxy, backend []byte, threshold int) (path string, estimated int) {
	ep := bytes.Count(proxy, []byte("\r\n\r\n"))
	eb := bytes.Count(backend, []byte("\r\n\r\n"))
	estimated = ep
	if eb > estimated {
		estimated = eb
	}
	if estimated < threshold {
		return PathPrecise, estimated
	}
	return PathStream, estimated
}

// adjudicatePrecise 逐请求精算路径：先物化边界再比较。
func adjudicatePrecise(proxy, backend []byte, maxReq int) (Adjudication, View, View) {
	pv := ParseView(proxy, maxReq)
	bv := ParseView(backend, maxReq)
	cmp := &comparer{}
	n := len(pv.Boundaries)
	if len(bv.Boundaries) > n {
		n = len(bv.Boundaries)
	}
	for i := 0; i < n; i++ {
		if i < len(pv.Boundaries) {
			cmp.FeedProxy(pv.Boundaries[i])
		}
		if i < len(bv.Boundaries) {
			cmp.FeedBackend(bv.Boundaries[i])
		}
	}
	return cmp.finish(pv, bv), pv, bv
}

// adjudicateStreaming 流式扫描路径：交错推进两个视角并增量比较，
// 不为比较而物化中间序列（边界仅为报告留存）。
func adjudicateStreaming(proxy, backend []byte, maxReq int) (Adjudication, View, View) {
	pv := View{Code: CodeOK, CodeOffset: -1}
	bv := View{Code: CodeOK, CodeOffset: -1}
	cmp := &comparer{}
	pc := &cursor{buf: proxy}
	bc := &cursor{buf: backend}
	pDone, bDone := false, false
	for !pDone || !bDone {
		if !pDone {
			pDone = stepView(pc, &pv, maxReq, func(b Boundary) {
				cmp.FeedProxy(b)
				pv.Boundaries = append(pv.Boundaries, b)
			})
		}
		if !bDone {
			bDone = stepView(bc, &bv, maxReq, func(b Boundary) {
				cmp.FeedBackend(b)
				bv.Boundaries = append(bv.Boundaries, b)
			})
		}
	}
	return cmp.finish(pv, bv), pv, bv
}
