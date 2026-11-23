package smuggle

import (
	"bytes"
	"strconv"
	"strings"
)

// Boundary 描述单条请求在原始报文中的字节边界区间与解析业务码。
type Boundary struct {
	Index   int    `json:"index"`
	Start   int    `json:"start"`
	HeadEnd int    `json:"head_end"`
	End     int    `json:"end"`
	Code    Code   `json:"code"`
	Field   string `json:"field"`
}

// View 是一个视角（代理或后端）下整段报文的解析结果。
type View struct {
	Boundaries []Boundary
	Requests   int
	Code       Code
	CodeOffset int
	CodeField  string
}

type cursor struct {
	buf []byte
	pos int
}

func (c *cursor) eof() bool { return c.pos >= len(c.buf) }

// readLine 读取到下一个 CRLF 为止，返回不含 CRLF 的行内容。
func (c *cursor) readLine() ([]byte, bool) {
	i := bytes.Index(c.buf[c.pos:], []byte("\r\n"))
	if i < 0 {
		return nil, false
	}
	line := c.buf[c.pos : c.pos+i]
	c.pos += i + 2
	return line, true
}

func trimOWS(b []byte) []byte { return bytes.Trim(b, " \t") }

func validStartLine(line []byte) bool {
	if len(line) == 0 || line[0] == ' ' || line[0] == '\t' {
		return false
	}
	s := string(line)
	if !strings.HasSuffix(s, " HTTP/1.0") && !strings.HasSuffix(s, " HTTP/1.1") {
		return false
	}
	return strings.IndexByte(s, ' ') > 0
}

// scanNext 从游标处解析一条请求，返回其边界。
// 返回值的 End 为 -1 表示边界无法确定，调用方必须停止扫描。
func scanNext(c *cursor, index int) Boundary {
	b := Boundary{Index: index, Start: c.pos, HeadEnd: -1, End: -1, Code: CodeOK}

	line, ok := c.readLine()
	if !ok || !validStartLine(line) {
		b.Code = CodeStartMalformed
		return b
	}

	var clVals []string
	var teVals []string
	prevName := ""
	havePrev := false
	for {
		line, ok = c.readLine()
		if !ok {
			b.Code = CodeHeaderMalformed
			return b
		}
		if len(line) == 0 {
			break
		}
		if line[0] == ' ' || line[0] == '\t' {
			// obs-fold 续行：归一化为单个空格拼接至前一字段值。
			if !havePrev {
				b.Code = CodeHeaderMalformed
				return b
			}
			cont := string(trimOWS(line))
			switch prevName {
			case "content-length":
				clVals[len(clVals)-1] += " " + cont
			case "transfer-encoding":
				teVals[len(teVals)-1] += " " + cont
			}
			continue
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			b.Code = CodeHeaderMalformed
			return b
		}
		name := strings.ToLower(string(trimOWS(line[:colon])))
		value := string(trimOWS(line[colon+1:]))
		havePrev = true
		prevName = name
		switch name {
		case "content-length":
			clVals = append(clVals, value)
		case "transfer-encoding":
			teVals = append(teVals, value)
		}
	}
	b.HeadEnd = c.pos

	// Content-Length 归一化：多值需数值一致，否则为冲突。
	clSet := false
	var clVal int64
	if len(clVals) > 0 {
		first := true
		for _, v := range clVals {
			for _, tok := range strings.Split(v, ",") {
				tok = strings.Trim(tok, " \t")
				n, err := strconv.ParseInt(tok, 10, 64)
				if err != nil || n < 0 {
					b.Code = CodeCLMalformed
					b.Field = "content-length"
					return b
				}
				if first {
					clVal = n
					first = false
				} else if n != clVal {
					b.Code = CodeCLConflict
					b.Field = "content-length"
					return b
				}
			}
		}
		clSet = true
	}

	// Transfer-Encoding 归一化：多值合并、大小写统一、chunked 必须为最终编码。
	chunked := false
	if len(teVals) > 0 {
		var toks []string
		for _, v := range teVals {
			for _, t := range strings.Split(v, ",") {
				t = strings.ToLower(strings.Trim(t, " \t"))
				if t != "" {
					toks = append(toks, t)
				}
			}
		}
		if len(toks) > 0 {
			for _, t := range toks {
				if t != "chunked" && t != "identity" {
					b.Code = CodeTEUnknown
					b.Field = "transfer-encoding"
					return b
				}
			}
			if toks[len(toks)-1] == "chunked" {
				chunked = true
			} else {
				for _, t := range toks {
					if t == "chunked" {
						b.Code = CodeTEOrder
						b.Field = "transfer-encoding"
						return b
					}
				}
			}
		}
	}
	if chunked && clSet {
		// CL 与 TE 并存：按 TE 优先继续定界，但记录歧义业务码。
		b.Code = CodeCLTEConflict
		b.Field = "content-length"
	}

	switch {
	case chunked:
		end, code := parseChunked(c)
		if code != CodeOK {
			b.Code = code
			b.Field = "transfer-encoding"
			return b
		}
		b.End = end
	case clSet:
		if int64(len(c.buf)-c.pos) < clVal {
			b.Code = CodeBodyTruncated
			b.Field = "content-length"
			return b
		}
		b.End = c.pos + int(clVal)
		c.pos = b.End
	default:
		b.End = c.pos
	}
	return b
}

// parseChunked 解析分块体，返回终止偏移。分块长度允许前导零与大小写十六进制。
func parseChunked(c *cursor) (int, Code) {
	for {
		line, ok := c.readLine()
		if !ok {
			return c.pos, CodeChunkMalformed
		}
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			// obs-fold 嵌入分块边界。
			return c.pos, CodeChunkObsFold
		}
		sizeTok := line
		if i := bytes.IndexByte(line, ';'); i >= 0 {
			sizeTok = line[:i]
		}
		sizeTok = trimOWS(sizeTok)
		if len(sizeTok) == 0 {
			return c.pos, CodeChunkMalformed
		}
		n, err := strconv.ParseUint(string(sizeTok), 16, 63)
		if err != nil {
			return c.pos, CodeChunkMalformed
		}
		if n == 0 {
			//  trailer 段直至空行。
			for {
				tl, ok := c.readLine()
				if !ok {
					return c.pos, CodeBodyTruncated
				}
				if len(tl) == 0 {
					return c.pos, CodeOK
				}
			}
		}
		if uint64(len(c.buf)-c.pos) < n+2 {
			return c.pos, CodeBodyTruncated
		}
		c.pos += int(n)
		if c.buf[c.pos] != '\r' || c.buf[c.pos+1] != '\n' {
			return c.pos, CodeChunkMalformed
		}
		c.pos += 2
	}
}

// stepView 推进单条请求的扫描；返回 true 表示该视角扫描结束。
// 供逐请求精算与流式扫描两条路径共用以保证结论一致。
func stepView(c *cursor, v *View, maxReq int, emit func(Boundary)) bool {
	if c.eof() {
		return true
	}
	if v.Requests >= maxReq {
		if v.Code == CodeOK {
			v.Code = CodeRequestBudget
			v.CodeOffset = c.pos
		}
		return true
	}
	b := scanNext(c, v.Requests)
	if b.Code != CodeOK && v.Code == CodeOK {
		v.Code = b.Code
		v.CodeOffset = b.Start
		v.CodeField = b.Field
	}
	if b.End < 0 {
		return true
	}
	v.Requests++
	if emit != nil {
		emit(b)
	}
	return false
}

// ParseView 逐请求精算：完整物化每条请求的边界。
func ParseView(buf []byte, maxReq int) View {
	v := View{Code: CodeOK, CodeOffset: -1}
	c := &cursor{buf: buf}
	for !stepView(c, &v, maxReq, func(b Boundary) {
		v.Boundaries = append(v.Boundaries, b)
	}) {
	}
	return v
}
