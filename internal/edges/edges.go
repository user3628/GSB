package edges

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"depverify/internal/model"
)

type rawEdge struct {
	Parent     string `json:"parent"`
	Child      string `json:"child"`
	Constraint []int  `json:"constraint"`
}

// Parse 逐行解析依赖边 JSON，跳过空行，保留原始行号。
func Parse(r io.Reader) ([]model.Edge, []model.ParseIssue) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var out []model.Edge
	var issues []model.ParseIssue
	line := 0
	for scanner.Scan() {
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		var re rawEdge
		if err := json.Unmarshal(raw, &re); err != nil {
			issues = append(issues, model.ParseIssue{
				Code: model.EEdgeMalformed, Line: line, Field: "line",
				Message: fmt.Sprintf("invalid json: %v", err),
			})
			continue
		}
		if re.Parent == "" {
			issues = append(issues, model.ParseIssue{
				Code: model.EEdgeMalformed, Line: line, Field: "parent",
				Message: "parent is required",
			})
			continue
		}
		if re.Child == "" {
			issues = append(issues, model.ParseIssue{
				Code: model.EEdgeMalformed, Line: line, Field: "child",
				Message: "child is required",
			})
			continue
		}
		if len(re.Constraint) == 0 {
			issues = append(issues, model.ParseIssue{
				Code: model.EEdgeMalformed, Line: line, Field: "constraint",
				Message: "constraint version must be a non-empty integer array",
			})
			continue
		}
		for _, v := range re.Constraint {
			if v < 0 {
				issues = append(issues, model.ParseIssue{
					Code: model.EEdgeMalformed, Line: line, Field: "constraint",
					Message: "constraint segment must be non-negative",
				})
				goto nextLine
			}
		}
		out = append(out, model.Edge{
			Line:       line,
			Parent:     model.NormalizeKey(re.Parent),
			Child:      model.NormalizeKey(re.Child),
			Constraint: append([]int(nil), re.Constraint...),
		})
	nextLine:
	}
	return out, issues
}

// CheckGraph 校验重复边与悬挂边，known 为清单中存在的组件键集合。
// 行号与字段名随问题一并记录。
func CheckGraph(edgeList []model.Edge, known map[string]struct{}) ([]model.ParseIssue, map[model.EdgeKey]model.Edge) {
	var issues []model.ParseIssue
	unique := make(map[model.EdgeKey]model.Edge)
	keys := make([]model.EdgeKey, 0, len(edgeList))
	for _, e := range edgeList {
		key := model.EdgeKey{Parent: e.Parent, Child: e.Child}
		if first, dup := unique[key]; dup {
			issues = append(issues, model.ParseIssue{
				Code: model.EDupEdge, Line: e.Line, Field: "parent+child",
				Message: fmt.Sprintf("duplicate edge %s -> %s, first seen at line %d", e.Parent, e.Child, first.Line),
			})
			continue
		}
		unique[key] = e
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].Parent != keys[j].Parent {
			return keys[i].Parent < keys[j].Parent
		}
		return keys[i].Child < keys[j].Child
	})
	for _, key := range keys {
		e := unique[key]
		if _, ok := known[e.Parent]; !ok {
			issues = append(issues, model.ParseIssue{
				Code: model.EEdgeDangling, Line: e.Line, Field: "parent",
				Message: fmt.Sprintf("parent %s not present in manifest", e.Parent),
			})
		}
		if _, ok := known[e.Child]; !ok {
			issues = append(issues, model.ParseIssue{
				Code: model.EEdgeDangling, Line: e.Line, Field: "child",
				Message: fmt.Sprintf("child %s not present in manifest", e.Child),
			})
		}
	}
	return issues, unique
}
