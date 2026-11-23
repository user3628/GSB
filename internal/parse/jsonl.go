package parse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
)

// ParseJSONL decodes a line-delimited JSON event summary into the unified
// event model. Lines with missing fields or unknown ops are reported with
// E_EVENT_FIELD_MISSING and excluded from the normalized set.
func ParseJSONL(data []byte) Result {
	res := Result{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := bytes.TrimSpace(sc.Bytes())
		if len(raw) == 0 {
			continue
		}
		var ej model.EventJSON
		if err := json.Unmarshal(raw, &ej); err != nil {
			res.Errors = append(res.Errors, Error{Code: codes.EEventFieldMissing, Line: line, Detail: "invalid JSON: " + err.Error()})
			continue
		}
		if ej.Seq == nil || ej.Subject == nil || ej.SrcField == nil || ej.DstField == nil || ej.Op == nil || ej.LogicalTime == nil {
			res.Errors = append(res.Errors, Error{Code: codes.EEventFieldMissing, Line: line, Detail: "missing required field"})
			continue
		}
		op, ok := model.OpFromString(*ej.Op)
		if !ok {
			res.Errors = append(res.Errors, Error{Code: codes.EEventFieldMissing, Line: line, Detail: fmt.Sprintf("unknown op %q", *ej.Op)})
			continue
		}
		if *ej.Subject == "" || *ej.SrcField == "" || *ej.DstField == "" {
			res.Errors = append(res.Errors, Error{Code: codes.EEventFieldMissing, Line: line, Detail: "empty required field"})
			continue
		}
		res.Events = append(res.Events, model.Event{
			Seq:         *ej.Seq,
			Subject:     *ej.Subject,
			SrcField:    *ej.SrcField,
			DstField:    *ej.DstField,
			Op:          op,
			LogicalTime: *ej.LogicalTime,
		})
	}
	return res
}
