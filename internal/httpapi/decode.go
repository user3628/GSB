package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"configtree/internal/engine"
	"configtree/internal/spec"
)

func decodeBody(r *http.Request, req *engine.CommitRequest) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errSpec("empty request body")
	}
	var envelope struct {
		CommitID string          `json:"commit_id"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return errSpec("invalid JSON body: " + err.Error())
	}
	if envelope.CommitID == "" {
		return errSpec("commit_id is required")
	}
	if len(envelope.Payload) == 0 {
		return errSpec("payload is required")
	}
	req.CommitID = envelope.CommitID
	req.RawBody = envelope.Payload
	return nil
}

func reencode(raw json.RawMessage, dst any) error {
	if !json.Valid(raw) {
		return errSpec("payload is not valid JSON")
	}
	return json.Unmarshal(raw, dst)
}

func queryInt64(r *http.Request, name string) (int64, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, errSpec(name + " is required")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, errSpec(name + " must be a non-negative integer")
	}
	return n, nil
}

func pathInt64(r *http.Request, name string) (int64, error) {
	n, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func errSpec(msg string) error {
	return spec.NewError(spec.CodeSpecInvalid, msg, nil)
}
