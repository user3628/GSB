package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Frame accepts both the compact shorthand ("handler") and the structured form
// ({"id":"handler","name":"handler","file":"api.go","line":12}). The compact
// form is useful for deterministic fixtures while the structured form is useful
// for real profilers.
type Frame struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
	File string `json:"file,omitempty"`
	Line int    `json:"line,omitempty"`
}

func (f *Frame) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return fmt.Errorf("frame must not be empty")
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		f.ID, f.Name = s, s
		return nil
	}
	type frameAlias Frame
	var a frameAlias
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("frame must be a string or an object: %w", err)
	}
	*f = Frame(a)
	return nil
}

func (f *Frame) Normalize() error {
	f.ID = strings.TrimSpace(f.ID)
	f.Name = strings.TrimSpace(f.Name)
	f.File = strings.TrimSpace(f.File)
	if f.ID == "" {
		f.ID = f.Name
	}
	if f.Name == "" {
		f.Name = f.ID
	}
	if f.ID == "" {
		return fmt.Errorf("frame id/name must not be empty")
	}
	if f.Line < 0 {
		return fmt.Errorf("frame line must not be negative")
	}
	return nil
}

func (f Frame) MarshalJSON() ([]byte, error) {
	type frameAlias Frame
	return json.Marshal(frameAlias(f))
}
