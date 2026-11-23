package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

func newID(prefix string) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(buf)
}

func digestString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func fingerprintOf(operation string, payload any) string {
	data, _ := json.Marshal(payload)
	return digestString(operation + "\x00" + string(data))
}

func stringPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func int64Ptr(v int64) *int64 { return &v }
