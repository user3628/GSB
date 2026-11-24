// Package samples generates the deterministic local sample tree used
// for verification and fault injection. Generation is purely
// deterministic: no clock, no randomness.
package samples

import (
	"fmt"
	"os"
	"path/filepath"
)

const hqJSON = `{
  "snapshot": "hq",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8080, "port_hi": 8080, "action": "allow", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8000, "port_hi": 9000, "action": "deny", "priority": 50},
    {"index": 3, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

const branchAJSON = `{
  "snapshot": "branch-a",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8080, "port_hi": 8080, "action": "allow", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8000, "port_hi": 9000, "action": "deny", "priority": 50},
    {"index": 3, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

// branch-b rewrites rule 1 dst CIDR and rule 2 port range: probes p1
// and p3 diverge against hq.
const branchBJSON = `{
  "snapshot": "branch-b",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.2.0/24", "port_lo": 8080, "port_hi": 8080, "action": "allow", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 9000, "port_hi": 9100, "action": "deny", "priority": 50},
    {"index": 3, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

// branch-c flips rule 1 action: probe p1 diverges on field "action".
const branchCJSON = `{
  "snapshot": "branch-c",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8080, "port_hi": 8080, "action": "deny", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8000, "port_hi": 9000, "action": "deny", "priority": 50},
    {"index": 3, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

const probesJSON = `{
  "probes": [
    {"id": "p1", "direction": "in", "protocol": "tcp", "src": "10.0.0.5", "dst": "10.0.1.9", "port": 8080, "weight": "2.5", "count": "4"},
    {"id": "p2", "direction": "in", "protocol": "tcp", "src": "10.0.0.6", "dst": "10.0.1.20", "port": 8500, "weight": "1.0000005", "count": "2"},
    {"id": "p3", "direction": "in", "protocol": "tcp", "src": "10.0.0.7", "dst": "10.0.2.9", "port": 8080, "weight": "3", "count": "1"}
  ]
}
`

const badCIDRJSON = `{
  "snapshot": "bad-cidr",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 80, "port_hi": 80, "action": "allow", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/33", "dst": "10.0.1.0/24", "port_lo": 443, "port_hi": 443, "action": "deny", "priority": 90},
    {"index": 3, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

const portInvertJSON = `{
  "snapshot": "port-invert",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 9000, "port_hi": 8000, "action": "allow", "priority": 100},
    {"index": 2, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

const ovfJSON = `{
  "snapshot": "ovf",
  "rules": [
    {"index": 1, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "allow", "priority": 10}
  ]
}
`

const ovfProbesJSON = `{
  "probes": [
    {"id": "ovf1", "direction": "in", "protocol": "tcp", "src": "10.0.0.5", "dst": "10.0.1.9", "port": 8080, "weight": "5000000000", "count": "5000000000"}
  ]
}
`

const conflictJSON = `{
  "snapshot": "conflict",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8080, "port_hi": 8080, "action": "allow", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 8080, "port_hi": 8080, "action": "deny", "priority": 100},
    {"index": 3, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

const replaceV1JSON = `{
  "snapshot": "repl",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "10.0.1.0/24", "port_lo": 80, "port_hi": 80, "action": "allow", "priority": 100},
    {"index": 2, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}
`

const replaceV2JSON = `{
  "snapshot": "repl",
  "rules": [
    {"index": 7, "direction": "out", "protocol": "udp", "src": "192.168.0.0/16", "dst": "172.16.0.0/12", "port_lo": 53, "port_hi": 53, "action": "allow", "priority": 200},
    {"index": 9, "direction": "out", "protocol": "icmp", "src": "192.168.0.0/16", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 0, "action": "deny", "priority": 150}
  ]
}
`

// blk encodes one length-prefixed text block.
func blk(payload string) string {
	return fmt.Sprintf("%08d%s", len(payload), payload)
}

// Blocks returns the deterministic length-prefixed text block stream:
// two valid rules plus one with an unsupported protocol.
func Blocks() []byte {
	r1 := `{"index":1,"direction":"in","protocol":"tcp","src":"10.0.0.0/8","dst":"10.0.1.0/24","port_lo":22,"port_hi":22,"action":"allow","priority":100}`
	r2 := `{"index":2,"direction":"in","protocol":"sctp","src":"10.0.0.0/8","dst":"10.0.1.0/24","port_lo":9,"port_hi":9,"action":"deny","priority":90}`
	r3 := `{"index":3,"direction":"any","protocol":"any","src":"0.0.0.0/0","dst":"0.0.0.0/0","port_lo":0,"port_hi":65535,"action":"deny","priority":1}`
	return []byte(blk(r1) + blk(r2) + blk(r3))
}

// BranchDBlocks is a fully valid block stream semantically identical
// to the hq snapshot, used to exercise the text-block source in the
// normal scenario.
func BranchDBlocks() []byte {
	r1 := `{"index":1,"direction":"in","protocol":"tcp","src":"10.0.0.0/8","dst":"10.0.1.0/24","port_lo":8080,"port_hi":8080,"action":"allow","priority":100}`
	r2 := `{"index":2,"direction":"in","protocol":"tcp","src":"10.0.0.0/8","dst":"10.0.1.0/24","port_lo":8000,"port_hi":9000,"action":"deny","priority":50}`
	r3 := `{"index":3,"direction":"any","protocol":"any","src":"0.0.0.0/0","dst":"0.0.0.0/0","port_lo":0,"port_hi":65535,"action":"deny","priority":1}`
	return []byte(blk(r1) + blk(r2) + blk(r3))
}

var files = map[string]string{
	"normal/hq.json":         hqJSON,
	"normal/branch-a.json":   branchAJSON,
	"normal/branch-b.json":   branchBJSON,
	"normal/branch-c.json":   branchCJSON,
	"normal/probes.json":     probesJSON,
	"bad-cidr/bad.json":      badCIDRJSON,
	"port-invert/inv.json":   portInvertJSON,
	"overflow/ovf.json":      ovfJSON,
	"overflow/probes.json":   ovfProbesJSON,
	"conflict/conflict.json": conflictJSON,
	"replace/v1.json":        replaceV1JSON,
	"replace/v2.json":        replaceV2JSON,
}

// Generate writes the deterministic sample tree under dir.
func Generate(dir string) error {
	for rel, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "textblk"), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "textblk", "hq.blk"), Blocks(), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "textblk", "branch-d.blk"), BranchDBlocks(), 0o644)
}
