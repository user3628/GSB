package model

import "time"

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

type NodeConfig struct {
	ID       string `json:"id"`
	OriginID string `json:"origin_id"`
	Priority int    `json:"priority"`
}

type Config struct {
	ListenAddr        string       `json:"listen_addr"`
	DBPath            string       `json:"db_path"`
	OriginRoot        string       `json:"origin_root"`
	TagsFile          string       `json:"tags_file"`
	OriginTimeoutMs   int64        `json:"origin_timeout_ms"`
	RevalidateTimeout int64        `json:"revalidate_timeout_ms"`
	InitialPolicy     string       `json:"initial_policy"`
	RecoveryReport    string       `json:"recovery_report"`
	Nodes             []NodeConfig `json:"nodes"`
	AllowDebugCrash   bool         `json:"allow_debug_crash"`
}

func (c Config) Node(id string) (NodeConfig, bool) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeConfig{}, false
}
