// Package model defines the normalized internal rule and probe model.
package model

import (
	"fmt"

	"fwadjudge/internal/fixed"
)

type Direction string
type Protocol string
type Action string

const (
	DirIn  Direction = "in"
	DirOut Direction = "out"
	DirAny Direction = "any"
)

const (
	ProtoTCP  Protocol = "tcp"
	ProtoUDP  Protocol = "udp"
	ProtoICMP Protocol = "icmp"
	ProtoAny  Protocol = "any"
)

const (
	ActionAllow Action = "allow"
	ActionDeny  Action = "deny"
)

func ValidDirection(d string) bool {
	switch Direction(d) {
	case DirIn, DirOut, DirAny:
		return true
	}
	return false
}

func ValidProtocol(p string) bool {
	switch Protocol(p) {
	case ProtoTCP, ProtoUDP, ProtoICMP, ProtoAny:
		return true
	}
	return false
}

func ValidAction(a string) bool {
	switch Action(a) {
	case ActionAllow, ActionDeny:
		return true
	}
	return false
}

// Rule is the normalized internal rule model. The fixed-point and
// 128-bit views are filled by Normalize.
type Rule struct {
	SnapshotID string
	Index      int
	Direction  Direction
	Protocol   Protocol
	SrcCIDR    string
	DstCIDR    string
	PortLo     int
	PortHi     int
	Action     Action
	Priority   int

	SrcLo, SrcHi fixed.U128
	DstLo, DstHi fixed.U128
	PortLoF      fixed.Fixed
	PortHiF      fixed.Fixed
}

// Normalize computes the integer fixed-point views of the rule. The
// CIDR fields must already have been validated by the parser.
func (r *Rule) Normalize() error {
	var err error
	r.SrcLo, r.SrcHi, err = fixed.ParseCIDR(r.SrcCIDR)
	if err != nil {
		return err
	}
	r.DstLo, r.DstHi, err = fixed.ParseCIDR(r.DstCIDR)
	if err != nil {
		return err
	}
	r.PortLoF = fixed.PortToFixed(r.PortLo)
	r.PortHiF = fixed.PortToFixed(r.PortHi)
	return nil
}

// Canonical returns a deterministic serialization used for hashing.
func (r *Rule) Canonical() string {
	return fmt.Sprintf("%d|%s|%s|%s|%s|%d|%d|%s|%d",
		r.Index, r.Direction, r.Protocol, r.SrcCIDR, r.DstCIDR,
		r.PortLo, r.PortHi, r.Action, r.Priority)
}

// Probe is a single traffic probe.
type Probe struct {
	ID        string
	Direction Direction
	Protocol  Protocol
	SrcIP     string
	DstIP     string
	DstPort   int
	Weight    string // decimal string, parsed to fixed-point
	Count     string // decimal string, parsed to fixed-point
}
