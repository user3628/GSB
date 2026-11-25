// Package ropechunk provides a deterministic streaming RoPE chunk operator.
//
// The public API exposes fixed-point rotation, high-precision reference
// generation, online softmax scoring, memory planning, the SQLite-backed
// engine, deterministic samples, and the local HTTP handler. The command in
// cmd/ropechunk is a thin client over the same library.
package ropechunk
