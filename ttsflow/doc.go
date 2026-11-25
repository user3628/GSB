// Package ttsflow implements a deterministic, streaming, checkpointed TTS
// domain component.
//
// The pipeline has four stages: text frontend, acoustic prediction, waveform
// synthesis, and consistency verification. Every indivisible pronunciation unit
// advances through those stages in SQLite. A process can stop after any complete
// unit or be killed at any point; recovery compares persisted stage
// fingerprints with a deterministic plan, reuses the valid prefix, and rebuilds
// the earliest affected stage and all downstream artifacts.
//
// The package has no real-model, GPU, network, or CGO dependency. Acoustic
// frames and 16-bit PCM are derived entirely from the supplied configuration,
// text samples, and fixed-seed hashes.
package ttsflow
