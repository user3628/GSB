// Package spmm implements deterministic, block-structured CSR times dense
// matrix multiplication. Its public pipeline is:
//
//  1. Validate/Load CSR and dense operands and compute fingerprints.
//  2. ChoosePlan selects accumulation-column and row blocks from explicit
//     sparse/cache observations and builds an auditable peak-memory schedule.
//  3. ExecuteRowUnit computes one execution-row unit through every output
//     column chunk and returns values, a deterministic trace and a sampled
//     high-precision error report.
//
// The package does not access SQLite or the network; persistence and the dual
// interactive/settlement paths live in the store and engine packages.
package spmm
