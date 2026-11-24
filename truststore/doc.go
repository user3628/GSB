// Package truststore implements a local, deterministic PKI trust-management
// domain component.
//
// The package has four layers:
//
//   - X.509 chain reconstruction and validation using only explicit trust
//     anchors supplied in a SnapshotContent. The operating-system root pool is
//     never consulted.
//   - An immutable, versioned trust snapshot model: trusted roots, trusted
//     intermediates, revocation/supersession sets, named trust scopes and host
//     chain samples.
//   - Incremental change sets applied atomically to SQLite with monotonic parent
//     links, dependency closure calculation and a retained replay history.
//   - Deterministic JSON verdict reports with stable ordering for review and
//     integration testing.
//
// A typical host initializes a namespace with Store.Bootstrap, applies updates
// with Store.Apply, validates named chains with Store.Verify and exports its
// audit view with Store.ExportReportJSON. All failures expose stable values
// from Code via CodeOf.
package truststore
