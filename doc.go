// Package tokenforge implements multi-tenant access/refresh token issuance,
// atomic rotation, token-family revocation, fixed-point quota accounting, and
// versioned SQLite snapshots for SaaS open-platform hosts.
//
// Monetary and quota values are int64 fixed point with Scale=1e9. Business
// failures expose stable codes through CodeOf.
package tokenforge
