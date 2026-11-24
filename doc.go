// Package authlab provides a deterministic, single-process multi-tenant
// authentication and session-isolation engine for local verification.
//
// Session state is stored as a per-tenant hash-linked snapshot chain. Tokens are
// HMAC-signed, refresh tokens are single-use with family-wide replay revocation,
// and cross-tenant operations are rejected before any tenant state is read or
// modified. The bundled sample keys and deterministic clock are lab material,
// not production secrets.
package authlab
