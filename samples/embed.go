// Package samples embeds the deterministic local acceptance fixtures so the
// workbench can list and load them without network or filesystem dependencies.
package samples

import "embed"

//go:embed batches/*.json
var FS embed.FS
