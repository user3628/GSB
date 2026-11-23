package web

import "embed"

// Assets contains the offline workbench UI.
//
//go:embed index.html app.js core.js render.js styles.css
var Assets embed.FS
