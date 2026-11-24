package cli

import "path/filepath"

func reportsDir(app *App) string {
	return filepath.Join(app.WorkDir, "reports")
}
