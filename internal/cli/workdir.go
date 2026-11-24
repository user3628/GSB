package cli

import "os"

// resetWorkdir removes the database and reports from a previous run so each
// deterministic demo starts from an identical, empty state. The deterministic
// params.json is regenerated separately by NewApp.
func resetWorkdir(app *App) {
	_ = os.Remove(app.DBPath)
	_ = os.Remove(app.DBPath + "-wal")
	_ = os.Remove(app.DBPath + "-shm")
	entries, _ := os.ReadDir(reportsDir(app))
	for _, e := range entries {
		_ = os.Remove(reportsDir(app) + string(os.PathSeparator) + e.Name())
	}
}
