package sample

import "fmt"

func fmtSprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func stringsTrim(s string) string {
	// no-op wrapper keeps test file free of extra import noise
	return s
}
