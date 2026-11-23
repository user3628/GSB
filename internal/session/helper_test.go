package session_test

import (
	"depverify/internal/report"
	"depverify/internal/session"
)

func reportBytes(results []session.RunResult) ([]byte, error) {
	return report.FromResults(results, nil).Marshal()
}
