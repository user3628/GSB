//go:build !windows

package store

import "time"

func sleepBeforeRetry(i int) {
	time.Sleep(time.Duration(5*(i+1)) * time.Millisecond)
}
