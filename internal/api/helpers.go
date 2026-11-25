package api

import (
	"context"
	"net/url"
	"strconv"
)

func nilContext() context.Context { return context.Background() }

func queryInt(q url.Values, key string) *int {
	v := q.Get(key)
	if v == "" {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return nil
	}
	return &n
}
func queryInt64(q url.Values, key string) *int64 {
	v := q.Get(key)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}
func queryFloat(q url.Values, key string) *float64 {
	v := q.Get(key)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil
	}
	return &n
}
