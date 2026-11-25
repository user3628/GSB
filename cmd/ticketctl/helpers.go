package main

import (
	"fmt"
	"strconv"
)

func parseFloat(s string) (float64, error) { return strconv.ParseFloat(s, 64) }

func parseInt(s string) (int, error) { return strconv.Atoi(s) }

func parseThreeFloats(args []string) ([]float64, error) {
	out := make([]float64, 3)
	for i := 0; i < 3; i++ {
		v, err := parseFloat(args[i])
		if err != nil {
			return nil, fmt.Errorf("invalid float %q: %w", args[i], err)
		}
		out[i] = v
	}
	return out, nil
}
