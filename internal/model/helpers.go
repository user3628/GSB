package model

import "math"

// InputDimRoot returns the square image side length implied by the flattened input.
func (d *Dataset) InputDimRoot() int {
	return int(math.Sqrt(float64(d.InputDim)))
}
