package pipeline

import "strconv"

func floatToString(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
