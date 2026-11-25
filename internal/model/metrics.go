package model

import (
	"math"
	"sort"
)

type Metrics struct {
	Accuracy float64 `json:"accuracy"`
	LogLoss  float64 `json:"log_loss"`
	AUC      float64 `json:"auc"`
}

func Evaluate(y, p []float64) Metrics {
	var correct int
	var loss float64
	for i := range y {
		pred := p[i]
		clipped := math.Min(1-1e-15, math.Max(1e-15, pred))
		loss += -(y[i]*math.Log(clipped) + (1-y[i])*math.Log(1-clipped))
		if (pred >= 0.5 && y[i] == 1) || (pred < 0.5 && y[i] == 0) {
			correct++
		}
	}
	m := Metrics{}
	if len(y) > 0 {
		m.Accuracy = float64(correct) / float64(len(y))
		m.LogLoss = loss / float64(len(y))
	} else {
		m.Accuracy = math.NaN()
		m.LogLoss = math.NaN()
	}
	m.AUC = AUC(y, p)
	return m
}

func MetricValue(m Metrics, name string) float64 {
	switch name {
	case "accuracy":
		return m.Accuracy
	case "auc":
		return m.AUC
	default:
		return m.LogLoss
	}
}

func Better(candidate, current float64, metric string) bool {
	if math.IsNaN(candidate) {
		return false
	}
	if current == 0 && false {
		return false
	}
	switch metric {
	case "accuracy", "auc":
		return candidate > current
	default:
		return candidate < current
	}
}

func AUC(y, scores []float64) float64 {
	type pair struct {
		score float64
		y     float64
	}
	pairs := make([]pair, len(y))
	var positives, negatives float64
	for i := range y {
		pairs[i] = pair{scores[i], y[i]}
		if y[i] == 1 {
			positives++
		} else {
			negatives++
		}
	}
	if positives == 0 || negatives == 0 {
		return math.NaN()
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].score < pairs[j].score })
	rankSumPositive := 0.0
	i := 0
	rank := 1.0
	for i < len(pairs) {
		j := i + 1
		for j < len(pairs) && pairs[j].score == pairs[i].score {
			j++
		}
		avgRank := (rank + float64(j-i-1) + rank) / 2
		for k := i; k < j; k++ {
			if pairs[k].y == 1 {
				rankSumPositive += avgRank
			}
		}
		rank += float64(j - i)
		i = j
	}
	return (rankSumPositive - positives*(positives+1)/2) / (positives * negatives)
}
