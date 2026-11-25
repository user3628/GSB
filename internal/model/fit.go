package model

import (
	"math"

	"regworkbench/internal/canonical"
	"regworkbench/internal/workerr"
)

type Point struct {
	Lambda        float64 `json:"lambda"`
	L1Ratio       float64 `json:"l1_ratio"`
	Unregularized bool    `json:"unregularized"`
}

type Iteration struct {
	Iteration  int     `json:"iteration"`
	Objective  float64 `json:"objective"`
	CoefChange float64 `json:"coefficient_change"`
	StepSize   float64 `json:"step_size"`
	L2Change   float64 `json:"l2_change"`
}

type FitResult struct {
	Point
	Fold              int         `json:"fold"`
	Coefficients      []float64   `json:"coefficients"`
	Intercept         float64     `json:"intercept"`
	Iterations        int         `json:"iterations"`
	Converged         bool        `json:"converged"`
	Trajectory        []Iteration `json:"trajectory"`
	FinalObjective    float64     `json:"final_objective"`
	CoefficientDigest string      `json:"coefficient_digest"`
}

type FitOptions struct {
	Fold           int
	Lambda         float64
	L1Ratio        float64
	MaxIterations  int
	Tolerance      float64
	StopAfterRound func(round int) bool
	OnRound        func(iteration Iteration, trajectory []Iteration)
}

func Fit(X [][]float64, y []float64, opts FitOptions) (*FitResult, bool, error) {
	n := len(X)
	if n == 0 {
		return nil, false, workerr.InvalidFold(opts.Fold, "empty training matrix")
	}
	p := len(X[0])
	lambda := opts.Lambda
	alpha := opts.L1Ratio
	if lambda == 0 {
		alpha = 0
	}
	l2Penalty := lambda * (1 - alpha)
	step := lipschitzStep(X, n, p, l2Penalty)

	x := make([]float64, p)
	z := make([]float64, p)
	xNext := make([]float64, p)
	grad := make([]float64, p)
	var b, zb, bNext float64
	var gradB float64
	t := 1.0
	trajectory := make([]Iteration, 0, opts.MaxIterations)
	lastObjective := objectiveAt(X, y, x, b, lambda, alpha)
	if !finite(lastObjective) {
		return nil, false, workerr.Numeric(opts.Fold, 0, "objective")
	}
	converged := false
	stopped := false

	for iter := 1; iter <= opts.MaxIterations; iter++ {
		gradientAt(X, y, z, zb, l2Penalty, grad, &gradB)
		for j := 0; j < p; j++ {
			xNext[j] = softThreshold(z[j]-step*grad[j], step*lambda*alpha)
		}
		bNext = zb - step*gradB
		obj := objectiveAt(X, y, xNext, bNext, lambda, alpha)
		if !finite(obj) || !finiteSlice(xNext) || !finiteScalar(bNext) {
			return nil, false, workerr.Numeric(opts.Fold, iter, "coefficients_or_objective")
		}

		restarted := false
		if obj > lastObjective+1e-12 {
			copy(z, x)
			zb = b
			t = 1
			gradientAt(X, y, z, zb, l2Penalty, grad, &gradB)
			for j := 0; j < p; j++ {
				xNext[j] = softThreshold(z[j]-step*grad[j], step*lambda*alpha)
			}
			bNext = zb - step*gradB
			obj = objectiveAt(X, y, xNext, bNext, lambda, alpha)
			restarted = true
			if !finite(obj) || !finiteSlice(xNext) {
				return nil, false, workerr.Numeric(opts.Fold, iter, "restart_coefficients")
			}
		}

		change := coefficientDistance(x, xNext, b, bNext)
		l2Change := l2Distance(x, xNext, b, bNext)
		deltaB := bNext - b
		if !restarted {
			tNext := (1 + math.Sqrt(1+4*t*t)) / 2
			momentum := (t - 1) / tNext
			for j := 0; j < p; j++ {
				delta := xNext[j] - x[j]
				z[j] = xNext[j] + momentum*delta
			}
			zb = bNext + momentum*deltaB
			t = tNext
		} else {
			copy(z, xNext)
			zb = bNext
		}
		copy(x, xNext)
		b = bNext

		trajectory = append(trajectory, Iteration{Iteration: iter, Objective: obj, CoefChange: change, L2Change: l2Change, StepSize: step})
		if opts.OnRound != nil {
			opts.OnRound(trajectory[len(trajectory)-1], trajectory)
		}
		lastObjective = obj
		if change < opts.Tolerance && iter >= 8 {
			converged = true
			if opts.StopAfterRound != nil && opts.StopAfterRound(iter) {
				stopped = true
				break
			}
			break
		}
		if opts.StopAfterRound != nil && opts.StopAfterRound(iter) {
			stopped = true
			break
		}
	}

	coefs := append([]float64(nil), x...)
	digest, err := coefficientDigest(coefs, b)
	if err != nil {
		return nil, false, err
	}
	res := &FitResult{
		Point: Point{Lambda: opts.Lambda, L1Ratio: opts.L1Ratio, Unregularized: opts.Lambda == 0},
		Fold:  opts.Fold, Coefficients: coefs, Intercept: b,
		Iterations: len(trajectory), Converged: converged, Trajectory: trajectory,
		FinalObjective: lastObjective, CoefficientDigest: digest,
	}
	return res, stopped, nil
}

func lipschitzStep(X [][]float64, n, p int, l2Penalty float64) float64 {
	sumSq := float64(n)
	for i := 0; i < n; i++ {
		for j := 0; j < p; j++ {
			sumSq += X[i][j] * X[i][j]
		}
	}
	L := 0.25*sumSq/float64(n) + l2Penalty
	if L < 1e-12 {
		L = 1
	}
	return 0.95 / L
}

func gradientAt(X [][]float64, y, w []float64, b, l2Penalty float64, grad []float64, gradB *float64) {
	n := len(X)
	p := len(w)
	for j := 0; j < p; j++ {
		grad[j] = l2Penalty * w[j]
	}
	var gb float64
	for i := 0; i < n; i++ {
		z := b
		row := X[i]
		for j := 0; j < p; j++ {
			z += row[j] * w[j]
		}
		err := (sigmoid(z) - y[i]) / float64(n)
		gb += err
		for j := 0; j < p; j++ {
			grad[j] += row[j] * err
		}
	}
	*gradB = gb
}

func objectiveAt(X [][]float64, y, w []float64, b, lambda, alpha float64) float64 {
	n := len(X)
	p := len(w)
	var loss float64
	for i := 0; i < n; i++ {
		z := b
		row := X[i]
		for j := 0; j < p; j++ {
			z += row[j] * w[j]
		}
		loss += softplus(z) - y[i]*z
	}
	obj := loss / float64(n)
	var l1, l2 float64
	for j := 0; j < p; j++ {
		l1 += math.Abs(w[j])
		l2 += w[j] * w[j]
	}
	obj += lambda * (alpha*l1 + (1-alpha)*0.5*l2)
	return obj
}

func softThreshold(v, threshold float64) float64 {
	if v > threshold {
		return v - threshold
	}
	if v < -threshold {
		return v + threshold
	}
	return 0
}

func coefficientDistance(a, b []float64, ab, bb float64) float64 {
	sum := (ab - bb) * (ab - bb)
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum)
}

func l2Distance(a, b []float64, ab, bb float64) float64 {
	sum := (ab - bb) * (ab - bb)
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(a)+1))
}

func coefficientDigest(coefs []float64, intercept float64) (string, error) {
	payload := struct {
		Coefficients []float64 `json:"coefficients"`
		Intercept    float64   `json:"intercept"`
	}{Coefficients: coefs, Intercept: intercept}
	sum, _, err := canonical.SHA256JSON(payload)
	return sum, err
}

func finite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func finiteScalar(v float64) bool { return finite(v) }

func finiteSlice(v []float64) bool {
	for _, x := range v {
		if !finite(x) {
			return false
		}
	}
	return true
}

func Predict(X [][]float64, coefs []float64, intercept float64) []float64 {
	out := make([]float64, len(X))
	for i, row := range X {
		z := intercept
		for j, c := range coefs {
			if j < len(row) {
				z += c * row[j]
			}
		}
		out[i] = sigmoid(z)
	}
	return out
}
