package cv

import (
	"math"
	"math/rand"
	"sort"

	"regworkbench/internal/canonical"
	"regworkbench/internal/workerr"
)

type LabelDistribution struct {
	Count0       int     `json:"count_0"`
	Count1       int     `json:"count_1"`
	Total        int     `json:"total"`
	PositiveRate float64 `json:"positive_rate"`
}

type Fold struct {
	Index                  int               `json:"index"`
	Train                  []int             `json:"train"`
	Validation             []int             `json:"validation"`
	TrainDistribution      LabelDistribution `json:"train_distribution"`
	ValidationDistribution LabelDistribution `json:"validation_distribution"`
	OverallPositiveRate    float64           `json:"overall_positive_rate"`
	TrainValidationGap     float64           `json:"train_validation_gap"`
	DistributionShiftAbs   float64           `json:"distribution_shift_abs"`
	Fingerprint            string            `json:"fingerprint"`
}

type Split struct {
	KFolds              int     `json:"k_folds"`
	Seed                int64   `json:"seed"`
	SampleFingerprint   string  `json:"sample_fingerprint"`
	ConfigVersion       string  `json:"config_version"`
	OverallPositiveRate float64 `json:"overall_positive_rate"`
	Folds               []*Fold `json:"folds"`
	Fingerprint         string  `json:"fingerprint"`
}

func Stratified(y []float64, k int, seed int64, sampleFingerprint, configVersion string) (*Split, error) {
	if k < 2 {
		return nil, workerr.InvalidFold(-1, "k_folds_must_be_at_least_2")
	}
	byClass := map[int][]int{0: {}, 1: {}}
	positives := 0
	for i, v := range y {
		class := 0
		if v == 1 {
			class = 1
			positives++
		}
		byClass[class] = append(byClass[class], i)
	}
	overall := float64(positives) / float64(len(y))
	rng := rand.New(rand.NewSource(seed + int64(k)*1000003))

	buckets := make([][]int, k)
	for class := 0; class <= 1; class++ {
		indices := append([]int(nil), byClass[class]...)
		perm := rng.Perm(len(indices))
		shuffled := make([]int, len(indices))
		for out, p := range perm {
			shuffled[out] = indices[p]
		}
		for pos, rowIdx := range shuffled {
			fold := pos % k
			buckets[fold] = append(buckets[fold], rowIdx)
		}
	}

	split := &Split{KFolds: k, Seed: seed, SampleFingerprint: sampleFingerprint, ConfigVersion: configVersion, OverallPositiveRate: overall}
	trainUsed := map[int]bool{}
	valUsed := map[int]bool{}
	for f := 0; f < k; f++ {
		val := append([]int(nil), buckets[f]...)
		sort.Ints(val)
		valSet := map[int]bool{}
		for _, idx := range val {
			valSet[idx] = true
			if valUsed[idx] {
				return nil, workerr.InvalidFold(f, "duplicate_validation_index")
			}
			valUsed[idx] = true
		}
		train := make([]int, 0, len(y)-len(val))
		for idx := 0; idx < len(y); idx++ {
			if !valSet[idx] {
				train = append(train, idx)
				if trainUsed[idx] && false {
					// train index repeats across folds by design, so this map is not global uniqueness.
				}
			}
		}
		sort.Ints(train)
		foldObj := &Fold{Index: f, Train: train, Validation: val, OverallPositiveRate: overall}
		foldObj.TrainDistribution = distribution(y, train)
		foldObj.ValidationDistribution = distribution(y, val)
		foldObj.TrainValidationGap = round9(foldObj.TrainDistribution.PositiveRate - foldObj.ValidationDistribution.PositiveRate)
		foldObj.DistributionShiftAbs = math.Abs(foldObj.TrainValidationGap)
		if foldObj.TrainDistribution.Count0 == 0 || foldObj.TrainDistribution.Count1 == 0 {
			return nil, workerr.InvalidFold(f, "train_contains_single_class")
		}
		if foldObj.ValidationDistribution.Count0 == 0 || foldObj.ValidationDistribution.Count1 == 0 {
			return nil, workerr.InvalidFold(f, "validation_contains_single_class")
		}
		fpPayload := struct {
			Sample string `json:"sample"`
			K      int    `json:"k"`
			Seed   int64  `json:"seed"`
			Fold   int    `json:"fold"`
			Train  []int  `json:"train"`
			Val    []int  `json:"validation"`
		}{sampleFingerprint, k, seed, f, train, val}
		fp, _, err := canonical.SHA256JSON(fpPayload)
		if err != nil {
			return nil, err
		}
		foldObj.Fingerprint = fp
		split.Folds = append(split.Folds, foldObj)
	}
	if len(valUsed) != len(y) {
		return nil, workerr.InvalidFold(-1, "fold_assignments_do_not_cover_all_rows")
	}
	fp, _, err := canonical.SHA256JSON(struct {
		Sample string  `json:"sample"`
		K      int     `json:"k"`
		Seed   int64   `json:"seed"`
		Folds  []*Fold `json:"folds"`
	}{sampleFingerprint, k, seed, split.Folds})
	if err != nil {
		return nil, err
	}
	split.Fingerprint = fp
	return split, nil
}

func distribution(y []float64, idx []int) LabelDistribution {
	d := LabelDistribution{Total: len(idx)}
	for _, i := range idx {
		if y[i] == 1 {
			d.Count1++
		} else {
			d.Count0++
		}
	}
	if d.Total > 0 {
		d.PositiveRate = round9(float64(d.Count1) / float64(d.Total))
	}
	return d
}

func round9(v float64) float64 {
	return math.Round(v*1e9) / 1e9
}

type CacheKey struct {
	SampleFingerprint string `json:"sample_fingerprint"`
	ConfigVersion     string `json:"config_version"`
	Fold              int    `json:"fold"`
	Kind              string `json:"kind"`
	Point             string `json:"point,omitempty"`
}

func (k CacheKey) String() string {
	sum, _, err := canonical.SHA256JSON(k)
	if err != nil {
		panic(err)
	}
	return sum
}
