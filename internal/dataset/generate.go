package dataset

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"regworkbench/internal/canonical"
)

const DefaultSeed int64 = 20261125

type sizeSpec struct {
	key          string
	file         string
	rows         int
	columns      int
	numeric      int
	categorical  int
	scale        string
	cardinality  int
	missingEvery int
}

func sizeSpecs() []sizeSpec {
	return []sizeSpec{
		{key: "small", file: "regularized_small.csv", rows: 600, columns: 12, numeric: 6, categorical: 5, scale: "small_about_600_rows_12_columns", cardinality: 4, missingEvery: 3},
		{key: "medium", file: "regularized_medium.csv", rows: 6000, columns: 40, numeric: 24, categorical: 15, scale: "medium_about_6000_rows_40_columns", cardinality: 5, missingEvery: 4},
	}
}

func GenerateDirectory(dir string, seed int64) (*Manifest, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	manifest := &Manifest{Version: "regworkbench.manifest/v1", Seed: seed, Manifest: ManifestName}
	for _, ss := range sizeSpecs() {
		ds, records, err := generateOne(ss, seed)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(dir, ss.file)
		if err := writeCSV(path, ds.Features, records); err != nil {
			return nil, err
		}
		manifest.Datasets = append(manifest.Datasets, ds)
	}
	if err := SaveManifest(filepath.Join(dir, ManifestName), manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

type generatedRow struct {
	values []string
}

func generateOne(ss sizeSpec, seed int64) (DatasetManifest, [][]string, error) {
	if ss.numeric+ss.categorical+1 != ss.columns {
		return DatasetManifest{}, nil, fmt.Errorf("invalid column split for %s", ss.key)
	}
	rng := rand.New(rand.NewSource(seed + int64(ss.rows)*7 + int64(ss.columns)))
	features := make([]FeatureSpec, 0, ss.numeric+ss.categorical)
	headers := make([]string, 0, ss.columns)

	for i := 0; i < ss.numeric; i++ {
		name := fmt.Sprintf("num_%02d", i+1)
		missing := i%ss.missingEvery == 1
		features = append(features, FeatureSpec{Name: name, Index: i, Semantic: "continuous", DeclaredMissing: missing, DefaultEncoding: "standardize"})
		headers = append(headers, name)
	}
	categoryValues := make([][]string, ss.categorical)
	for j := 0; j < ss.categorical; j++ {
		name := fmt.Sprintf("cat_%02d", j+1)
		card := 3 + (j % (ss.cardinality - 2))
		cats := make([]string, card)
		for c := range cats {
			cats[c] = fmt.Sprintf("%s_v%d", name, c+1)
		}
		categoryValues[j] = cats
		missing := j%ss.missingEvery == 0
		features = append(features, FeatureSpec{Name: name, Index: ss.numeric + j, Semantic: "categorical", DeclaredMissing: missing, DefaultEncoding: "one_hot", Categories: cats})
		headers = append(headers, name)
	}
	headers = append(headers, "label")

	records := make([][]string, ss.rows)
	numericValues := make([]float64, ss.numeric)
	catIndexes := make([]int, ss.categorical)
	for r := 0; r < ss.rows; r++ {
		logit := -0.08
		for i := 0; i < ss.numeric; i++ {
			base := 50 + float64(i*7%31)
			scale := 8 + float64(i%5)*2.5
			v := base + scale*rng.NormFloat64()
			if i%5 == 0 {
				v += 0.004 * float64(r%ss.rows)
			}
			numericValues[i] = v
			effect := 0.34 * math.Sqrt(6.0/float64(ss.numeric+2))
			if i%2 == 0 {
				effect *= -1
			}
			logit += effect * (v - base) / scale
		}
		for j := 0; j < ss.categorical; j++ {
			idx := rng.Intn(len(categoryValues[j]))
			catIndexes[j] = idx
			effect := 0.28 * math.Sqrt(5.0/float64(ss.categorical+1))
			logit += effect * float64(idx-(len(categoryValues[j])-1)/2)
		}
		if rng.Float64() < 0.08 {
			logit += 0.45 * rng.NormFloat64()
		}
		prob := 1.0 / (1.0 + math.Exp(-logit))
		label := 0
		if rng.Float64() < prob {
			label = 1
		}

		row := make([]string, ss.columns)
		for i := 0; i < ss.numeric; i++ {
			missing := features[i].DeclaredMissing && deterministicMissing(rng, i, r)
			if missing {
				row[i] = ""
			} else {
				row[i] = strconv.FormatFloat(numericValues[i], 'f', 6, 64)
			}
		}
		for j := 0; j < ss.categorical; j++ {
			col := ss.numeric + j
			missing := features[col].DeclaredMissing && deterministicMissing(rng, col, r)
			if missing {
				row[col] = ""
			} else {
				row[col] = categoryValues[j][catIndexes[j]]
			}
		}
		row[ss.columns-1] = strconv.Itoa(label)
		records[r] = row
	}

	spec := DatasetManifest{
		Key: ss.key, File: filepath.Base(ss.file), Rows: ss.rows, Columns: ss.columns,
		FeatureColumns: ss.columns - 1, LabelColumn: "label", Seed: seed,
		Generator: "regworkbench.deterministic.v1", Scale: ss.scale, Features: features,
	}
	return spec, records, nil
}

func deterministicMissing(rng *rand.Rand, col, row int) bool {
	rate := 0.055 + float64(col%3)*0.015
	if row%17 == 0 {
		rate += 0.02
	}
	return rng.Float64() < rate
}

func writeCSV(path string, features []FeatureSpec, records [][]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	header := make([]string, 0, len(features)+1)
	for _, feature := range features {
		header = append(header, feature.Name)
	}
	header = append(header, "label")
	if err := w.Write(header); err != nil {
		return err
	}
	if err := w.WriteAll(records); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func (m *Manifest) Fingerprint() (string, error) {
	b, err := canonical.JSON(m)
	if err != nil {
		return "", err
	}
	return canonical.SHA256Bytes(b), nil
}
