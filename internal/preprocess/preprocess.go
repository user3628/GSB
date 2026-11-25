package preprocess

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"regworkbench/internal/config"
	"regworkbench/internal/dataset"
	"regworkbench/internal/workerr"
)

type ColumnStat struct {
	Name              string   `json:"name"`
	Index             int      `json:"index"`
	Kind              string   `json:"kind"`
	Encoding          string   `json:"encoding,omitempty"`
	Total             int      `json:"total"`
	Present           int      `json:"present"`
	Missing           int      `json:"missing"`
	MissingRate       float64  `json:"missing_rate"`
	NumericParseRatio float64  `json:"numeric_parse_ratio"`
	Mean              float64  `json:"mean,omitempty"`
	Std               float64  `json:"std,omitempty"`
	NumericFill       float64  `json:"numeric_fill,omitempty"`
	CategoricalFill   string   `json:"categorical_fill,omitempty"`
	Mode              string   `json:"mode,omitempty"`
	Categories        []string `json:"categories,omitempty"`
	FeatureNames      []string `json:"feature_names"`
}

type Prepared struct {
	SampleFingerprint string       `json:"sample_fingerprint"`
	ConfigVersion     string       `json:"config_version"`
	RowCount          int          `json:"row_count"`
	Columns           []ColumnStat `json:"columns"`
	FeatureNames      []string     `json:"feature_names"`
	Numeric           [][]float64  `json:"numeric"`
	OneHot            [][]float64  `json:"one_hot"`
	TargetRaw         [][]string   `json:"target_raw,omitempty"`
	TargetColumns     []int        `json:"target_columns,omitempty"`
	Y                 []float64    `json:"y"`
}

func Prepare(ds *dataset.Dataset, cfg config.Config) (*Prepared, error) {
	cfg, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}
	version, err := cfg.Version()
	if err != nil {
		return nil, err
	}
	n := len(ds.Records)
	p := &Prepared{SampleFingerprint: ds.RawSHA, ConfigVersion: version, RowCount: n, Y: make([]float64, n)}
	numericNames := []string{}
	oneHotNames := []string{}
	targetNames := []string{}

	labelIndex := len(ds.Header) - 1
	if ds.Spec.LabelColumn != "" {
		labelIndex = indexOf(ds.Header, ds.Spec.LabelColumn)
		if labelIndex < 0 {
			return nil, workerr.New(workerr.EConfig, "label column not found: "+ds.Spec.LabelColumn)
		}
	}
	for r, row := range ds.Records {
		y, err := strconv.ParseFloat(row[labelIndex], 64)
		if err != nil || (y != 0 && y != 1) {
			return nil, workerr.InvalidFold(-1, fmt.Sprintf("row %d has non-binary label %q", r, row[labelIndex]))
		}
		p.Y[r] = y
	}

	featureSpecs := ds.Spec.Features
	for _, spec := range featureSpecs {
		idx := indexOf(ds.Header, spec.Name)
		if idx < 0 {
			return nil, workerr.New(workerr.EConfig, "feature column not found: "+spec.Name)
		}
		values := columnValues(ds.Records, idx)
		stat := ColumnStat{Name: spec.Name, Index: idx, Total: n, Categories: append([]string(nil), spec.Categories...)}
		for _, raw := range values {
			if raw == "" {
				stat.Missing++
			} else {
				stat.Present++
			}
		}
		if n > 0 {
			stat.MissingRate = float64(stat.Missing) / float64(n)
		}

		declared := spec.Semantic
		if declared == "" {
			declared = detectSemantic(values)
		}
		parsed := numericValues(values)
		if stat.Present > 0 {
			stat.NumericParseRatio = float64(len(parsed)) / float64(stat.Present)
		}
		if declared == "continuous" || (declared != "categorical" && stat.NumericParseRatio == 1) {
			stat.Kind = "continuous"
			if err := prepareNumeric(&stat, values, parsed, cfg); err != nil {
				return nil, err
			}
			stat.Encoding = "standardize"
			stat.FeatureNames = []string{stat.Name}
			numericNames = append(numericNames, stat.FeatureNames...)
		} else {
			stat.Kind = "categorical"
			stat.Encoding = cfg.EncodingFor(spec.Name)
			prepareCategorical(&stat, values, cfg)
			if stat.Encoding == "target" {
				stat.FeatureNames = []string{spec.Name + "__target"}
				targetNames = append(targetNames, stat.FeatureNames...)
				p.TargetColumns = append(p.TargetColumns, len(p.Columns))
				raw := make([]string, n)
				for r := range values {
					raw[r] = fillCategorical(values[r], stat)
				}
				p.TargetRaw = append(p.TargetRaw, raw)
			} else {
				stat.Encoding = "one_hot"
				stat.FeatureNames = make([]string, len(stat.Categories))
				for i, cat := range stat.Categories {
					stat.FeatureNames[i] = spec.Name + "=" + cat
				}
				matrix := make([][]float64, n)
				for r := 0; r < n; r++ {
					matrix[r] = make([]float64, len(stat.Categories))
					v := fillCategorical(values[r], stat)
					pos := sort.SearchStrings(stat.Categories, v)
					if pos < len(stat.Categories) && stat.Categories[pos] == v {
						matrix[r][pos] = 1
					}
				}
				oneHotNames = append(oneHotNames, stat.FeatureNames...)
				p.OneHot = appendOneHotColumns(p.OneHot, matrix)
			}
		}
		p.Columns = append(p.Columns, stat)
	}
	p.Numeric = BuildNumericMatrix(ds, p)
	p.FeatureNames = append(p.FeatureNames, numericNames...)
	p.FeatureNames = append(p.FeatureNames, oneHotNames...)
	p.FeatureNames = append(p.FeatureNames, targetNames...)
	return p, nil
}

func appendOneHotColumns(existing [][]float64, columns [][]float64) [][]float64 {
	if len(existing) == 0 {
		out := make([][]float64, len(columns))
		for i := range columns {
			out[i] = columns[i]
		}
		return out
	}
	for r := range existing {
		existing[r] = append(existing[r], columns[r]...)
	}
	return existing
}

func columnValues(records [][]string, idx int) []string {
	out := make([]string, len(records))
	for i, row := range records {
		if idx < len(row) {
			out[i] = row[idx]
		}
	}
	return out
}

func indexOf(header []string, name string) int {
	for i, h := range header {
		if h == name {
			return i
		}
	}
	return -1
}

func detectSemantic(values []string) string {
	parsed := 0
	present := 0
	for _, v := range values {
		if v == "" {
			continue
		}
		present++
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			parsed++
		}
	}
	if present > 0 && parsed == present {
		return "continuous"
	}
	return "categorical"
}

func numericValues(values []string) []float64 {
	out := make([]float64, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if x, err := strconv.ParseFloat(v, 64); err == nil && isFinite(x) {
			out = append(out, x)
		}
	}
	sort.Float64s(out)
	return out
}

func prepareNumeric(stat *ColumnStat, raw []string, values []float64, cfg config.Config) error {
	if len(values) == 0 {
		stat.Mean = 0
		stat.Std = 1
		stat.NumericFill = 0
		return nil
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	stdSum := 0.0
	for _, v := range values {
		d := v - mean
		stdSum += d * d
	}
	std := math.Sqrt(stdSum / float64(len(values)))
	if std < 1e-12 {
		std = 1
	}
	fill := mean
	switch cfg.ImputeNumeric {
	case "median":
		fill = medianSorted(values)
	case "zero":
		fill = 0
	}
	stat.Mean = roundCanonical(mean)
	stat.Std = roundCanonical(std)
	stat.NumericFill = roundCanonical(fill)
	return nil
}

func BuildNumericMatrix(ds *dataset.Dataset, p *Prepared) [][]float64 {
	out := make([][]float64, len(ds.Records))
	numericStats := make([]ColumnStat, 0)
	for _, c := range p.Columns {
		if c.Kind == "continuous" {
			numericStats = append(numericStats, c)
		}
	}
	for r, row := range ds.Records {
		out[r] = make([]float64, 0, len(numericStats))
		for _, stat := range numericStats {
			v := stat.NumericFill
			if stat.Index < len(row) && row[stat.Index] != "" {
				if parsed, err := strconv.ParseFloat(row[stat.Index], 64); err == nil {
					v = parsed
				}
			}
			out[r] = append(out[r], (v-stat.Mean)/stat.Std)
		}
	}
	return out
}

func prepareCategorical(stat *ColumnStat, values []string, cfg config.Config) {
	counts := map[string]int{}
	cats := map[string]struct{}{}
	for _, c := range stat.Categories {
		cats[c] = struct{}{}
	}
	for _, v := range values {
		if v != "" {
			counts[v]++
			cats[v] = struct{}{}
		}
	}
	mode := ""
	best := -1
	known := append([]string(nil), stat.Categories...)
	sort.Strings(known)
	for _, v := range known {
		if counts[v] > best || (counts[v] == best && counts[v] > 0 && v < mode) {
			mode = v
			best = counts[v]
		}
	}
	if mode == "" {
		all := make([]string, 0, len(counts))
		for v := range counts {
			all = append(all, v)
		}
		sort.Strings(all)
		if len(all) > 0 {
			mode = all[0]
		} else {
			mode = "missing"
		}
	}
	stat.Mode = mode
	fill := mode
	if cfg.ImputeCategorical == "missing" {
		fill = "__missing__"
		cats[fill] = struct{}{}
	}
	stat.CategoricalFill = fill
	list := make([]string, 0, len(cats))
	for c := range cats {
		list = append(list, c)
	}
	sort.Strings(list)
	stat.Categories = list
}

func fillCategorical(raw string, stat ColumnStat) string {
	if raw == "" {
		return stat.CategoricalFill
	}
	return raw
}

func medianSorted(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return values[n/2]
	}
	return (values[n/2-1] + values[n/2]) / 2
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func roundCanonical(v float64) float64 {
	if math.Abs(v) < 1e-15 {
		return 0
	}
	return v
}
