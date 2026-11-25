package samples

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"spmmopt/spmm"
)

const (
	ManifestVersion = 1
	DefaultSeed     = int64(0x5eed5eed)
)

type ShapeDefinition struct {
	Name       string    `json:"name"`
	Rows       int       `json:"rows"`
	Cols       int       `json:"cols"`
	DenseRows  int       `json:"dense_rows"`
	DenseCols  int       `json:"dense_cols"`
	Sparsities []float64 `json:"-"`
}

var Shapes = []ShapeDefinition{
	{Name: "square", Rows: 96, Cols: 96, DenseRows: 96, DenseCols: 48},
	{Name: "tall", Rows: 240, Cols: 48, DenseRows: 48, DenseCols: 40},
	{Name: "wide", Rows: 48, Cols: 240, DenseRows: 240, DenseCols: 40},
}

var Sparsities = []float64{0.01, 0.10, 0.40}

type Definition struct {
	Name             string  `json:"name"`
	Shape            string  `json:"shape"`
	DeclaredSparsity float64 `json:"declared_sparsity"`
	Rows             int     `json:"rows"`
	Cols             int     `json:"cols"`
	DenseRows        int     `json:"dense_rows"`
	DenseCols        int     `json:"dense_cols"`
	Seed             int64   `json:"seed"`
	MatrixFile       string  `json:"matrix_file"`
	DenseFile        string  `json:"dense_file"`
	Degenerate       bool    `json:"degenerate,omitempty"`
}

type Sample struct {
	Definition
	Stats spmm.MatrixStats  `json:"stats"`
	CSR   *spmm.CSRMatrix   `json:"-"`
	Dense *spmm.DenseMatrix `json:"-"`
}

type Manifest struct {
	Version   int      `json:"version"`
	Seed      int64    `json:"seed"`
	Generator string   `json:"generator"`
	Samples   []Sample `json:"samples"`
}

// GenerateAll writes the reproducible sample corpus below outDir. Existing
// files for the same sample names are replaced atomically by SaveJSONFile.
func GenerateAll(outDir string, seed int64) (*Manifest, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create sample directory: %w", err)
	}
	m := &Manifest{Version: ManifestVersion, Seed: seed, Generator: "spmmopt/samples/v1"}
	idx := 0
	for _, shape := range Shapes {
		for _, sparsity := range Sparsities {
			idx++
			name := fmt.Sprintf("%s_p%02d", shape.Name, int(math.Round(sparsity*100)))
			def := Definition{
				Name: name, Shape: shape.Name, DeclaredSparsity: sparsity,
				Rows: shape.Rows, Cols: shape.Cols,
				DenseRows: shape.DenseRows, DenseCols: shape.DenseCols,
				Seed:       seed + int64(idx*1009),
				MatrixFile: name + ".csr.json", DenseFile: name + ".dense.json",
			}
			s, err := generate(def)
			if err != nil {
				return nil, err
			}
			if err := writeSample(outDir, s); err != nil {
				return nil, err
			}
			m.Samples = append(m.Samples, *s)
		}
	}

	// A dedicated sample combines every structural degeneracy. It is kept
	// separate so the nine primary samples retain their declared sparsities.
	degenDef := Definition{
		Name: "degenerate_mixed", Shape: "wide", DeclaredSparsity: 0.08,
		Rows: 48, Cols: 64, DenseRows: 64, DenseCols: 24,
		Seed: seed + 90001, MatrixFile: "degenerate_mixed.csr.json",
		DenseFile: "degenerate_mixed.dense.json", Degenerate: true,
	}
	degen, err := generateDegenerate(degenDef)
	if err != nil {
		return nil, err
	}
	if err := writeSample(outDir, degen); err != nil {
		return nil, err
	}
	m.Samples = append(m.Samples, *degen)

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	b = append(b, '\n')
	if err := atomicWrite(filepath.Join(outDir, "manifest.json"), b); err != nil {
		return nil, err
	}
	return m, nil
}

func generate(def Definition) (*Sample, error) {
	rng := rand.New(rand.NewSource(def.Seed))
	target := int(math.Round(float64(def.Rows*def.Cols) * def.DeclaredSparsity))
	if target < 12 {
		target = 12
	}
	empty := emptyRows(def.Rows, 8)
	allowed := allowedRows(def.Rows, empty)
	if len(allowed) == 0 {
		return nil, fmt.Errorf("no rows available for sample %s", def.Name)
	}

	rowCols := make([][]int64, def.Rows)
	rowData := make([][]float64, def.Rows)
	explicitTarget := maxInt(1, target/24)
	duplicateTarget := maxInt(2, target/32)
	baseTarget := target - explicitTarget - duplicateTarget
	if baseTarget < 1 {
		baseTarget = 1
	}

	// First create the base nonzeros. A small fraction of repeated positions
	// is intentional; the explicit duplicateTarget entries below guarantee
	// duplicate columns even at the 1% density.
	for i := 0; i < baseTarget; i++ {
		r := allowed[rng.Intn(len(allowed))]
		c := rng.Intn(def.Cols)
		v := rng.Float64()*2 - 1
		if math.Abs(v) < 1e-9 {
			v = 0.25
		}
		rowCols[r] = append(rowCols[r], int64(c))
		rowData[r] = append(rowData[r], v)
	}

	// Duplicate columns are appended after the original occurrence. Keeping
	// them adjacent would hide ordering bugs, so choose any populated row and
	// then a random existing entry in that row.
	for i := 0; i < duplicateTarget; i++ {
		r := allowed[rng.Intn(len(allowed))]
		for len(rowCols[r]) == 0 {
			r = allowed[rng.Intn(len(allowed))]
		}
		j := rng.Intn(len(rowCols[r]))
		c := rowCols[r][j]
		v := rng.Float64()*0.5 - 0.25
		rowCols[r] = append(rowCols[r], c)
		rowData[r] = append(rowData[r], v)
	}

	// Explicit zeros remain represented in CSR and are counted separately by
	// the kernel.
	for i := 0; i < explicitTarget; i++ {
		r := allowed[rng.Intn(len(allowed))]
		c := rng.Intn(def.Cols)
		rowCols[r] = append(rowCols[r], int64(c))
		rowData[r] = append(rowData[r], 0)
	}

	// Break ascending order in a deterministic way to exercise stable sorting
	// and column-block-order accumulation.
	outOfOrder := maxInt(1, target/20)
	for i := 0; i < outOfOrder; i++ {
		r := allowed[rng.Intn(len(allowed))]
		if len(rowCols[r]) < 2 {
			continue
		}
		j := 1 + rng.Intn(len(rowCols[r])-1)
		rowCols[r][j], rowCols[r][len(rowCols[r])-1] = rowCols[r][len(rowCols[r])-1], rowCols[r][j]
		rowData[r][j], rowData[r][len(rowData[r])-1] = rowData[r][len(rowData[r])-1], rowData[r][j]
	}

	m := buildCSR(def, rowCols, rowData)
	dense := generateDense(def, rng)
	s := &Sample{Definition: def, CSR: m, Dense: dense, Stats: m.Stats(dense)}
	return s, nil
}

func generateDegenerate(def Definition) (*Sample, error) {
	rng := rand.New(rand.NewSource(def.Seed))
	rowCols := make([][]int64, def.Rows)
	rowData := make([][]float64, def.Rows)
	empty := emptyRows(def.Rows, 8)
	emptySet := make(map[int]bool, len(empty))
	for _, r := range empty {
		emptySet[r] = true
	}

	for r := 0; r < def.Rows; r++ {
		if emptySet[r] {
			continue
		}
		n := 2 + rng.Intn(5)
		for i := 0; i < n; i++ {
			col := rng.Intn(def.Cols)
			val := rng.Float64()*2 - 1
			rowCols[r] = append(rowCols[r], int64(col))
			rowData[r] = append(rowData[r], val)
		}
		// Fixed duplicate preserving first-occurrence order.
		if len(rowCols[r]) > 0 {
			rowCols[r] = append(rowCols[r], rowCols[r][0])
			rowData[r] = append(rowData[r], 0.125)
		}
		// At least one explicit zero per non-empty row.
		rowCols[r] = append(rowCols[r], int64(rng.Intn(def.Cols)))
		rowData[r] = append(rowData[r], 0)
		// Reverse this row's prefix to guarantee out-of-order columns and to
		// ensure the first duplicate's second occurrence is not last-only.
		for i, j := 0, len(rowCols[r])/2-1; i < j; i, j = i+1, j-1 {
			rowCols[r][i], rowCols[r][j] = rowCols[r][j], rowCols[r][i]
			rowData[r][i], rowData[r][j] = rowData[r][j], rowData[r][i]
		}
	}
	// The two rows after the empty block are entirely explicit zeros.
	for r := def.Rows - 3; r < def.Rows-1; r++ {
		if r < 0 {
			continue
		}
		rowCols[r] = append(rowCols[r], 0, 1, 1)
		rowData[r] = append(rowData[r], 0, 0, 0)
	}
	m := buildCSR(def, rowCols, rowData)
	dense := generateDense(def, rng)
	s := &Sample{Definition: def, CSR: m, Dense: dense, Stats: m.Stats(dense)}
	return s, nil
}

func buildCSR(def Definition, rowCols [][]int64, rowData [][]float64) *spmm.CSRMatrix {
	indptr := make([]int64, def.Rows+1)
	var nnz int64
	for r := 0; r < def.Rows; r++ {
		indptr[r] = nnz
		nnz += int64(len(rowCols[r]))
	}
	indptr[def.Rows] = nnz
	indices := make([]int64, 0, nnz)
	data := make([]float64, 0, nnz)
	for r := 0; r < def.Rows; r++ {
		indices = append(indices, rowCols[r]...)
		data = append(data, rowData[r]...)
	}
	return &spmm.CSRMatrix{
		Name: def.Name, Rows: def.Rows, Cols: def.Cols,
		Indptr: indptr, Indices: indices, Data: data,
		DeclaredShape: def.Shape, DeclaredSparsity: def.DeclaredSparsity,
	}
}

func generateDense(def Definition, rng *rand.Rand) *spmm.DenseMatrix {
	d := &spmm.DenseMatrix{Name: def.Name + "_B", Rows: def.DenseRows, Cols: def.DenseCols}
	d.Data = make([]float64, d.Rows*d.Cols)
	for i := range d.Data {
		d.Data[i] = rng.Float64() - 0.5
	}
	return d
}

func emptyRows(rows, blockSize int) []int {
	if rows <= 0 {
		return nil
	}
	size := minInt(blockSize, maxInt(1, rows/4))
	start := rows / 2
	if start+size > rows {
		start = rows - size
	}
	if start < 0 {
		start = 0
	}
	out := make([]int, 0, size+1)
	if start > 0 {
		out = append(out, 0)
	}
	for r := start; r < start+size; r++ {
		if r != 0 {
			out = append(out, r)
		}
	}
	sort.Ints(out)
	return out
}

func allowedRows(rows int, empty []int) []int {
	blocked := make(map[int]bool, len(empty))
	for _, r := range empty {
		blocked[r] = true
	}
	out := make([]int, 0, rows)
	for r := 0; r < rows; r++ {
		if !blocked[r] {
			out = append(out, r)
		}
	}
	return out
}

func writeSample(outDir string, s *Sample) error {
	if err := spmm.SaveCSR(filepath.Join(outDir, s.MatrixFile), s.CSR); err != nil {
		return err
	}
	if err := spmm.SaveDense(filepath.Join(outDir, s.DenseFile), s.Dense); err != nil {
		return err
	}
	return nil
}

func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
