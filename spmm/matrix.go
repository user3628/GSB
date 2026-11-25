package spmm

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
)

// CSRMatrix is a row-major compressed sparse matrix. Columns in a row are not
// required to be sorted and duplicate columns are legal; their appearance order
// is semantically significant.
type CSRMatrix struct {
	Name             string    `json:"name,omitempty"`
	Rows             int       `json:"rows"`
	Cols             int       `json:"cols"`
	Indptr           []int64   `json:"indptr"`
	Indices          []int64   `json:"indices"`
	Data             []float64 `json:"data"`
	DeclaredShape    string    `json:"declared_shape,omitempty"`
	DeclaredSparsity float64   `json:"declared_sparsity,omitempty"`
}

// DenseMatrix is a row-major dense matrix.
type DenseMatrix struct {
	Name string    `json:"name,omitempty"`
	Rows int       `json:"rows"`
	Cols int       `json:"cols"`
	Data []float64 `json:"data"`
}

type MatrixStats struct {
	Rows                 int     `json:"rows"`
	Cols                 int     `json:"cols"`
	NNZ                  int     `json:"nnz"`
	ObservedSparsity     float64 `json:"observed_sparsity"`
	AverageRowNNZ        float64 `json:"average_row_nnz"`
	MaxRowNNZ            int     `json:"max_row_nnz"`
	EmptyRows            int     `json:"empty_rows"`
	EmptyRowRatio        float64 `json:"empty_row_ratio"`
	ExplicitZeros        int     `json:"explicit_zeros"`
	DuplicateEntries     int     `json:"duplicate_entries"`
	OutOfOrderEntries    int     `json:"out_of_order_entries"`
	DenseRows            int     `json:"dense_rows"`
	DenseCols            int     `json:"dense_cols"`
	ShapeFingerprint     string  `json:"shape_fingerprint"`
	StructureFingerprint string  `json:"structure_fingerprint"`
	DenseFingerprint     string  `json:"dense_fingerprint"`
}

func (m *CSRMatrix) Validate() error {
	if m.Rows < 0 || m.Cols < 0 {
		return NewError(CodeInput, "matrix dimensions must be non-negative")
	}
	if len(m.Indptr) != m.Rows+1 {
		return NewError(CodeInput, "indptr length %d, want rows+1=%d", len(m.Indptr), m.Rows+1)
	}
	if m.Indptr[0] != 0 {
		return NewError(CodeInput, "indptr[0] must be 0")
	}
	for i := 0; i < m.Rows; i++ {
		if m.Indptr[i] > m.Indptr[i+1] {
			return NewError(CodeInput, "indptr decreases at row %d", i)
		}
	}
	nnz := int(m.Indptr[m.Rows])
	if nnz != len(m.Indices) || nnz != len(m.Data) {
		return NewError(CodeInput, "nnz mismatch: indptr=%d indices=%d data=%d", nnz, len(m.Indices), len(m.Data))
	}
	for i, col := range m.Indices {
		if col < 0 || col >= int64(m.Cols) {
			return NewError(CodeInput, "column index %d out of range at entry %d", col, i)
		}
	}
	for i, v := range m.Data {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return NewError(CodeInput, "non-finite sparse value at entry %d", i)
		}
	}
	return nil
}

func (m *DenseMatrix) Validate() error {
	if m.Rows < 0 || m.Cols < 0 {
		return NewError(CodeInput, "dense dimensions must be non-negative")
	}
	if len(m.Data) != m.Rows*m.Cols {
		return NewError(CodeInput, "dense data length %d, want %d", len(m.Data), m.Rows*m.Cols)
	}
	for i, v := range m.Data {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return NewError(CodeInput, "non-finite dense value at index %d", i)
		}
	}
	return nil
}

func (m *CSRMatrix) NNZ() int { return len(m.Indices) }

func (m *CSRMatrix) Stats(dense *DenseMatrix) MatrixStats {
	s := MatrixStats{Rows: m.Rows, Cols: m.Cols, NNZ: len(m.Indices)}
	if dense != nil {
		s.DenseRows, s.DenseCols = dense.Rows, dense.Cols
	}
	if m.Rows*m.Cols > 0 {
		s.ObservedSparsity = float64(len(m.Indices)) / float64(m.Rows*m.Cols)
	}
	for r := 0; r < m.Rows; r++ {
		n := int(m.Indptr[r+1] - m.Indptr[r])
		if n > s.MaxRowNNZ {
			s.MaxRowNNZ = n
		}
		if n == 0 {
			s.EmptyRows++
		}
		seen := make(map[int64]struct{}, n)
		for p := m.Indptr[r]; p < m.Indptr[r+1]; p++ {
			col := m.Indices[p]
			if m.Data[p] == 0 {
				s.ExplicitZeros++
			}
			if _, ok := seen[col]; ok {
				s.DuplicateEntries++
			} else {
				seen[col] = struct{}{}
			}
			if p > m.Indptr[r] && col < m.Indices[p-1] {
				s.OutOfOrderEntries++
			}
		}
	}
	if m.Rows > 0 {
		s.AverageRowNNZ = float64(len(m.Indices)) / float64(m.Rows)
		s.EmptyRowRatio = float64(s.EmptyRows) / float64(m.Rows)
	}
	s.ShapeFingerprint = ShapeFingerprint(m, dense)
	s.StructureFingerprint = StructureFingerprint(m)
	if dense != nil {
		s.DenseFingerprint = DenseFingerprint(dense)
	}
	return s
}

func ShapeFingerprint(m *CSRMatrix, dense *DenseMatrix) string {
	type shape struct {
		Rows, Cols, DenseRows, DenseCols int
	}
	s := shape{Rows: m.Rows, Cols: m.Cols}
	if dense != nil {
		s.DenseRows, s.DenseCols = dense.Rows, dense.Cols
	}
	return HashJSON(s)
}

func StructureFingerprint(m *CSRMatrix) string {
	type structure struct {
		Rows, Cols int
		Indptr     []int64
		Indices    []int64
	}
	return HashJSON(structure{Rows: m.Rows, Cols: m.Cols, Indptr: m.Indptr, Indices: m.Indices})
}

func DenseFingerprint(m *DenseMatrix) string {
	type dense struct {
		Rows, Cols int
		Bits       []uint64
	}
	bits := make([]uint64, len(m.Data))
	for i, v := range m.Data {
		bits[i] = math.Float64bits(v)
	}
	return HashJSON(dense{Rows: m.Rows, Cols: m.Cols, Bits: bits})
}

// RowBlockFingerprint identifies all bytes that can affect one row block.
func RowBlockFingerprint(m *CSRMatrix, dense *DenseMatrix, rowStart, rowEnd int) string {
	type input struct {
		Structure string
		Dense     string
		RowStart  int
		RowEnd    int
		Indices   []int64
		Data      []float64
		DenseBits []uint64
	}
	if rowStart < 0 {
		rowStart = 0
	}
	if rowEnd > m.Rows {
		rowEnd = m.Rows
	}
	p0, p1 := m.Indptr[rowStart], m.Indptr[rowEnd]
	bits := make([]uint64, len(dense.Data))
	for i, v := range dense.Data {
		bits[i] = math.Float64bits(v)
	}
	return HashJSON(input{
		Structure: StructureFingerprint(m),
		Dense:     DenseFingerprint(dense),
		RowStart:  rowStart,
		RowEnd:    rowEnd,
		Indices:   m.Indices[p0:p1],
		Data:      m.Data[p0:p1],
		DenseBits: bits,
	})
}

func LoadCSR(path string) (*CSRMatrix, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, NewError(CodeInput, "read CSR %q: %v", path, err)
	}
	var m CSRMatrix
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, NewError(CodeInput, "decode CSR %q: %v", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func LoadDense(path string) (*DenseMatrix, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, NewError(CodeInput, "read dense matrix %q: %v", path, err)
	}
	var m DenseMatrix
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, NewError(CodeInput, "decode dense matrix %q: %v", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func SaveCSR(path string, m *CSRMatrix) error {
	if err := m.Validate(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode CSR: %w", err)
	}
	b = append(b, '\n')
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write CSR %q: %w", path, err)
	}
	return nil
}

func SaveDense(path string, m *DenseMatrix) error {
	if err := m.Validate(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode dense: %w", err)
	}
	b = append(b, '\n')
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write dense %q: %w", path, err)
	}
	return nil
}
