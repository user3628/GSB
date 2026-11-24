package model

import (
	"fmt"
	"math/bits"
)

const (
	FormAll    = "all"
	FormNone   = "none"
	FormSparse = "sparse"
	FormDense  = "dense"
)

// Bitmap is an immutable-by-convention row selection bitmap. Constructors and
// operations always return a new value; callers must not mutate input words.
type Bitmap struct {
	Rows  int      `json:"rows"`
	Words []uint64 `json:"words"`
}

func ExpectedWords(rows int) int {
	if rows <= 0 {
		return 0
	}
	return (rows + 63) / 64
}

func NewNone(rows int) Bitmap {
	if rows < 0 {
		rows = 0
	}
	return Bitmap{Rows: rows, Words: make([]uint64, ExpectedWords(rows))}
}

func NewAll(rows int) Bitmap {
	bm := NewNone(rows)
	for i := 0; i < rows; i++ {
		bm.Words[i/64] |= uint64(1) << uint(i%64)
	}
	return bm
}

func BitmapFromRows(rows int, selected []int) (Bitmap, error) {
	if rows < 0 {
		return Bitmap{}, NewBizError(CodeBitmapMismatch, "bitmap row count is negative").With("rows", rows)
	}
	bm := NewNone(rows)
	for _, row := range selected {
		if row < 0 || row >= rows {
			return Bitmap{}, NewBizError(CodeBitmapMismatch, "row id outside bitmap range").
				With("row", row).With("rows", rows)
		}
		bm.Words[row/64] |= uint64(1) << uint(row%64)
	}
	return bm, nil
}

func (b Bitmap) Clone() Bitmap {
	words := make([]uint64, len(b.Words))
	copy(words, b.Words)
	return Bitmap{Rows: b.Rows, Words: words}
}

func (b Bitmap) Validate() error {
	if b.Rows < 0 {
		return NewBizError(CodeBitmapMismatch, "bitmap row count is negative").With("rows", b.Rows)
	}
	expected := ExpectedWords(b.Rows)
	if len(b.Words) != expected {
		return NewBizError(CodeBitmapMismatch, "bitmap word length does not match row count").
			With("expected_words", expected).With("actual_words", len(b.Words)).With("rows", b.Rows)
	}
	if b.Rows > 0 && b.Rows%64 != 0 {
		used := uint(b.Rows % 64)
		if b.Words[len(b.Words)-1]>>used != 0 {
			return NewBizError(CodeBitmapMismatch, "bitmap has bits set past row count").With("rows", b.Rows)
		}
	}
	return nil
}

func (b Bitmap) Get(row int) bool {
	if row < 0 || row >= b.Rows || row/64 >= len(b.Words) {
		return false
	}
	return b.Words[row/64]&(uint64(1)<<uint(row%64)) != 0
}

func (b *Bitmap) Set(row int) error {
	if row < 0 || row >= b.Rows {
		return NewBizError(CodeBitmapMismatch, "row id outside bitmap range").With("row", row).With("rows", b.Rows)
	}
	if len(b.Words) != ExpectedWords(b.Rows) {
		return NewBizError(CodeBitmapMismatch, "bitmap word length does not match row count").
			With("expected_words", ExpectedWords(b.Rows)).With("actual_words", len(b.Words))
	}
	b.Words[row/64] |= uint64(1) << uint(row%64)
	return nil
}

func (b Bitmap) Count() int {
	n := 0
	for _, word := range b.Words {
		n += bits.OnesCount64(word)
	}
	return n
}

func (b Bitmap) And(other Bitmap) (Bitmap, error) {
	if b.Rows != other.Rows || len(b.Words) != len(other.Words) {
		return Bitmap{}, NewBizError(CodeBitmapMismatch, "bitmap lengths do not match").
			With("left_rows", b.Rows).With("right_rows", other.Rows).
			With("left_words", len(b.Words)).With("right_words", len(other.Words))
	}
	out := Bitmap{Rows: b.Rows, Words: make([]uint64, len(b.Words))}
	for i := range b.Words {
		out.Words[i] = b.Words[i] & other.Words[i]
	}
	return out, nil
}

func (b Bitmap) Equal(other Bitmap) bool {
	if b.Rows != other.Rows || len(b.Words) != len(other.Words) {
		return false
	}
	for i := range b.Words {
		if b.Words[i] != other.Words[i] {
			return false
		}
	}
	return true
}

func (b Bitmap) RowIDs() []int {
	ids := make([]int, 0, b.Count())
	for row := 0; row < b.Rows; row++ {
		if b.Get(row) {
			ids = append(ids, row)
		}
	}
	return ids
}

func (b Bitmap) String() string {
	return fmt.Sprintf("Bitmap{rows:%d, selected:%d}", b.Rows, b.Count())
}

func ValidBitmapForm(form string) bool {
	switch form {
	case FormAll, FormNone, FormSparse, FormDense:
		return true
	default:
		return false
	}
}
