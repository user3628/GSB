package snapshot

import (
	"fmt"
	"time"

	"columnar-executor/internal/model"
)

type splitMix64 struct {
	state uint64
}

func (r *splitMix64) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *splitMix64) intn(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(r.next() % uint64(n))
}

func setNilByte(bits []byte, row int) {
	bits[row/8] |= byte(1) << uint(row%8)
}

func nilBitmap(rows int) []byte {
	return make([]byte, (rows+7)/8)
}

// Generate creates the fixed built-in data set. Row values are deterministic
// for a given (seed, rows, version); generated_at is also deterministic.
func Generate(version int64, rows int, seed uint64) (model.Snapshot, error) {
	if version <= 0 {
		return model.Snapshot{}, model.NewBizError(model.CodeInvalidRequest, "snapshot version must be positive").With("version", version)
	}
	if rows <= 0 || rows > 1_000_000 {
		return model.Snapshot{}, model.NewBizError(model.CodeInvalidRequest, "snapshot rows must be in [1,1000000]").With("rows", rows)
	}

	rng := &splitMix64{state: seed}
	dictionary := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}

	idValues := make([]int64, rows)
	quantityValues := make([]int64, rows)
	priceValues := make([]int64, rows)
	discountValues := make([]int64, rows)
	dictValues := make([]int32, rows)
	quantityNulls := nilBitmap(rows)
	priceNulls := nilBitmap(rows)
	discountNulls := nilBitmap(rows)
	dictNulls := nilBitmap(rows)

	for i := 0; i < rows; i++ {
		idValues[i] = int64(i + 1)
		quantityValues[i] = rng.intn(1000) + 1
		priceValues[i] = 1_000_000 + rng.intn(9_000_000)
		discountValues[i] = rng.intn(500_000)
		dictValues[i] = int32(rng.intn(int64(len(dictionary))))
		// Deterministic null placement keeps all four bitmap forms meaningful.
		if i%41 == 7 {
			setNilByte(quantityNulls, i)
		}
		if i%37 == 11 {
			setNilByte(priceNulls, i)
		}
		if i%43 == 19 {
			setNilByte(discountNulls, i)
		}
		if i%53 == 23 {
			setNilByte(dictNulls, i)
			dictValues[i] = -1
		}
	}

	all := model.NewAll(rows)
	none := model.NewNone(rows)
	sparse := model.NewNone(rows)
	dense := model.NewNone(rows)
	for i := 0; i < rows; i++ {
		if i%11 == 0 {
			_ = sparse.Set(i)
		}
		if i%9 != 0 || i == 0 {
			_ = dense.Set(i)
		}
	}

	snap := model.Snapshot{
		Version:     version,
		GeneratedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(version) * time.Second).Format(time.RFC3339Nano),
		Seed:        seed,
		RowCount:    rows,
		Columns: []model.Column{
			{Name: "id", Type: model.ColumnInt, IntValues: idValues, Nulls: nilBitmap(rows)},
			{Name: "quantity", Type: model.ColumnInt, IntValues: quantityValues, Nulls: quantityNulls},
			{Name: "price", Type: model.ColumnFixed, ScaleUnits: model.FixedScale, FixedValues: priceValues, Nulls: priceNulls},
			{Name: "discount", Type: model.ColumnFixed, ScaleUnits: model.FixedScale, FixedValues: discountValues, Nulls: discountNulls},
			{Name: "category", Type: model.ColumnDict, DictIndices: dictValues, Dictionary: dictionary, Nulls: dictNulls},
		},
		Selections: map[string]model.Bitmap{
			model.FormAll:    all,
			model.FormNone:   none,
			model.FormSparse: sparse,
			model.FormDense:  dense,
		},
	}
	digest, err := model.ComputeSnapshotDigest(snap)
	if err != nil {
		return model.Snapshot{}, fmt.Errorf("compute snapshot digest: %w", err)
	}
	snap.SHA256Digest = digest
	return snap, nil
}
