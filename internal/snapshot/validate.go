package snapshot

import (
	"time"

	"columnar-executor/internal/model"
)

func invalid(message string) *model.BizError {
	return model.NewBizError(model.CodeSnapshotInvalid, message)
}

func checkPackedBitmap(bits []byte, rowCount int, bitmapName string) error {
	expected := (rowCount + 7) / 8
	if len(bits) != expected {
		return invalid("null bitmap length does not match row count").
			With("bitmap", bitmapName).With("rows", rowCount).With("expected_bytes", expected).With("actual_bytes", len(bits))
	}
	if rowCount%8 != 0 && rowCount > 0 {
		used := uint(rowCount % 8)
		if bits[len(bits)-1]>>used != 0 {
			return invalid("null bitmap has bits past row count").With("bitmap", bitmapName).With("rows", rowCount)
		}
	}
	return nil
}

// Validate checks every invariant before a snapshot can become visible. It also
// verifies the content digest so a partially edited payload is rejected.
func Validate(s model.Snapshot) error {
	if s.Version <= 0 {
		return invalid("snapshot version must be positive").With("version", s.Version)
	}
	if s.RowCount <= 0 {
		return invalid("snapshot row_count must be positive").With("row_count", s.RowCount)
	}
	if s.GeneratedAt == "" {
		return invalid("snapshot generated_at is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, s.GeneratedAt); err != nil {
		return invalid("snapshot generated_at is not RFC3339").With("generated_at", s.GeneratedAt)
	}
	if len(s.Columns) == 0 {
		return invalid("snapshot must contain at least one column")
	}

	seen := map[string]struct{}{}
	for _, col := range s.Columns {
		if col.Name == "" {
			return invalid("column name must not be empty")
		}
		if _, exists := seen[col.Name]; exists {
			return invalid("duplicate column name").With("column", col.Name)
		}
		seen[col.Name] = struct{}{}
		if err := checkPackedBitmap(col.Nulls, s.RowCount, col.Name); err != nil {
			return err
		}
		switch col.Type {
		case model.ColumnInt:
			if len(col.IntValues) != s.RowCount {
				return invalid("integer column length does not match rows").
					With("column", col.Name).With("expected", s.RowCount).With("actual", len(col.IntValues))
			}
		case model.ColumnFixed:
			if col.ScaleUnits != model.FixedScale {
				return invalid("fixed column scale must be 1000000").
					With("column", col.Name).With("scale_units", col.ScaleUnits)
			}
			if len(col.FixedValues) != s.RowCount {
				return invalid("fixed column length does not match rows").
					With("column", col.Name).With("expected", s.RowCount).With("actual", len(col.FixedValues))
			}
		case model.ColumnDict:
			if len(col.Dictionary) == 0 {
				return invalid("dictionary must not be empty").With("column", col.Name)
			}
			if len(col.DictIndices) != s.RowCount {
				return invalid("dictionary index length does not match rows").
					With("column", col.Name).With("expected", s.RowCount).With("actual", len(col.DictIndices))
			}
			for row, idx := range col.DictIndices {
				if col.NullAt(row) {
					if idx != -1 && (idx < 0 || int(idx) >= len(col.Dictionary)) {
						return invalid("dictionary index outside dictionary while null").
							With("column", col.Name).With("row", row).With("index", idx).With("dictionary_size", len(col.Dictionary))
					}
					continue
				}
				if idx < 0 || int(idx) >= len(col.Dictionary) {
					return invalid("dictionary index outside dictionary").
						With("column", col.Name).With("row", row).With("index", idx).With("dictionary_size", len(col.Dictionary))
				}
			}
		default:
			return invalid("unknown column type").With("column", col.Name).With("type", col.Type)
		}
	}

	for _, form := range []string{model.FormAll, model.FormNone, model.FormSparse, model.FormDense} {
		bm, ok := s.Selections[form]
		if !ok {
			return invalid("required selection bitmap form is missing").With("form", form)
		}
		if bm.Rows != s.RowCount {
			return invalid("selection bitmap rows do not match snapshot rows").
				With("form", form).With("snapshot_rows", s.RowCount).With("bitmap_rows", bm.Rows)
		}
		if err := bm.Validate(); err != nil {
			biz, _ := model.AsBizError(err)
			return invalid("selection bitmap is invalid").With("form", form).With("cause", biz.Context)
		}
	}

	if s.SHA256Digest == "" {
		return invalid("snapshot sha256 digest is required")
	}
	computed, err := model.ComputeSnapshotDigest(s)
	if err != nil {
		return invalid("snapshot digest cannot be computed").With("cause", err.Error())
	}
	if s.SHA256Digest != computed {
		return invalid("snapshot sha256 digest mismatch").
			With("expected", s.SHA256Digest).With("computed", computed)
	}
	return nil
}
