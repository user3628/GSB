package demo

import (
	"math/rand"

	"layoutread/internal/model"
)

const GenerationSeed int64 = 20261126

// Generate returns a deterministic local fixture. Geometry and document IDs are
// fixed; the seeded generator selects language-mixed text fragments so every
// call produces byte-identical input and exercises the complete layout matrix.
func Generate() model.Bundle {
	rng := rand.New(rand.NewSource(GenerationSeed))
	strategy := model.DefaultStrategy()
	strategy.Version = 3
	strategy.MergeGapPX = 6
	strategy.MarginXPX = 60
	strategy.MarginYPX = 96
	strategy.ColumnHistogramBinPX = 16
	strategy.ColumnGapPX = 40
	strategy.ColumnMinSupport = 1
	strategy.SpanWidthRatio = 0.92
	strategy.CrossColumnPriority = model.PrioritySpanFirst
	strategy.ConfidenceVerticalWeight = 0.6
	strategy.ConfidenceColumnWeight = 0.4
	strategy.LatencyBudgetMS = 25

	picker := textPicker{rng: rng}
	documents := []model.Document{
		singleColumnDocument(picker),
		doubleColumnDocument(picker),
		spanTitleDocument(picker),
		dualBandDocument(picker),
	}
	return model.Bundle{Documents: documents, Strategy: strategy}
}

type textPicker struct {
	rng *rand.Rand
}

func (p textPicker) pick(zh, en []string) (string, string) {
	if len(zh) == 0 || len(en) == 0 {
		return "", "unknown"
	}
	if p.rng.Intn(2) == 0 {
		return zh[p.rng.Intn(len(zh))], "zh"
	}
	return en[p.rng.Intn(len(en))], "en"
}

func singleColumnDocument(picker textPicker) model.Document {
	zh := []string{
		"文档版面阅读顺序分析将规范化坐标、栏结构与阅读顺序分离处理。",
		"页眉与页脚通过页边距距离和字号档位共同识别。",
		"增量更新必须保证块序列与全量重建完全一致。",
		"本地样本覆盖单栏、双栏、通栏标题和中英文混排。",
	}
	en := []string{
		"Reading order is inferred after deterministic box normalization.",
		"Headers and footers are classified with margin and font evidence.",
		"Checkpoint publication advances only with a contiguous sequence.",
		"Query results are cached by document and policy fingerprints.",
	}
	makeBody := func(index int, y, h float64) model.TextBox {
		text, lang := picker.pick(zh, en)
		return model.TextBox{Index: index, X: 0.10, Y: y, W: 0.80, H: h, FontTier: 2, Text: text, Lang: lang, ColumnHint: 0}
	}
	return model.Document{
		DocID: "doc-single-zh",
		Page:  model.Page{Width: 1200, Height: 1600},
		Boxes: []model.TextBox{
			{Index: 0, X: 0.06, Y: 0.015, W: 0.88, H: 0.025, FontTier: 1, Text: "GSB · 文档版面分析内部资料", Lang: "zh", ColumnHint: 0},
			makeBody(1, 0.13, 0.055),
			makeBody(2, 0.20, 0.050),
			makeBody(3, 0.27, 0.060),
			makeBody(4, 0.35, 0.055),
			makeBody(5, 0.43, 0.060),
			{Index: 6, X: 0.08, Y: 0.955, W: 0.84, H: 0.024, FontTier: 1, Text: "第 1 页 / 共 1 页 · deterministic demo", Lang: "zh", ColumnHint: 0},
		},
	}
}

func doubleColumnDocument(picker textPicker) model.Document {
	zh := []string{
		"左栏先按纵向阅读，再进入右栏顶部继续阅读。",
		"栏区间互不重叠并连续覆盖声明的有效版面宽度。",
		"同一文档内的源框只能在最终序列中出现一次。",
		"顺序置信度同时考虑垂直间隙与栏归属。",
	}
	en := []string{
		"The left column is read before the right column.",
		"Column intervals are contiguous and non-overlapping.",
		"Every source box appears exactly once in the final order.",
		"Confidence combines vertical gaps and column affinity.",
	}
	boxes := make([]model.TextBox, 0, 12)
	boxes = append(boxes, model.TextBox{Index: 0, X: 0.06, Y: 0.015, W: 0.88, H: 0.025, FontTier: 1, Text: "Layout Reading Order / Two Columns", Lang: "en", ColumnHint: 0})
	index := 1
	for column := 0; column < 2; column++ {
		x := 0.08 + float64(column)*0.44
		for row := 0; row < 4; row++ {
			text, lang := picker.pick(zh, en)
			boxes = append(boxes, model.TextBox{
				Index: index, X: x, Y: 0.12 + float64(row)*0.075,
				W: 0.41, H: 0.052, FontTier: 2, Text: text, Lang: lang, ColumnHint: column,
			})
			index++
		}
	}
	boxes = append(boxes, model.TextBox{Index: index, X: 0.08, Y: 0.955, W: 0.84, H: 0.024, FontTier: 1, Text: "Footer · 双栏混排样本", Lang: "zh", ColumnHint: 0})
	return model.Document{DocID: "doc-double-en", Page: model.Page{Width: 1200, Height: 1600}, Boxes: boxes}
}

func spanTitleDocument(picker textPicker) model.Document {
	zh := []string{
		"通栏标题之后的正文先读左栏，再读右栏。",
		"跨栏块的顺序由策略中的优先级决定。",
		"栏统计来自最终块序列的显式归属。",
	}
	en := []string{
		"A full-span title introduces the two-column body.",
		"Cross-column precedence is explicit and versioned.",
		"Column statistics come from explicit assignments.",
	}
	boxes := []model.TextBox{
		{Index: 0, X: 0.06, Y: 0.015, W: 0.88, H: 0.024, FontTier: 1, Text: "研究报告 · Research Notes", Lang: "zh", ColumnHint: 0},
		{Index: 1, X: 0.06, Y: 0.095, W: 0.88, H: 0.060, FontTier: 3, Text: "面向中英混排文档的确定性阅读顺序引擎", Lang: "zh", ColumnHint: -1},
	}
	index := 2
	for column := 0; column < 2; column++ {
		x := 0.08 + float64(column)*0.44
		for row := 0; row < 3; row++ {
			text, lang := picker.pick(zh, en)
			boxes = append(boxes, model.TextBox{
				Index: index, X: x, Y: 0.22 + float64(row)*0.085,
				W: 0.40, H: 0.060, FontTier: 2, Text: text, Lang: lang, ColumnHint: column,
			})
			index++
		}
	}
	boxes = append(boxes, model.TextBox{Index: index, X: 0.08, Y: 0.952, W: 0.84, H: 0.025, FontTier: 1, Text: "页脚 · full-span title fixture", Lang: "zh", ColumnHint: 0})
	return model.Document{DocID: "doc-span-title-zh-en", Page: model.Page{Width: 1200, Height: 1600}, Boxes: boxes}
}

func dualBandDocument(picker textPicker) model.Document {
	zh := []string{
		"上半版内容先于中部通栏小标题。",
		"中部通栏小标题之后继续左右栏阅读。",
		"相邻且间隙很小的片段被确定性合并。",
		"页脚始终排在正文之后。",
	}
	en := []string{
		"Top-band paragraphs precede the mid-page full-span subtitle.",
		"Lower bands resume left-to-right after the full-span block.",
		"Nearby fragments merge in declaration-order ties.",
		"Footer material is ordered after the article body.",
	}
	boxes := []model.TextBox{
		{Index: 0, X: 0.06, Y: 0.014, W: 0.88, H: 0.024, FontTier: 1, Text: "季度技术简报 · Quarterly Brief", Lang: "zh", ColumnHint: 0},
	}
	index := 1
	appendParagraph := func(column int, y, height float64) {
		text, lang := picker.pick(zh, en)
		x := 0.08 + float64(column)*0.44
		boxes = append(boxes, model.TextBox{
			Index: index, X: x, Y: y, W: 0.40, H: height, FontTier: 2,
			Text: text, Lang: lang, ColumnHint: column,
		})
		index++
	}
	appendParagraph(0, 0.12, 0.040)
	appendParagraph(1, 0.12, 0.040)
	appendParagraph(0, 0.20, 0.040)
	appendParagraph(1, 0.20, 0.040)
	// Two fragments in the left column are separated by less than the 6 px merge gap.
	appendParagraph(0, 0.300, 0.020)
	appendParagraph(0, 0.322, 0.020)
	appendParagraph(1, 0.300, 0.040)
	boxes = append(boxes, model.TextBox{Index: index, X: 0.08, Y: 0.46, W: 0.84, H: 0.035, FontTier: 3, Text: "跨栏小标题 / Cross-column subtitle", Lang: "zh", ColumnHint: -1})
	index++
	appendParagraph(0, 0.55, 0.040)
	appendParagraph(1, 0.55, 0.040)
	appendParagraph(0, 0.64, 0.045)
	appendParagraph(1, 0.64, 0.045)
	boxes = append(boxes, model.TextBox{Index: index, X: 0.08, Y: 0.952, W: 0.84, H: 0.026, FontTier: 1, Text: "页脚 · 第四份混排样本 · Page 1", Lang: "zh", ColumnHint: 0})
	return model.Document{DocID: "doc-dual-band-mixed", Page: model.Page{Width: 1200, Height: 1600}, Boxes: boxes}
}
