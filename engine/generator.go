package engine

import "fmt"

type DeterministicRNG struct {
	state uint64
}

func NewDeterministicRNG(seed int64) *DeterministicRNG {
	return &DeterministicRNG{state: uint64(seed) + 0x9E3779B97F4A7C15}
}

func (r *DeterministicRNG) Uint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *DeterministicRNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}

var chinesePhrases = []string{
	"退款流程", "发票信息", "物流延迟", "商品质量", "客服态度",
	"账户安全", "会员积分", "订单修改", "售后维修", "优惠券",
}

var latinWords = []string{
	"refund", "invoice", "shipping", "delay", "quality",
	"support", "account", "vip", "order", "coupon",
}

type Batch struct {
	Index   int           `json:"index"`
	ID      string        `json:"id"`
	Samples []SampleInput `json:"samples"`
}

type Correction struct {
	SampleID string            `json:"sample_id"`
	Label    *int              `json:"label,omitempty"`
	Fields   map[string]string `json:"fields,omitempty"`
	Reason   string            `json:"reason"`
}

type DemoData struct {
	Batches     []Batch           `json:"batches"`
	Correction  Correction        `json:"correction"`
	QueryText   string            `json:"query_text"`
	QueryFields map[string]string `json:"query_fields,omitempty"`
}

func intPtr(v int) *int { return &v }

// GenerateDemoData creates at least three batches from a fixed seed.
func GenerateDemoData(seed int64) DemoData {
	rng := NewDeterministicRNG(seed)
	const batchCount = 4
	const batchSize = 6
	batches := make([]Batch, 0, batchCount)
	seq := 1
	for b := 0; b < batchCount; b++ {
		batch := Batch{Index: b + 1, ID: fmt.Sprintf("batch-%02d", b+1)}
		for i := 0; i < batchSize; i++ {
			phrase := chinesePhrases[rng.Intn(len(chinesePhrases))]
			word := latinWords[rng.Intn(len(latinWords))]
			// Every seventh sample contains NFKC compatibility width characters.
			text := fmt.Sprintf("%s %s 编号%02d", phrase, word, rng.Intn(99))
			if seq%7 == 0 {
				text = "ＶＩＰ " + text
			}
			batch.Samples = append(batch.Samples, SampleInput{
				ID:    fmt.Sprintf("s-%04d", seq),
				Text:  text,
				Label: rng.Intn(2),
			})
			seq++
		}
		batches = append(batches, batch)
	}

	target := batches[0].Samples[1] // s-0002: corrected by a later backfill event.
	newLabel := 1 - target.Label
	return DemoData{
		Batches: batches,
		Correction: Correction{
			SampleID: target.ID,
			Label:    intPtr(newLabel),
			Fields:   map[string]string{"source": "cross-batch-backfill"},
			Reason:   "cross-batch label correction and feature supplement",
		},
		QueryText:   "退款 invoice 延迟 vip",
		QueryFields: map[string]string{"source": "online-query"},
	}
}

func FlattenBatches(batches []Batch) []StoredSample {
	var out []StoredSample
	seq := int64(1)
	for _, b := range batches {
		for _, s := range b.Samples {
			out = append(out, StoredSample{
				SampleInput: s,
				Seq:         seq,
				BatchID:     b.ID,
				BatchIndex:  b.Index,
				Revision:    1,
			})
			seq++
		}
	}
	return out
}
