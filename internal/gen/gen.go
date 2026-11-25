// Package gen produces the deterministic demo dataset and policy snapshot.
// Everything is derived from fixed constants and the splitmix64 generator in
// internal/determ, so repeated runs yield byte-identical files.
package gen

import (
	"bufio"
	"os"
	"path/filepath"

	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
)

// FixedSeed is the single root of randomness for generated data.
const FixedSeed uint64 = 0x5eed_1234_abcd_6789

// Intent names are fixed so trained classes are stable across machines.
var intents = []string{"billing", "refund", "technical", "account", "logistics", "complaint"}

// Channels are picked deterministically and attached to every token.
var channels = []string{"web", "app", "mail", "im"}

// templates maps an intent to three deterministic text templates, mixing
// Chinese, Chinese/English and fullwidth/latin content.
var templates = map[string][]string{
	"billing": {
		"我的 账单 invoice 金额不对，发票 invoice 有错误",
		"请帮我核对本月账单，自动扣费金额异常",
		"Billing issue: 重复扣款 duplicate charge 请尽快处理",
	},
	"refund": {
		"我要申请退款 refund，商品退货流程是什么",
		"订单取消后多久可以收到退款 refund",
		"Refund please: 未收到货要求全额退款",
	},
	"technical": {
		"系统登录报错 error，页面无法打开 500",
		"App 崩溃 crash 频繁闪退，技术支持请联系我",
		"接口 timeout 超时，功能 technical 故障无法使用",
	},
	"account": {
		"账号 account 被锁定，无法登录请帮我解锁",
		"我想修改密码 password，账号安全设置在哪里",
		"Account login 账号验证 verification 失败",
	},
	"logistics": {
		"快递 logistics 一直没有更新，物流单号查不到",
		"包裹显示已签收但我没有收到，配送 delivery 异常",
		"Shipping 物流延迟 delivery 三天了还没发货",
	},
	"complaint": {
		"客服态度很差，我要投诉 complaint 并要求赔偿",
		"对你们的服务非常不满，投诉 complaint 升级处理",
		"Complaint: 体验糟糕，要求主管回电道歉",
	},
}

// GenerateShards builds the fixed demo dataset: 3 shards of 20 tickets each,
// including duplicates, mixed languages, empty text, out-of-range priority
// and missing agreement. The last two tickets of each shard are reserved
// slots so anomaly tickets always land at stable positions.
func GenerateShards() []model.Shard {
	rng := determ.NewRand(FixedSeed)
	shardCount, perShard := 3, 20
	shards := make([]model.Shard, 0, shardCount)
	counter := 0
	for s := 0; s < shardCount; s++ {
		shardID := shardName(s)
		tickets := make([]model.RawTicket, 0, perShard)
		for i := 0; i < perShard; i++ {
			t := makeTicket(rng, counter, s, i)
			tickets = append(tickets, t)
			counter++
		}
		shards = append(shards, model.Shard{
			Meta:    model.ShardMeta{ShardID: shardID, DeclaredRows: perShard, PolicyVersion: 1},
			Tickets: tickets,
		})
	}
	plantAnomalies(shards)
	plantDuplicates(shards)
	return shards
}

func makeTicket(rng *determ.Rand, globalIdx, shardIdx, inShard int) model.RawTicket {
	intent := intents[rng.Intn(len(intents))]
	channel := channels[rng.Intn(len(channels))]
	tpl := templates[intent][rng.Intn(3)]
	// A fixed subset is rendered with fullwidth punctuation/digits so NFKC
	// normalization has observable, stable effects.
	if globalIdx%7 == 3 {
		tpl = toFullwidth(tpl)
	}
	agreement := 60 + float64(rng.Intn(40)) // 60..99, always present here
	priority := rng.Intn(4)
	lang := "zh"
	if globalIdx%5 == 1 {
		lang = "zh-en"
	}
	return model.RawTicket{
		TicketID:    ticketID(globalIdx),
		Text:        tpl,
		Channel:     channel,
		Language:    lang,
		IntentLabel: intent,
		Priority:    priority,
		Agreement:   &agreement,
	}
}

// plantAnomalies overwrites reserved slots with the required edge cases.
func plantAnomalies(shards []model.Shard) {
	last := shards[len(shards)-1].Tickets
	emptyAgreement := 0.0
	// 1) empty text (also pure punctuation in the second case).
	last[16] = model.RawTicket{TicketID: ticketID(16 + 2*20), Text: "", Channel: "im",
		Language: "zh", IntentLabel: "billing", Priority: 2, Agreement: &emptyAgreement}
	last[17] = model.RawTicket{TicketID: ticketID(17 + 2*20), Text: "。。。 ！？ \u3000", Channel: "mail",
		Language: "zh", IntentLabel: "refund", Priority: 1, Agreement: &emptyAgreement}
	// 2) priority out of range.
	last[18] = model.RawTicket{TicketID: ticketID(18 + 2*20), Text: "账单金额有误请核查 invoice",
		Channel: "web", Language: "zh", IntentLabel: "billing", Priority: 9, Agreement: &emptyAgreement}
	// 3) missing agreement.
	last[19] = model.RawTicket{TicketID: ticketID(19 + 2*20), Text: "快递 logistics 物流停滞三天未更新",
		Channel: "app", Language: "zh-en", IntentLabel: "logistics", Priority: 3, Agreement: nil}
}

// plantDuplicates forces the same text on two channels in different shards so
// tokenization and scoring can be proven channel-sensitive yet deterministic.
func plantDuplicates(shards []model.Shard) {
	const dupText = "账号 account 锁定无法登录，请协助解锁 account"
	shards[0].Tickets[5].Text = dupText
	shards[0].Tickets[5].Channel = "web"
	shards[1].Tickets[5].Text = dupText
	shards[1].Tickets[5].Channel = "im"
}

// GeneratePolicy returns the baseline monotonic policy snapshot (version 1).
func GeneratePolicy() model.Policy {
	return model.Policy{
		Version:        1,
		IntentWeights:  map[string]float64{"billing": 1.0, "refund": 1.0, "technical": 1.0, "account": 1.0, "logistics": 1.0, "complaint": 1.0},
		PriorityThresh: []float64{-30, -20, -10},
		Smoothing:      1.0,
		HashDim:        1024,
		SampleRatio:    0.7,
		Injection:      model.Injection{},
	}
}

// WriteShard writes one shard as canonical JSONL plus its sidecar metadata.
func WriteShard(dir string, shard model.Shard) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, shard.Meta.ShardID+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, t := range shard.Tickets {
		b, err := determ.MarshalJSON(t)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	metaBytes, err := determ.MarshalJSON(shard.Meta)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, shard.Meta.ShardID+".meta.json"), metaBytes, 0o644)
}

// WritePolicy writes the policy snapshot as canonical JSON.
func WritePolicy(dir string, p model.Policy) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := determ.MarshalJSON(p)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, policyFileName(p.Version)), append(b, '\n'), 0o644)
}

func policyFileName(version int) string {
	return "policy.v" + itoa(version) + ".json"
}

func shardName(i int) string { return "shard-00" + string(rune('1'+i)) }

func ticketID(globalIdx int) string {
	return "TKT-" + pad4(globalIdx+1)
}

func pad4(n int) string {
	s := itoa(n)
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// toFullwidth converts ASCII letters/digits and common punctuation to their
// fullwidth Unicode forms so NFKC folding can collapse them back.
func toFullwidth(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r == ' ':
			out = append(out, '\u3000')
		case r >= '!' && r <= '~':
			out = append(out, r+0xFEE0)
		default:
			out = append(out, r)
		}
	}
	return string(out)
}
