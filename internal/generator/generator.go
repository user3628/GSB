package generator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"csrouter/internal/domain"
	"csrouter/internal/nlp"
)

const DefaultSeed int64 = 20261126

type RNG struct{ state uint64 }

func NewRNG(seed int64) *RNG { return &RNG{state: uint64(seed) + 0x9e3779b97f4a7c15} }

func (r *RNG) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *RNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}

func (r *RNG) Float64() float64 {
	return float64(r.Uint64()>>11) / float64(1<<53)
}

type intentSpec struct {
	Code     string
	Zh       []string
	En       []string
	Severity float64
}

var intentSpecs = []intentSpec{
	{"account_access", []string{"登录", "账号", "验证"}, []string{"login", "account", "verify"}, 0.58},
	{"billing_payment", []string{"扣款", "支付", "账单"}, []string{"payment", "billing", "charge"}, 0.72},
	{"refund_return", []string{"退款", "退货", "到账"}, []string{"refund", "return", "money"}, 0.76},
	{"logistics_delivery", []string{"物流", "快递", "签收"}, []string{"delivery", "shipping", "package"}, 0.50},
	{"technical_outage", []string{"故障", "崩溃", "无法使用"}, []string{"outage", "crash", "error"}, 0.92},
	{"security_fraud", []string{"盗刷", "安全", "异常登录"}, []string{"fraud", "security", "hacked"}, 0.96},
	{"subscription_plan", []string{"订阅", "续费", "套餐"}, []string{"subscription", "renew", "plan"}, 0.46},
	{"feedback_other", []string{"建议", "反馈", "咨询"}, []string{"feedback", "question", "suggestion"}, 0.24},
	{"order_modify", []string{"订单", "修改", "取消"}, []string{"order", "modify", "cancel"}, 0.48},
	{"invoice_tax", []string{"发票", "税号", "抬头"}, []string{"invoice", "tax", "receipt"}, 0.34},
	{"coupon_promo", []string{"优惠券", "活动", "折扣"}, []string{"coupon", "promo", "discount"}, 0.30},
	{"product_quality", []string{"质量", "破损", "瑕疵"}, []string{"quality", "broken", "defect"}, 0.68},
	{"warranty_repair", []string{"保修", "维修", "售后"}, []string{"warranty", "repair", "service"}, 0.56},
	{"installation_guide", []string{"安装", "教程", "配置"}, []string{"install", "setup", "guide"}, 0.38},
	{"mobile_app", []string{"应用", "闪退", "手机"}, []string{"mobile", "app", "android"}, 0.62},
	{"web_portal", []string{"网页", "浏览器", "页面"}, []string{"web", "portal", "browser"}, 0.52},
	{"notification_sms", []string{"短信", "通知", "推送"}, []string{"sms", "notification", "alert"}, 0.42},
	{"privacy_data", []string{"隐私", "个人信息", "授权"}, []string{"privacy", "data", "consent"}, 0.74},
	{"enterprise_sla", []string{"企业", "工单", "SLA"}, []string{"enterprise", "sla", "contract"}, 0.82},
	{"api_integration", []string{"接口", "调用", "集成"}, []string{"api", "integration", "webhook"}, 0.70},
	{"data_export", []string{"导出", "报表", "数据"}, []string{"export", "report", "data"}, 0.36},
	{"account_deletion", []string{"注销", "删除", "清空"}, []string{"deletion", "delete", "gdpr"}, 0.60},
	{"shipping_address", []string{"地址", "修改地址", "收件"}, []string{"address", "shipping", "recipient"}, 0.40},
	{"complaint_escalation", []string{"投诉", "升级", "经理"}, []string{"complaint", "escalation", "manager"}, 0.88},
}

var channels = []string{"app", "web", "wechat", "email", "phone"}

type tierConfig struct {
	name      string
	rows      int
	intents   int
	shards    int
	rowBand   string
	hashDim   int
	budget    int
	vocabTopK int
	prefix    string
	idWidth   int
}

var tierConfigs = []tierConfig{
	{name: "small", rows: 400, intents: 8, shards: 2, rowBand: "~400", hashDim: 1024, budget: 180000, vocabTopK: 96, prefix: "S", idWidth: 4},
	{name: "medium", rows: 4000, intents: 24, shards: 4, rowBand: "~4000", hashDim: 4096, budget: 3500000, vocabTopK: 256, prefix: "M", idWidth: 6},
}

func GenerateAll(root string, seed int64) (*domain.Manifest, map[string]domain.Policy, error) {
	if seed == 0 {
		seed = DefaultSeed
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, nil, err
	}
	manifest := domain.Manifest{
		SchemaVersion: "manifest/v1",
		Generator:     "csrouter-fixed-seed-generator",
		Seed:          seed,
		Columns:       []string{"ticket_id", "text", "channel", "language", "intent", "priority", "confidence_pct"},
		Tiers:         map[string]domain.TierManifest{},
		TierOrder:     []string{"small", "medium"},
	}
	policies := map[string]domain.Policy{}

	for _, cfg := range tierConfigs {
		rng := NewRNG(seed + int64(cfg.rows))
		tickets := generateTierTickets(cfg, rng)
		tierDir := filepath.Join(root, cfg.name)
		if err := os.MkdirAll(tierDir, 0o755); err != nil {
			return nil, nil, err
		}

		shardSize := cfg.rows / cfg.shards
		tierManifest := domain.TierManifest{
			Tier:        cfg.name,
			Seed:        seed,
			RowBand:     cfg.rowBand,
			Rows:        cfg.rows,
			IntentCount: cfg.intents,
			Columns:     manifest.Columns,
			PolicyPath:  filepath.Join(cfg.name, "policy.json"),
		}

		for s := 0; s < cfg.shards; s++ {
			start := s * shardSize
			end := start + shardSize
			if s == cfg.shards-1 {
				end = cfg.rows
			}
			shardTickets := tickets[start:end]
			fileName := fmt.Sprintf("shard-%03d.jsonl", s)
			relPath := filepath.Join(cfg.name, fileName)
			absPath := filepath.Join(root, relPath)
			sha, err := writeJSONL(absPath, shardTickets)
			if err != nil {
				return nil, nil, err
			}
			tierManifest.Shards = append(tierManifest.Shards, domain.ShardManifest{
				Path:          relPath,
				Tier:          cfg.name,
				Rows:          len(shardTickets),
				Columns:       len(manifest.Columns),
				DeclaredRows:  len(shardTickets),
				DeclaredCols:  len(manifest.Columns),
				SHA256:        sha,
				FirstTicketID: shardTickets[0].TicketID,
				LastTicketID:  shardTickets[len(shardTickets)-1].TicketID,
			})
		}

		policy := buildPolicy(cfg, tickets)
		policyPath := filepath.Join(root, tierManifest.PolicyPath)
		if err := writeJSON(policyPath, policy); err != nil {
			return nil, nil, err
		}
		policies[cfg.name] = policy
		manifest.Tiers[cfg.name] = tierManifest
	}

	if err := writeJSON(filepath.Join(root, "manifest.json"), manifest); err != nil {
		return nil, nil, err
	}
	return &manifest, policies, nil
}

func generateTierTickets(cfg tierConfig, rng *RNG) []domain.Ticket {
	tickets := make([]domain.Ticket, cfg.rows)
	for i := 0; i < cfg.rows; i++ {
		spec := intentSpecs[rng.Intn(cfg.intents)]
		zh1 := spec.Zh[rng.Intn(len(spec.Zh))]
		zh2 := spec.Zh[rng.Intn(len(spec.Zh))]
		en1 := spec.En[rng.Intn(len(spec.En))]
		en2 := spec.En[rng.Intn(len(spec.En))]
		channel := channels[rng.Intn(len(channels))]

		var text, language string
		style := rng.Intn(10)
		switch {
		case style < 6:
			text = fmt.Sprintf("你好，我的%s问题还没解决，%s页面一直报错；%s/%s please follow up。", zh1, zh2, en1, en2)
			language = "mixed"
		case style < 8:
			text = fmt.Sprintf("请问%s进度如何？刚才%s失败了，订单页面提示%s，请处理。", zh1, zh2, en1)
			language = "zh"
		default:
			text = fmt.Sprintf("Hello team, %s and %s are broken in production; %s alert keeps firing.", en1, en2, zh1)
			language = "en"
		}

		confidence := math.Round((72+rng.Float64()*27.9)*10) / 10
		modelConf := 0.88 + rng.Float64()*0.11
		combined := 0.60*spec.Severity + 0.20*modelConf + 0.20*(confidence/100)
		priority := priorityFromScore(combined)
		if rng.Intn(100) < 12 {
			delta := 1
			if rng.Intn(2) == 0 {
				delta = -1
			}
			priority = clampPriority(priority + delta)
		}

		id := fmt.Sprintf("%s-%0*d", cfg.prefix, cfg.idWidth, i+1)
		tickets[i] = domain.Ticket{
			TicketID:      id,
			Text:          text,
			Channel:       channel,
			Language:      language,
			Intent:        spec.Code,
			Priority:      &priority,
			ConfidencePct: &confidence,
		}
	}

	applyCoverageOverrides(tickets)
	applyDuplicateGroups(tickets)
	return tickets
}

func applyCoverageOverrides(tickets []domain.Ticket) {
	setText := func(i int, text string) {
		if i < len(tickets) {
			tickets[i].Text = text
		}
	}
	setText(4, "")
	setText(17, "   \n\t  \r\n ")
	setText(29, "。，！？、；：“”‘’（）")

	setPriority := func(i, value int) {
		if i < len(tickets) {
			tickets[i].Priority = &value
		}
	}
	setPriority(41, 9)
	setPriority(53, -1)
	setPriority(61, 7)

	setConfidenceNil := func(i int) {
		if i < len(tickets) {
			tickets[i].ConfidencePct = nil
		}
	}
	setConfidenceNil(67)
	setConfidenceNil(79)
	setConfidenceNil(61)
}

func applyDuplicateGroups(tickets []domain.Ticket) {
	groups := [][2]int{{101, 102}, {205, 206}, {307, 308}}
	for _, group := range groups {
		a, b := group[0], group[1]
		if b >= len(tickets) {
			continue
		}
		tickets[b].Text = tickets[a].Text
		tickets[b].Channel = tickets[a].Channel
		tickets[b].Intent = tickets[a].Intent
		if tickets[a].Priority != nil {
			value := *tickets[a].Priority
			tickets[b].Priority = &value
		}
		if tickets[a].ConfidencePct != nil {
			value := *tickets[a].ConfidencePct
			tickets[b].ConfidencePct = &value
		}
	}
}

func priorityFromScore(score float64) int {
	switch {
	case score >= 0.78:
		return 3
	case score >= 0.62:
		return 2
	case score >= 0.45:
		return 1
	default:
		return 0
	}
}

func clampPriority(v int) int {
	if v < 0 {
		return 0
	}
	if v > 3 {
		return 3
	}
	return v
}

func buildPolicy(cfg tierConfig, tickets []domain.Ticket) domain.Policy {
	intentNames := make([]string, cfg.intents)
	severity := map[string]float64{}
	weights := map[string]map[string]float64{}
	for i := 0; i < cfg.intents; i++ {
		spec := intentSpecs[i]
		intentNames[i] = spec.Code
		severity[spec.Code] = spec.Severity
		group := map[string]float64{}
		for j, term := range spec.Zh {
			group[term] = []float64{1.28, 1.18, 1.10}[j%3]
		}
		for j, term := range spec.En {
			group[term] = []float64{1.24, 1.15, 1.08}[j%3]
		}
		weights[spec.Code] = group
	}

	uniqueTerms := map[string]bool{}
	totalTerms := 0
	for _, ticket := range tickets {
		norm, _ := nlp.NormalizeTicket(ticket, 1)
		totalTerms += len(norm.Tokens)
		for _, token := range norm.Tokens {
			uniqueTerms[token.Text] = true
		}
	}
	avg := 0.0
	if len(tickets) > 0 {
		avg = math.Round(float64(totalTerms)/float64(len(tickets))*1e6) / 1e6
	}

	return domain.Policy{
		SchemaVersion:             "policy/v1",
		Version:                   1,
		MonotonicVersion:          1,
		Tier:                      cfg.name,
		IntentNames:               intentNames,
		IntentVocabWeights:        weights,
		IntentBias:                map[string]float64{},
		IntentSeverity:            severity,
		PriorityThresholds:        domain.PriorityThresholds{P1: 0.45, P2: 0.62, P3: 0.78},
		SmoothingAlpha:            0.75,
		HashDimensions:            cfg.hashDim,
		RoundBudget:               cfg.budget,
		VocabTopK:                 cfg.vocabTopK,
		MissingConfidenceFallback: 65,
		Scale: domain.ScaleObservation{
			Tier:                cfg.name,
			RowBand:             cfg.rowBand,
			DeclaredTickets:     cfg.rows,
			ActualTickets:       cfg.rows,
			IntentCount:         cfg.intents,
			ShardCount:          cfg.shards,
			HashDimensions:      cfg.hashDim,
			RoundBudget:         cfg.budget,
			ObservedUniqueTerms: len(uniqueTerms),
			ObservedTotalTerms:  totalTerms,
			AvgTermsPerTicket:   avg,
		},
		ChangeCode: domain.CodeSnapshotReplaced,
		ChangeNote: "initial deterministic policy snapshot",
	}
}

func writeJSONL(path string, tickets []domain.Ticket) (string, error) {
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, ticket := range tickets {
		b, err := json.Marshal(ticket)
		if err != nil {
			_ = file.Close()
			return "", err
		}
		if _, err := file.Write(append(b, '\n')); err != nil {
			_ = file.Close()
			return "", err
		}
		_, _ = h.Write(b)
		_, _ = h.Write([]byte{'\n'})
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
