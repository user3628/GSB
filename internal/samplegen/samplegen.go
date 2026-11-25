package samplegen

import (
	"math/rand"

	"featureplatform/internal/configx"
	"featureplatform/internal/model"
)

const FixedSeed int64 = 20261125

const (
	TaskClassification = "text_classification"
	TaskSentiment      = "short_text_sentiment"
	TaskIntent         = "intent_annotation"
)

type template struct {
	Text        string
	Label       string
	TaskVersion int
	Denominator int
}

func DefaultConfig() configx.Config {
	return configx.Config{
		ConfigVersion:     1,
		HashDim:           128,
		LabelTableVersion: "label-table-v1",
		CoverageThreshold: 0.50,
		BatchBudget:       12,
		TaskVersions: map[string]int{
			TaskClassification: 2,
			TaskSentiment:      1,
			TaskIntent:         1,
		},
		VersionPolicy: configx.VersionPolicy{
			MismatchAction: "use_fallback",
			FallbackVersions: map[string]int{
				TaskClassification: 1,
				TaskSentiment:      1,
				TaskIntent:         1,
			},
			MaxSkew: 2,
		},
		TaskMappings: map[string]configx.TaskMapping{
			TaskClassification: {Rules: map[string]configx.MappingRule{
				"1": {
					MappingVersion: "map-cls-v1",
					Terms: map[string]string{
						"人工": "ai",
						"智能": "ai",
						"芯片": "chip",
						"算力": "compute",
					},
				},
				"2": {
					MappingVersion: "map-cls-v2",
					Terms: map[string]string{
						"人工": "artificial_intelligence",
						"智能": "artificial_intelligence",
						"芯片": "semiconductor",
						"算力": "compute_power",
						"赛场": "sports_event",
						"股市": "stock_market",
					},
				},
			}},
			TaskSentiment: {Rules: map[string]configx.MappingRule{
				"1": {
					MappingVersion: "map-sent-v1",
					Terms: map[string]string{
						"物流": "logistics",
						"快递": "logistics",
						"客服": "service_agent",
						"退款": "refund_flow",
						"满意": "positive_sentiment",
						"差":  "negative_sentiment",
					},
				},
				"2": {
					MappingVersion: "map-sent-v2",
					Terms: map[string]string{
						"物流": "delivery_network",
						"快递": "delivery_network",
						"客服": "customer_service",
						"退款": "money_refund",
						"满意": "positive_sentiment",
						"差":  "negative_sentiment",
					},
				},
			}},
			TaskIntent: {Rules: map[string]configx.MappingRule{
				"1": {
					MappingVersion: "map-intent-v1",
					Terms: map[string]string{
						"物流": "logistics",
						"快递": "logistics",
						"到哪": "track_package",
						"查":  "track_package",
						"退款": "refund",
						"退货": "refund",
						"人工": "human_agent",
						"客服": "human_agent",
					},
				},
			}},
		},
		LabelMappings: map[string]map[string]string{
			TaskClassification: {
				"体育": "sports",
				"科技": "technology",
				"财经": "finance",
				"教育": "education",
			},
			TaskSentiment: {
				"好评": "positive",
				"正面": "positive",
				"差评": "negative",
				"负面": "negative",
				"中性": "neutral",
				"旁观": "observation",
			},
			TaskIntent: {
				"查询物流": "track_order",
				"申请退款": "refund",
				"转人工":  "human_agent",
			},
		},
	}
}

func templatesFor(task string) []template {
	switch task {
	case TaskClassification:
		return []template{
			{"中国队在主场赢得比赛，球迷齐声欢呼", "体育", 2, 10},
			{"篮球联赛决赛落幕，新秀砍下三十分", "体育", 2, 10},
			{"新款人工智能芯片发布，算力大幅提升", "科技", 2, 10},
			{"国产操作系统更新，兼容更多应用软件", "科技", 2, 10},
			{"股市震荡收红，投资者关注公司财报", "财经", 2, 10},
			{"新能源汽车销量增长，产业链投资活跃", "财经", 2, 12},
			{"大学发布人工智能课程，推动产教融合", "教育", 1, 10},
			{"中学体育改革，增加足球和篮球课程", "教育", 1, 10},
			{"ＡＩ技术正在改变教育与医疗行业", "科技", 2, 8},
			{"数字经济论坛讨论算力基础设施建设", "财经", 2, 10},
		}
	case TaskSentiment:
		return []template{
			{"物流很快，包装完整，非常满意", "好评", 1, 8},
			{"快递第二天就到了，快递员态度很好", "好评", 1, 8},
			{"客服耐心解答问题，购物体验不错", "好评", 1, 8},
			{"商品质量很好，朋友看到也很喜欢", "好评", 1, 8},
			{"物流太慢了，一个星期还没有到货", "差评", 1, 8},
			{"客服态度很差，退款申请一直被拒绝", "差评", 1, 8},
			{"收到的商品破损，包装也很简陋", "差评", 1, 8},
			{"价格波动太大，刚买完就降价", "负面", 1, 8},
			{"东西中规中矩，没有特别惊喜", "中性", 1, 6},
			{"功能基本可用，界面还有优化空间", "中性", 1, 6},
		}
	default:
		return []template{
			{"我的快递到哪了，帮我查一下物流信息", "查询物流", 1, 8},
			{"订单已经发货，但是物流一直没有更新", "查询物流", 1, 8},
			{"我想查询包裹位置，预计什么时候送达", "查询物流", 1, 8},
			{"商品有质量问题，我要申请退款", "申请退款", 1, 8},
			{"退款多久到账，退货流程怎么操作", "申请退款", 1, 8},
			{"不想要了，帮我取消订单并退款", "申请退款", 1, 8},
			{"机器人解释不清楚，请转人工客服", "转人工", 1, 8},
			{"我需要人工帮助修改收货地址", "转人工", 1, 8},
			{"请让客服专员联系我，电话保持畅通", "转人工", 1, 8},
			{"发票信息有误，需要人工重新开具", "转人工", 1, 8},
		}
	}
}

func GenerateShards() []model.Shard {
	rng := rand.New(rand.NewSource(FixedSeed))
	tasks := []string{TaskClassification, TaskSentiment, TaskIntent}
	prefix := map[string]string{
		TaskClassification: "TC",
		TaskSentiment:      "SA",
		TaskIntent:         "INT",
	}
	shards := make([]model.Shard, 0, len(tasks))
	for _, task := range tasks {
		templates := templatesFor(task)
		samples := make([]model.Sample, 0, 14)
		for i := 1; i <= 10; i++ {
			tpl := templates[rng.Intn(len(templates))]
			text := tpl.Text
			if rng.Intn(5) == 0 {
				text = "  " + text + "\n\t "
			}
			samples = append(samples, model.Sample{
				SampleID:    prefix[task] + "-" + pad4(i),
				Task:        task,
				RawText:     text,
				Label:       tpl.Label,
				TaskVersion: tpl.TaskVersion,
				Coverage:    &model.CoverageDeclaration{Denominator: 32},
			})
		}
		specials := specialSamples(task, prefix[task])
		samples = append(samples, specials...)
		shards = append(shards, model.Shard{Task: task, Samples: samples})
	}
	return shards
}

func pad4(i int) string {
	if i < 10 {
		return "000" + string(rune('0'+i))
	}
	if i < 100 {
		return "00" + itoa(i)
	}
	return itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}

func specialSamples(task, prefix string) []model.Sample {
	switch task {
	case TaskClassification:
		return []model.Sample{
			{
				SampleID: prefix + "-E1", Task: task, RawText: "   \n\t ", Label: "体育", TaskVersion: 2,
			},
			{
				SampleID: prefix + "-V1", Task: task, RawText: "老版本样本仍可按 v1 规则降级映射", Label: "科技", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
			{
				SampleID: prefix + "-U1", Task: task, RawText: "未知标签样本：星际旅行", Label: "火星地理", TaskVersion: 2,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
			{
				SampleID: prefix + "-N1", Task: task, RawText: "短", Label: "教育", TaskVersion: 2,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
		}
	case TaskSentiment:
		return []model.Sample{
			{
				SampleID: prefix + "-E1", Task: task, RawText: "", Label: "中性", TaskVersion: 1,
			},
			{
				SampleID: prefix + "-U1", Task: task, RawText: "这种复杂情绪难以归类", Label: "悲喜交加", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
			{
				SampleID: prefix + "-N1", Task: task, RawText: "差", Label: "差评", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
			{
				SampleID: prefix + "-C1", Task: task, RawText: "客服和物流都很好，退款也顺利", Label: "好评", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32, DeclaredHitBits: intPtr(99)},
			},
			{
				SampleID: prefix + "-NA1", Task: task, RawText: "旁观记录没有覆盖分母", Label: "旁观", TaskVersion: 1,
			},
		}
	default:
		return []model.Sample{
			{
				SampleID: prefix + "-E1", Task: task, RawText: "\u3000", Label: "查询物流", TaskVersion: 1,
			},
			{
				SampleID: prefix + "-U1", Task: task, RawText: "帮我预约上门安装服务", Label: "预约安装", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
			{
				SampleID: prefix + "-N1", Task: task, RawText: "退", Label: "申请退款", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
			{
				SampleID: prefix + "-X1", Task: task, RawText: "ＡＩ客服协助查询快递到哪", Label: "查询物流", TaskVersion: 1,
				Coverage: &model.CoverageDeclaration{Denominator: 32},
			},
		}
	}
}

func intPtr(v int) *int { return &v }
