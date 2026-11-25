package sample

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"

	"multirecall/internal/configx"
	"multirecall/internal/model"
)

const FixedSeed int64 = 30820261126
const VectorDim = 8
const vectorCodeScale = 1.0 / 96.0

type categorySpec struct {
	leaf   string
	path   []string
	titles []string
}

func specs() []categorySpec {
	return []categorySpec{
		{leaf: "手机", path: []string{"数码", "手机"}, titles: []string{
			"旗舰拍照手机 光学变焦 phone camera",
			"轻薄五G手机 长续航 smartphone",
			"游戏手机 散热高刷 gaming phone",
		}},
		{leaf: "笔记本", path: []string{"数码", "电脑", "笔记本"}, titles: []string{
			"游戏笔记本电脑 独显高刷 laptop",
			"轻薄办公笔记本 长续航 notebook",
			"二合一平板电脑 键盘 laptop tablet",
		}},
		{leaf: "相机", path: []string{"数码", "影像", "相机"}, titles: []string{
			"微单相机 高像素 mirrorless camera",
			"运动相机 防水防抖 action camera",
			"复古胶片相机 定焦 film camera",
		}},
		{leaf: "跑步", path: []string{"运动", "跑步"}, titles: []string{
			"缓震跑步鞋 透气 running shoes",
			"碳板竞速跑鞋 马拉松 running",
			"运动速干T恤 跑步训练 shirt",
		}},
		{leaf: "足球", path: []string{"运动", "足球"}, titles: []string{
			"专业足球鞋 碎钉 football boots",
			"五号训练足球 耐磨 football",
			"足球训练服 透气 jersey",
		}},
		{leaf: "护肤", path: []string{"美妆", "护肤"}, titles: []string{
			"玻尿酸补水面膜 保湿 skincare mask",
			"烟酰胺精华液 提亮 serum",
			"温和洁面乳 敏感肌 cleanser",
		}},
		{leaf: "音频", path: []string{"数码", "音频"}, titles: []string{
			"无线降噪耳机 长续航 earphone",
			"蓝牙便携音箱 重低音 speaker",
			"头戴式游戏耳机 麦克风 headset",
		}},
		{leaf: "咖啡", path: []string{"家居", "咖啡"}, titles: []string{
			"全自动咖啡机 家用 espresso machine",
			"手冲咖啡壶套装 coffee pot",
			"空气炸锅 大容量 air fryer",
		}},
	}
}

func normalizedVector(v []float64) []float64 {
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	n := math.Sqrt(sum)
	out := make([]float64, len(v))
	if n == 0 {
		return out
	}
	for i, x := range v {
		out[i] = x / n
	}
	return out
}

func Generate() model.Dataset {
	r := rand.New(rand.NewSource(FixedSeed))
	categorySpecs := specs()
	bases := make([][]float64, len(categorySpecs))
	for i := range categorySpecs {
		raw := make([]float64, VectorDim)
		for j := range raw {
			raw[j] = r.NormFloat64()
		}
		bases[i] = normalizedVector(raw)
	}

	items := make([]model.Item, 0, len(categorySpecs)*3)
	id := 1
	for ci, spec := range categorySpecs {
		for j := 0; j < 3; j++ {
			v := make([]float64, VectorDim)
			for d := 0; d < VectorDim; d++ {
				v[d] = bases[ci][d]*0.78 + r.NormFloat64()*0.24
			}
			v = normalizedVector(v)
			codes := make([]float32, VectorDim)
			for d, x := range v {
				codes[d] = float32(math.Round(x * 96))
			}
			items = append(items, model.Item{
				ItemID:       fmt.Sprintf("I%03d", id),
				Category:     spec.leaf,
				CategoryPath: append([]string(nil), spec.path...),
				VectorDim:    VectorDim,
				VectorScale:  vectorCodeScale,
				VectorF32:    codes,
				Title:        spec.titles[j],
				Lang:         []string{"zh", "mixed", "en"}[j],
			})
			id++
		}
	}

	queries := []model.Query{
		{QueryID: "Q001", RawQuery: "拍照手机  optical zoom", Locale: "zh-CN", HistoryClickCategories: []string{"数码/手机", "数码/影像/相机"}},
		{QueryID: "Q002", RawQuery: "游戏笔记本 laptop", Locale: "zh-CN", HistoryClickCategories: []string{"数码/电脑/笔记本", "数码/手机"}},
		{QueryID: "Q003", RawQuery: "微单相机 mirrorless", Locale: "zh-CN", HistoryClickCategories: []string{"数码/影像/相机"}},
		{QueryID: "Q004", RawQuery: "ＲＵＮＮＩＮＧ ＳＨＯＥＳ 跑步鞋", Locale: "en-US", HistoryClickCategories: []string{"运动/跑步", "运动/足球"}},
		{QueryID: "Q005", RawQuery: "football boots 足球鞋", Locale: "en-GB", HistoryClickCategories: []string{"运动/足球"}},
		{QueryID: "Q006", RawQuery: "补水 面膜 skincare", Locale: "zh-CN", HistoryClickCategories: []string{"美妆/护肤"}},
		{QueryID: "Q007", RawQuery: "降噪耳机 earphone", Locale: "zh-CN", HistoryClickCategories: []string{"数码/音频", "数码/手机"}},
		{QueryID: "Q008", RawQuery: "espresso machine 咖啡机", Locale: "en-US", HistoryClickCategories: []string{"家居/咖啡"}},
		{QueryID: "Q009", RawQuery: "  马　拉松   carbon running  ", Locale: "zh-CN", HistoryClickCategories: []string{"运动/跑步"}},
		{QueryID: "Q900", RawQuery: "  　 ", Locale: "zh-CN", HistoryClickCategories: nil},
	}

	return model.Dataset{Seed: FixedSeed, VectorDim: VectorDim, Items: items, Queries: queries}
}

func DatasetPath(dir string) string { return filepath.Join(dir, "dataset.json") }
func ConfigPath(dir string) string  { return filepath.Join(dir, "config_snapshot.json") }

func Ensure(dir string) (model.Dataset, configx.Snapshot, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return model.Dataset{}, configx.Snapshot{}, err
	}
	datasetFile := DatasetPath(dir)
	configFile := ConfigPath(dir)

	var ds model.Dataset
	if b, err := os.ReadFile(datasetFile); err == nil {
		if err := json.Unmarshal(b, &ds); err != nil {
			return ds, configx.Snapshot{}, fmt.Errorf("parse dataset: %w", err)
		}
	} else if os.IsNotExist(err) {
		ds = Generate()
		b, err := json.MarshalIndent(ds, "", "  ")
		if err != nil {
			return ds, configx.Snapshot{}, err
		}
		if err := os.WriteFile(datasetFile, append(b, '\n'), 0o644); err != nil {
			return ds, configx.Snapshot{}, err
		}
	} else {
		return ds, configx.Snapshot{}, err
	}

	var cfg configx.Snapshot
	if b, err := os.ReadFile(configFile); err == nil {
		if err := json.Unmarshal(b, &cfg); err != nil {
			return ds, cfg, fmt.Errorf("parse config: %w", err)
		}
	} else if os.IsNotExist(err) {
		cfg = configx.DefaultSnapshot()
		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return ds, cfg, err
		}
		if err := os.WriteFile(configFile, append(b, '\n'), 0o644); err != nil {
			return ds, cfg, err
		}
	} else {
		return ds, cfg, err
	}
	return ds, cfg, nil
}

func SaveConfig(dir string, cfg configx.Snapshot) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	configx.RecalcDigest(&cfg)
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ConfigPath(dir), append(b, '\n'), 0o644)
}
