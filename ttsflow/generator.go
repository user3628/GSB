package ttsflow

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	DefaultSeed           int64 = 20261125
	DefaultWorkspaceDir         = ".ttsflow"
	WorkspaceConfigName         = "synthesis.yaml"
	WorkspaceTextsName          = "texts.jsonl"
	WorkspaceDatabaseName       = "state.db"
)

type WorkspacePaths struct {
	Dir    string
	Config string
	Texts  string
	DB     string
}

func PathsForWorkspace(dir string) WorkspacePaths {
	if dir == "" {
		dir = DefaultWorkspaceDir
	}
	return WorkspacePaths{
		Dir: dir, Config: filepath.Join(dir, WorkspaceConfigName),
		Texts: filepath.Join(dir, WorkspaceTextsName), DB: filepath.Join(dir, WorkspaceDatabaseName),
	}
}

func demoCorpus() []TextSample {
	return []TextSample{
		{UTTID: "utt-001", Text: "你好，世界！欢迎使用流式语音合成。", Language: "zh", SpeakerID: 11, SourceRank: 1},
		{UTTID: "utt-002", Text: "Streaming TTS resumes after a hard stop.", Language: "en", SpeakerID: 23, SourceRank: 2},
		{UTTID: "utt-003", Text: "清晨，我们用 Go 1.22 验证 mixed 中英合成。", Language: "zh", SpeakerID: 7, SourceRank: 3},
		{UTTID: "utt-004", Text: "每一帧，都要可复算；每一次中断，都要可恢复。", Language: "zh", SpeakerID: 19, SourceRank: 4},
	}
}

var demoPinyin = map[rune]string{
	'你': "ni", '好': "hao", '世': "shi", '界': "jie", '欢': "huan", '迎': "ying",
	'使': "shi", '用': "yong", '流': "liu", '式': "shi", '语': "yu", '音': "yin",
	'合': "he", '成': "cheng", '清': "qing", '晨': "chen", '我': "wo", '们': "men",
	'验': "yan", '证': "zheng", '中': "zhong", '英': "ying", '每': "mei", '一': "yi",
	'帧': "zhen", '都': "dou", '要': "yao", '可': "ke", '复': "fu", '算': "suan",
	'次': "ci", '断': "duan", '恢': "hui",
}

func GeneratedSamples(seed int64) []TextSample {
	samples := demoCorpus()
	for i := range samples {
		r := mix64(uint64(seed) + uint64(i+1)*0x9e3779b97f4a7c15)
		rate := 0.92 + 0.16*float64(r>>11)*floatScale
		pitch := 0.94 + 0.12*float64(mix64(r)>>11)*floatScale
		energy := 0.9 + 0.2*float64(mix64(r^0xa5a5a5a5)>>11)*floatScale
		emphasis := -0.15 + 0.35*float64(mix64(r^0x5a5a5a5a)>>11)*floatScale
		curve := -0.4 + 0.8*float64(mix64(r^0x12345678)>>11)*floatScale
		samples[i].Prosody = Prosody{
			Rate: round6(rate), Pitch: round6(pitch), Energy: round6(energy),
			PauseMS: 80 + int(mix64(r^0xdeadbeef)%80), Emphasis: round6(emphasis), PitchCurve: round6(curve),
		}
	}
	return samples
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

func GeneratedConfig(seed int64, samples []TextSample) Config {
	zh := map[string][]string{}
	for _, sample := range samples {
		for _, r := range sample.Text {
			if r < 0x3400 || r > 0x9fff {
				continue
			}
			p, ok := demoPinyin[r]
			if !ok {
				p = fmt.Sprintf("cjk%s", strings.ToLower(fmt.Sprintf("%04x", r)))
			}
			zh[string(r)] = []string{p}
		}
	}
	en := map[string][]string{}
	for r := 'a'; r <= 'z'; r++ {
		en[string(r)] = []string{"en_" + string(r)}
	}
	for r := 'A'; r <= 'Z'; r++ {
		en[string(r)] = []string{"en_" + strings.ToLower(string(r))}
	}
	for r := '0'; r <= '9'; r++ {
		en[string(r)] = []string{"num_" + string(r)}
	}
	common := map[string][]string{
		" ": {"pau"}, ",": {"pau"}, ".": {"pau"}, "!": {"pau"}, "?": {"pau"}, ";": {"pau"},
		":": {"pau"}, "，": {"pau"}, "。": {"pau"}, "！": {"pau"}, "？": {"pau"}, "；": {"pau"},
		"、": {"pau"}, "-": {"pau"}, "(": {"pau"}, ")": {"pau"}, "\"": {"pau"}, "'": {"pau"},
	}
	phonemes := map[string]string{"pau": "SIL"}
	for _, table := range []map[string][]string{zh, en, common} {
		for _, values := range table {
			for _, phoneme := range values {
				if phoneme == "pau" {
					continue
				}
				phonemes[phoneme] = "PH_" + strings.ToUpper(strings.ReplaceAll(phoneme, "-", "_"))
			}
		}
	}
	return Config{
		SchemaVersion: 1,
		Revision:      1,
		Seed:          seed,
		Acoustic: AcousticConfig{
			EmbeddingDim: 12, FrameShiftSamples: 160, Bands: 6,
			Layers:     []LayerSpec{{In: 12, Out: 10}, {In: 10, Out: 7}},
			WeightSeed: seed, MinFramesPerUnit: 1, MaxFramesPerUnit: 3,
		},
		Wave:      WaveConfig{SampleRate: 16000, BitDepth: 16, Channels: 1},
		Splitting: SplitConfig{FirstMinChars: 5, FollowingChars: 8, ForceAtPunctuation: true, CheckpointIntervalFrames: 2},
		Frontend:  FrontendConfig{Graphemes: map[string]map[string][]string{"zh": zh, "en": en, "common": common}, PhonemeUnits: phonemes},
	}
}

func EnsureWorkspace(dir string, seed int64) (WorkspacePaths, Config, []byte, []TextSample, error) {
	paths := PathsForWorkspace(dir)
	if err := os.MkdirAll(paths.Dir, 0o755); err != nil {
		return paths, Config{}, nil, nil, err
	}
	samples := GeneratedSamples(seed)
	cfg := GeneratedConfig(seed, samples)
	raw, err := cfg.EncodeYAML()
	if err != nil {
		return paths, Config{}, nil, nil, err
	}
	if _, err := os.Stat(paths.Config); os.IsNotExist(err) {
		if err := writeFileAtomic(paths.Config, raw, 0o644); err != nil {
			return paths, Config{}, nil, nil, err
		}
	} else if err != nil {
		return paths, Config{}, nil, nil, err
	}
	if _, err := os.Stat(paths.Texts); os.IsNotExist(err) {
		if err := WriteJSONL(paths.Texts, samples); err != nil {
			return paths, Config{}, nil, nil, err
		}
	} else if err != nil {
		return paths, Config{}, nil, nil, err
	}
	loadedCfg, loadedRaw, err := LoadConfigFile(paths.Config)
	if err != nil {
		return paths, Config{}, nil, nil, err
	}
	loadedSamples, err := LoadJSONL(paths.Texts)
	if err != nil {
		return paths, Config{}, nil, nil, err
	}
	return paths, loadedCfg, loadedRaw, loadedSamples, nil
}

func WriteJSONL(path string, samples []TextSample) error {
	cp := append([]TextSample(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].UTTID < cp[j].UTTID })
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, sample := range cp {
		if err := sample.Validate(); err != nil {
			return err
		}
		if err := enc.Encode(sample); err != nil {
			return err
		}
	}
	return writeFileAtomic(path, []byte(b.String()), 0o644)
}

func LoadJSONL(path string) ([]TextSample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var samples []TextSample
	seen := map[string]bool{}
	line := 0
	for scanner.Scan() {
		line++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		var sample TextSample
		if err := json.Unmarshal([]byte(raw), &sample); err != nil {
			return nil, &BizError{Code: CodeDatasetInvalid, Message: fmt.Sprintf("invalid JSONL line %d", line), Cause: err}
		}
		if err := sample.Validate(); err != nil {
			return nil, &BizError{Code: CodeDatasetInvalid, Message: fmt.Sprintf("invalid sample at line %d: %v", line, err)}
		}
		if seen[sample.UTTID] {
			return nil, &BizError{Code: CodeDatasetInvalid, Message: fmt.Sprintf("duplicate utt_id %q at line %d", sample.UTTID, line)}
		}
		seen[sample.UTTID] = true
		samples = append(samples, sample)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, &BizError{Code: CodeDatasetInvalid, Message: "text set is empty"}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].UTTID < samples[j].UTTID })
	return samples, nil
}

func OpenWorkspace(dir string) (WorkspacePaths, Config, []byte, []TextSample, *Store, error) {
	paths, cfg, raw, samples, err := EnsureWorkspace(dir, DefaultSeed)
	if err != nil {
		return paths, Config{}, nil, nil, nil, err
	}
	store, err := OpenStore(paths.DB)
	if err != nil {
		return paths, Config{}, nil, nil, nil, err
	}
	return paths, cfg, raw, samples, store, nil
}
