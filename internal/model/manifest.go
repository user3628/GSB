package model

type ManifestItem struct {
	ItemID    string `json:"item_id"`
	ShardID   string `json:"shard_id"`
	ImageFile string `json:"image_file"`
	Caption   string `json:"caption"`
	Lang      string `json:"lang"`
	Source    string `json:"source"`
}

type GeneratorParams struct {
	SchemaVersion int   `json:"schema_version"`
	Seed          int64 `json:"seed"`
	ShardCount    int   `json:"shard_count"`
	ImageWidth    int   `json:"image_width"`
	ImageHeight   int   `json:"image_height"`
}

func DefaultGeneratorParams() GeneratorParams {
	return GeneratorParams{SchemaVersion: 1, Seed: 295295295, ShardCount: 9, ImageWidth: 96, ImageHeight: 72}
}

type Paths struct {
	Root       string
	DataDir    string
	DBPath     string
	ConfigDir  string
	ParamsPath string
	PolicyPath string
	DemoDir    string
	ShardsDir  string
	Manifest   string
	LockPath   string
	ExportDir  string
	ReportPath string
}

func DefaultPaths(root string) Paths {
	data := root + "/data"
	return Paths{
		Root:       root,
		DataDir:    data,
		DBPath:     data + "/imretrace.sqlite",
		ConfigDir:  data + "/config",
		ParamsPath: data + "/config/default_params.json",
		PolicyPath: data + "/config/policy.v1.json",
		DemoDir:    data + "/demo",
		ShardsDir:  data + "/demo/shards",
		Manifest:   data + "/demo/manifest.jsonl",
		LockPath:   data + "/locks/publish.lock",
		ExportDir:  data + "/exports",
		ReportPath: data + "/exports/diagnostics.json",
	}
}
