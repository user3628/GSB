package merge_test

import (
	"encoding/base64"
	"encoding/hex"
	"testing"

	"supplyaudit/internal/merge"
	"supplyaudit/internal/model"
	"supplyaudit/internal/normalize"
)

func TestPriorityAuthorityAndConflicts(t *testing.T) {
	binary := declaration("binary.bin", model.FormatBinary, 10, "@Demo/Lib", "2.0.0", digestFor("binary"))
	jsonLine := declaration("packages.jsonl", model.FormatJSONL, 20, "@demo/lib", "2.0.0.0", digestFor("json"))
	text := declaration("legacy.txtblock", model.FormatTextBlock, 30, "@DEMO/LIB", "1.0.0", sha1For("text"))
	result := merge.Run([]*model.SourceResult{
		source(model.FormatBinary, binary),
		source(model.FormatJSONL, jsonLine),
		source(model.FormatTextBlock, text),
	}, merge.Config{Priority: []model.SourceFormat{model.FormatBinary, model.FormatJSONL, model.FormatTextBlock}})

	if !result.SelfConsistent || result.DeclarationCount != 3 || result.MergeGroupCount != 2 {
		t.Fatalf("unexpected merge accounting: %#v", result)
	}
	if len(result.Components) != 1 {
		t.Fatalf("expected one component, got %#v", result.Components)
	}
	component := result.Components[0]
	if component.HighestVersion.Canonical != "2.0.0" {
		t.Fatalf("unexpected highest version %#v", component.HighestVersion)
	}
	highGroup := component.Versions[0]
	if highGroup.Authority.SourcePath != "binary.bin" || highGroup.Authority.Digest.Hex != binary.Digest.Hex {
		t.Fatalf("priority authority was not selected: %#v", highGroup.Authority)
	}
	if !model.HasCode(highGroup.Codes, model.CodeDigestConflict) {
		t.Fatalf("digest conflict not recorded: %#v", highGroup)
	}
	if model.FindingsHaveCode(result.Findings, model.CodeVersionDowngrade) == false {
		t.Fatalf("version downgrade not recorded: %#v", result.Findings)
	}
	if model.FindingsHaveCode(result.Findings, model.CodeWeakDigest) == false {
		t.Fatalf("weak digest not recorded: %#v", result.Findings)
	}
	if !model.HasCode(component.Codes, model.CodeDigestConflict) || !model.HasCode(component.Codes, model.CodeVersionDowngrade) || !model.HasCode(component.Codes, model.CodeWeakDigest) {
		t.Fatalf("component aggregate codes lost findings: %#v", component.Codes)
	}
}

func TestPrioritySwitchMovesAuthorityDeterministically(t *testing.T) {
	binary := declaration("binary.bin", model.FormatBinary, 10, "lib/name", "1.0.0", digestFor("binary"))
	jsonLine := declaration("packages.jsonl", model.FormatJSONL, 20, "lib/name", "1.0.0", digestFor("json"))
	text := declaration("legacy.txtblock", model.FormatTextBlock, 30, "lib/name", "1.0.0", digestFor("text"))
	sources := []*model.SourceResult{source(model.FormatBinary, binary), source(model.FormatJSONL, jsonLine), source(model.FormatTextBlock, text)}
	result := merge.Run(sources, merge.Config{Priority: []model.SourceFormat{model.FormatJSONL, model.FormatTextBlock, model.FormatBinary}})
	if result.Components[0].Versions[0].Authority.SourcePath != "packages.jsonl" {
		t.Fatalf("switched priority did not select JSON authority: %#v", result.Components[0].Versions[0].Authority)
	}
	if len(result.Components[0].Versions[0].Declarations) != 3 {
		t.Fatalf("different digests should retain all source declarations")
	}
}

func TestBuildMetadataMergesWithEquivalentCore(t *testing.T) {
	binary := declaration("binary.bin", model.FormatBinary, 10, "@demo/build", "3.2.1+build.7", digestFor("same"))
	jsonLine := declaration("packages.jsonl", model.FormatJSONL, 20, "@demo/build", "3.2.1.0+build.9", digestFor("same"))
	result := merge.Run([]*model.SourceResult{source(model.FormatBinary, binary), source(model.FormatJSONL, jsonLine)}, merge.Config{Priority: []model.SourceFormat{model.FormatBinary, model.FormatJSONL, model.FormatTextBlock}})
	if len(result.Components) != 1 || len(result.Components[0].Versions) != 1 {
		t.Fatalf("build variants should merge into one version view: %#v", result.Components)
	}
	if len(result.Components[0].Versions[0].Declarations) != 2 || len(result.Findings) != 0 {
		t.Fatalf("unexpected declarations/findings: %#v %#v", result.Components[0].Versions[0].Declarations, result.Findings)
	}
}

func declaration(path string, format model.SourceFormat, offset int64, name, version, digest string) model.Declaration {
	key, _ := normalize.ComponentKey(name)
	versionView, err := normalize.ParseVersion(version)
	if err != nil {
		panic(err)
	}
	digestView, err := normalize.ParseDigest(digest)
	if err != nil {
		panic(err)
	}
	return model.Declaration{
		ID:           path + "#" + version,
		SourceID:     string(format),
		SourcePath:   path,
		SourceFormat: format,
		Origin:       "test",
		Offset:       offset,
		ComponentKey: key,
		RawName:      name,
		Version:      versionView,
		Digest:       digestView,
		RawDigest:    digest,
		Codes:        []string{},
	}
}

func source(format model.SourceFormat, declarations ...model.Declaration) *model.SourceResult {
	result := &model.SourceResult{Spec: model.SourceSpec{ID: string(format), Format: format, ReportPath: declarations[0].SourcePath}, Declarations: declarations}
	result.Finalize()
	return result
}

func digestFor(seed string) string {
	sum := make([]byte, 32)
	for index := range sum {
		sum[index] = seed[index%len(seed)] + byte(index)
	}
	return "sha256:" + base64.StdEncoding.EncodeToString(sum)
}

func sha1For(seed string) string {
	sum := make([]byte, 20)
	for index := range sum {
		sum[index] = seed[index%len(seed)] + byte(index)
	}
	return "sha1:" + hex.EncodeToString(sum)
}
