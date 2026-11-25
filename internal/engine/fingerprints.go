package engine

import "dialogalign/internal/domain"

type stageConfigs struct {
	parse   string
	upgrade string
	evolve  string
	align   string
	global  string
}

type stageInput struct {
	Version        string `json:"version"`
	PreviousDigest string `json:"previous_digest,omitempty"`
	UpstreamDigest string `json:"upstream_digest,omitempty"`
	MapVersion     string `json:"map_version,omitempty"`
	MapDigest      string `json:"map_digest,omitempty"`
	RulesDigest    string `json:"rules_digest,omitempty"`
	CatalogDigest  string `json:"catalog_digest,omitempty"`
	NormDigest     string `json:"norm_digest,omitempty"`
	AlignDigest    string `json:"align_digest,omitempty"`
	FinalTurn      bool   `json:"final_turn,omitempty"`
}

func buildStageConfigs(dataset domain.Dataset) (stageConfigs, error) {
	parse, err := domain.Digest(stageInput{Version: "parse-v1"})
	if err != nil {
		return stageConfigs{}, err
	}
	upgrade, err := domain.Digest(stageInput{
		Version:       "upgrade-v1",
		MapVersion:    dataset.ActiveMap.Version,
		MapDigest:     dataset.MapFileHash,
		CatalogDigest: dataset.RolesHash,
		NormDigest:    digestOrEmpty(dataset.Config.Normalization),
	})
	if err != nil {
		return stageConfigs{}, err
	}
	evolve, err := domain.Digest(stageInput{
		Version:       "evolve-v1",
		RulesDigest:   digestOrEmpty(dataset.Config.Rules),
		CatalogDigest: dataset.RolesHash,
	})
	if err != nil {
		return stageConfigs{}, err
	}
	align, err := domain.Digest(stageInput{
		Version:       "align-v1",
		AlignDigest:   digestOrEmpty(dataset.Config.Alignment),
		CatalogDigest: dataset.RolesHash,
		MapVersion:    dataset.ActiveMap.Version,
		MapDigest:     dataset.MapFileHash,
	})
	if err != nil {
		return stageConfigs{}, err
	}
	global, err := domain.Digest(struct {
		Version       string                     `json:"version"`
		Rules         domain.RulesConfig         `json:"rules"`
		Normalization domain.NormalizationConfig `json:"normalization"`
		Alignment     domain.AlignmentConfig     `json:"alignment"`
		CatalogDigest string                     `json:"catalog_digest"`
		MapVersion    string                     `json:"map_version"`
		MapDigest     string                     `json:"map_digest"`
	}{
		Version:       "config-v1",
		Rules:         dataset.Config.Rules,
		Normalization: dataset.Config.Normalization,
		Alignment:     dataset.Config.Alignment,
		CatalogDigest: dataset.RolesHash,
		MapVersion:    dataset.ActiveMap.Version,
		MapDigest:     dataset.MapFileHash,
	})
	if err != nil {
		return stageConfigs{}, err
	}
	return stageConfigs{parse: parse, upgrade: upgrade, evolve: evolve, align: align, global: global}, nil
}

func parseInputDigest(turn domain.TurnInput) (string, error) {
	return domain.Digest(stageInput{Version: "parse-input-v1", UpstreamDigest: digestOrEmpty(turn)})
}

func upgradeInputDigest(parseOutput string, cfg stageConfigs) (string, error) {
	return domain.Digest(stageInput{Version: cfg.upgrade, UpstreamDigest: parseOutput})
}

func evolveInputDigest(upgradeOutput, previousStateDigest string, cfg stageConfigs, final bool) (string, error) {
	return domain.Digest(stageInput{Version: cfg.evolve, UpstreamDigest: upgradeOutput, PreviousDigest: previousStateDigest, FinalTurn: final})
}

func alignInputDigest(evolveOutput string, cfg stageConfigs) (string, error) {
	return domain.Digest(stageInput{Version: cfg.align, UpstreamDigest: evolveOutput})
}

func digestOrEmpty(v any) string {
	digest, err := domain.Digest(v)
	if err != nil {
		return ""
	}
	return digest
}
