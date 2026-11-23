package samples

import (
	"embed"
	"encoding/json"

	"local/rollout-guard/internal/domain"
)

//go:embed policy-v1.json batches.json observations.json
var files embed.FS

type BatchList struct {
	Batches []domain.Batch `json:"batches"`
}

type ObservationSet struct {
	Batches map[string][]domain.Observation `json:"batches"`
}

func Policy() (domain.Policy, error) {
	raw, err := files.ReadFile("policy-v1.json")
	if err != nil {
		return domain.Policy{}, err
	}
	var policy domain.Policy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return domain.Policy{}, err
	}
	return policy, nil
}

func Batches() ([]domain.Batch, error) {
	raw, err := files.ReadFile("batches.json")
	if err != nil {
		return nil, err
	}
	var list BatchList
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, err
	}
	return list.Batches, nil
}

func Observations() (map[string][]domain.Observation, error) {
	raw, err := files.ReadFile("observations.json")
	if err != nil {
		return nil, err
	}
	var set ObservationSet
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, err
	}
	return set.Batches, nil
}
