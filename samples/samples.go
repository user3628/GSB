package samples

import (
	_ "embed"
	"encoding/json"

	"mvccengine/pkg/mvcc"
)

//go:embed deterministic.json
var deterministicJSON []byte

func Deterministic() (mvcc.Sample, error) {
	var sample mvcc.Sample
	if err := json.Unmarshal(deterministicJSON, &sample); err != nil {
		return mvcc.Sample{}, err
	}
	return sample, nil
}

func DeterministicJSON() []byte {
	return deterministicJSON
}
