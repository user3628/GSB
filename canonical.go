package abeval

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
)

func CanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func PrettyJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func MustCanonicalJSON(value any) []byte {
	data, err := CanonicalJSON(value)
	if err != nil {
		panic(err)
	}
	return data
}

func SHA256Hex(parts ...[]byte) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write(part)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func HashDataset(dataset Dataset) (RevisionInfo, error) {
	configJSON, err := CanonicalJSON(dataset.Config)
	if err != nil {
		return RevisionInfo{}, err
	}
	eventsJSON, err := CanonicalJSON(dataset.Events)
	if err != nil {
		return RevisionInfo{}, err
	}
	configHash := SHA256Hex(configJSON)
	sampleHash := SHA256Hex(eventsJSON)
	contentHash := SHA256Hex([]byte(configHash), []byte{':'}, []byte(sampleHash))
	return RevisionInfo{
		Version:     dataset.Version,
		ConfigHash:  configHash,
		SampleHash:  sampleHash,
		ContentHash: contentHash,
	}, nil
}

func roundFloat(value float64, digits int) float64 {
	power := math.Pow10(digits)
	return math.Round(value*power) / power
}
