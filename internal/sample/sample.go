// Package sample produces deterministic local workloads for validation.
package sample

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"local/eventbus/internal/bus"
)

const BaseEventTimeMS int64 = 1_700_000_000_000

type Spec struct {
	EventID      string
	PartitionKey string
	AggregateKey string
	Metric       float64
	BatchID      string
	EventTimeMS  int64
}

// Generate creates a fixed-seed workload with two hot partition keys, a long
// tail of cold keys, eight aggregation groups and metrics spanning 1e-12..1e12.
func Generate(seed int64, count, batchSize int) []Spec {
	if count < 0 {
		count = 0
	}
	if batchSize <= 0 {
		batchSize = 1
	}
	rng := rand.New(rand.NewSource(seed))
	result := make([]Spec, 0, count)
	for i := 0; i < count; i++ {
		var partitionKey string
		switch draw := rng.Intn(100); {
		case draw < 45:
			partitionKey = "hot-a"
		case draw < 80:
			partitionKey = "hot-b"
		default:
			partitionKey = fmt.Sprintf("tail-%03d", rng.Intn(200))
		}
		aggregateKey := fmt.Sprintf("key-%02d", rng.Intn(8))

		exponent := -12 + rng.Intn(25) // inclusive -12..12
		mantissa := 1 + rng.Float64()*9
		value := mantissa * math.Pow10(exponent)
		if rng.Intn(3) == 0 {
			value = -value
		}
		// Inject an exact cancellation companion every 16 events to exercise
		// catastrophic cancellation without changing the group total.
		if i > 0 && i%16 == 0 {
			previous := result[i-1]
			value = -previous.Metric
			aggregateKey = previous.AggregateKey
		}
		result = append(result, Spec{
			EventID:      fmt.Sprintf("evt-%06d", i+1),
			PartitionKey: partitionKey,
			AggregateKey: aggregateKey,
			Metric:       value,
			BatchID:      fmt.Sprintf("batch-%04d", i/batchSize+1),
			EventTimeMS:  BaseEventTimeMS + int64(i)*10,
		})
	}
	return result
}

func ToPublishRequest(requestID, topic string, spec Spec) bus.PublishRequest {
	body, _ := json.Marshal(map[string]any{
		"event_id": spec.EventID,
		"batch_id": spec.BatchID,
		"metric":   spec.Metric,
	})
	return bus.PublishRequest{
		RequestID:    requestID,
		EventID:      spec.EventID,
		Topic:        topic,
		PartitionKey: spec.PartitionKey,
		EventTimeMS:  spec.EventTimeMS,
		BatchID:      spec.BatchID,
		AggregateKey: spec.AggregateKey,
		Metric:       spec.Metric,
		Body:         body,
	}
}

func FixedClock() time.Time {
	return time.UnixMilli(BaseEventTimeMS).UTC()
}
