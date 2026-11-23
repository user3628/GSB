package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type samplePartitionRef struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

type sampleSpec struct {
	Topics    []Topic          `json:"topics"`
	Messages  []PublishRequest `json:"messages"`
	Retention []struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		FromSeq   int64  `json:"from_seq"`
	} `json:"retention,omitempty"`
}

func loadSampleFile(path string) (sampleSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return sampleSpec{}, err
	}
	var spec sampleSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return sampleSpec{}, fmt.Errorf("parse sample %s: %w", path, err)
	}
	return spec, nil
}

func (s *Service) SeedSample(ctx context.Context, spec sampleSpec) error {
	for _, topic := range spec.Topics {
		if _, err := s.CreateTopic(ctx, topic); err != nil {
			return err
		}
	}
	for _, message := range spec.Messages {
		if message.RequestID == "" {
			message.RequestID = "sample-" + digestString(message.MessageID)[:16]
		}
		if _, err := s.Publish(ctx, message); err != nil {
			return fmt.Errorf("seed message %s: %w", message.MessageID, err)
		}
	}
	for i, retention := range spec.Retention {
		if _, err := s.RetainFrom(ctx, retention.Topic, retention.Partition, RetainRequest{
			RequestID: fmt.Sprintf("sample-retain-%d", i),
			FromSeq:   retention.FromSeq,
		}); err != nil {
			return err
		}
	}
	return nil
}
