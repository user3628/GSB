package bus

import (
	"context"
	"strings"
)

func (s *Service) SetAlgorithm(ctx context.Context, request SetAlgorithmRequest) (SetAlgorithmResult, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	request.Group = strings.TrimSpace(request.Group)
	algorithm, err := algorithmFromString(request.Algorithm)
	if err != nil {
		return SetAlgorithmResult{}, err
	}
	if request.Topic == "" || request.Group == "" {
		return SetAlgorithmResult{}, invalidArgument("topic and group are required")
	}
	hash := contentHash("set_algorithm", request)
	return idempotentJSON(s, ctx, "set_algorithm", request.RequestID, hash, func() (SetAlgorithmResult, error) {
		lock := s.partitionLock(request.Topic, request.Partition, request.Group)
		lock.Lock()
		defer lock.Unlock()
		if err := s.validateTopicGroup(ctx, s.db, request.Topic, request.Group); err != nil {
			return SetAlgorithmResult{}, err
		}
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, request.Partition); err != nil {
			return SetAlgorithmResult{}, err
		}
		offset, err := s.loadOffset(ctx, s.db, request.Topic, request.Group, request.Partition)
		if err != nil {
			return SetAlgorithmResult{}, err
		}
		if offset.AckedOffset != 0 {
			return SetAlgorithmResult{}, NewError(CodeAlgorithmLocked, "aggregation algorithm is locked after the first acknowledgement")
		}
		it, err := s.beginIntent(ctx, "SET_ALGORITHM", request.RequestID, hash,
			request.Topic, request.Group, request.Partition, nil, request)
		if err != nil {
			return SetAlgorithmResult{}, err
		}
		if _, err := s.execWrite(ctx, `
			UPDATE topic_groups SET algorithm = ?, intent_id = ? WHERE topic = ? AND group_name = ?`,
			algorithm, it.ID, request.Topic, request.Group); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return SetAlgorithmResult{}, WrapError(CodeInternal, "set aggregation algorithm", err)
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return SetAlgorithmResult{}, err
		}
		return SetAlgorithmResult{
			Topic: request.Topic, Group: request.Group, Partition: request.Partition, Algorithm: algorithm,
		}, nil
	})
}

func (s *Service) SetRetention(ctx context.Context, request SetRetentionRequest) (SetRetentionResult, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	if request.Topic == "" || request.EarliestRetain <= 0 {
		return SetRetentionResult{}, invalidArgument("topic and positive earliest_retained_seq are required")
	}
	hash := contentHash("set_retention", request)
	return idempotentJSON(s, ctx, "set_retention", request.RequestID, hash, func() (SetRetentionResult, error) {
		lock := s.partitionLock(request.Topic, request.Partition, "")
		lock.Lock()
		defer lock.Unlock()
		s.publishMu.Lock()
		defer s.publishMu.Unlock()
		if err := s.validateTopicPartition(ctx, s.db, request.Topic, request.Partition); err != nil {
			return SetRetentionResult{}, err
		}
		var lastSeq, earliest int64
		if err := s.db.QueryRowContext(ctx,
			`SELECT last_seq, earliest_seq FROM partition_counters WHERE topic = ? AND partition = ?`,
			request.Topic, request.Partition).Scan(&lastSeq, &earliest); err != nil {
			return SetRetentionResult{}, WrapError(CodeInternal, "read partition counter", err)
		}
		if request.EarliestRetain < earliest || request.EarliestRetain > lastSeq+1 {
			return SetRetentionResult{}, invalidArgument("earliest retained sequence must be within [%d,%d]", earliest, lastSeq+1)
		}
		it, err := s.beginIntent(ctx, "SET_RETENTION", request.RequestID, hash,
			request.Topic, "", request.Partition, map[string]int64{"earliest_seq": earliest}, request)
		if err != nil {
			return SetRetentionResult{}, err
		}
		if _, err := s.execWrite(ctx, `
			UPDATE partition_counters SET earliest_seq = ?, last_intent_id = ?
			WHERE topic = ? AND partition = ?`, request.EarliestRetain, it.ID, request.Topic, request.Partition); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return SetRetentionResult{}, WrapError(CodeInternal, "set retention floor", err)
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return SetRetentionResult{}, err
		}
		return SetRetentionResult{
			Topic: request.Topic, Partition: request.Partition, EarliestRetain: request.EarliestRetain,
		}, nil
	})
}
