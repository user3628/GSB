package bus

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"local/eventbus/internal/agg"
)

func (s *Service) CreateTopic(ctx context.Context, request CreateTopicRequest) (Topic, error) {
	request.Topic = strings.TrimSpace(request.Topic)
	if request.Topic == "" {
		return Topic{}, invalidArgument("topic is required")
	}
	if request.Partitions <= 0 {
		return Topic{}, invalidArgument("partitions must be greater than zero")
	}
	if len(request.Groups) == 0 {
		return Topic{}, invalidArgument("at least one consumer group is required")
	}
	seenGroups := make(map[string]struct{}, len(request.Groups))
	for i := range request.Groups {
		request.Groups[i].Name = strings.TrimSpace(request.Groups[i].Name)
		if request.Groups[i].Name == "" {
			return Topic{}, invalidArgument("group name is required")
		}
		if _, exists := seenGroups[request.Groups[i].Name]; exists {
			return Topic{}, invalidArgument("duplicate group: %s", request.Groups[i].Name)
		}
		seenGroups[request.Groups[i].Name] = struct{}{}
		if request.Groups[i].Algorithm == "" {
			request.Groups[i].Algorithm = string(agg.Kahan)
		}
		if _, err := agg.ParseAlgorithm(request.Groups[i].Algorithm); err != nil {
			return Topic{}, invalidArgument("%s", err.Error())
		}
	}
	hash := contentHash("create_topic", request)
	return idempotentJSON(s, ctx, "create_topic", request.RequestID, hash, func() (Topic, error) {
		existing, err := s.getTopic(ctx, request.Topic)
		if err == nil {
			if sameTopic(existing, request) {
				return existing, nil
			}
			return Topic{}, NewError(CodeConflictingRequest, "topic already exists with different configuration")
		}
		if ErrorCode(err) != CodeNotFound {
			return Topic{}, err
		}

		it, err := s.beginIntent(ctx, "CREATE_TOPIC", request.RequestID, hash, request.Topic, "", 0, nil, request)
		if err != nil {
			return Topic{}, err
		}
		if err := s.injectFault("create_topic_after_intent"); err != nil {
			return Topic{}, err
		}
		now := s.nowMS()
		tx, release, err := s.beginWriteTx(ctx)
		if err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return Topic{}, WrapError(CodeInternal, "begin topic transaction", err)
		}
		defer release()
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO topics(topic, partitions, created_at_ms, intent_id) VALUES(?, ?, ?, ?)`,
			request.Topic, request.Partitions, now, it.ID); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return Topic{}, WrapError(CodeInternal, "insert topic", err)
		}
		for _, group := range request.Groups {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO topic_groups(topic, group_name, algorithm, created_at_ms, intent_id) VALUES(?, ?, ?, ?, ?)`,
				request.Topic, group.Name, group.Algorithm, now, it.ID); err != nil {
				_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
				return Topic{}, WrapError(CodeInternal, "insert topic group", err)
			}
		}
		for partition := 0; partition < request.Partitions; partition++ {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO partition_counters(topic, partition, last_seq, earliest_seq, last_intent_id) VALUES(?, ?, 0, 1, ?)`,
				request.Topic, partition, it.ID); err != nil {
				_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
				return Topic{}, WrapError(CodeInternal, "insert partition counter", err)
			}
		}
		if err := tx.Commit(); err != nil {
			_ = s.finishIntent(context.WithoutCancel(ctx), it.ID, "ROLLED_BACK")
			return Topic{}, WrapError(CodeInternal, "commit topic", err)
		}
		release()
		if err := s.injectFault("create_topic_after_commit"); err != nil {
			return Topic{}, err
		}
		if err := s.finishIntent(ctx, it.ID, "COMMITTED"); err != nil {
			return Topic{}, err
		}
		return Topic{
			Topic:      request.Topic,
			Partitions: request.Partitions,
			Groups:     groupsFromRequest(request.Groups),
			CreatedAt:  now,
		}, nil
	})
}

func (s *Service) GetTopic(ctx context.Context, name string) (Topic, error) {
	return s.getTopic(ctx, name)
}

func (s *Service) getTopic(ctx context.Context, name string) (Topic, error) {
	var topic Topic
	if err := s.db.QueryRowContext(ctx,
		`SELECT topic, partitions, created_at_ms FROM topics WHERE topic = ?`, name).
		Scan(&topic.Topic, &topic.Partitions, &topic.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return Topic{}, NewError(CodeNotFound, "topic does not exist: "+name)
		}
		return Topic{}, WrapError(CodeInternal, "read topic", err)
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_name, algorithm FROM topic_groups WHERE topic = ? ORDER BY group_name`, name)
	if err != nil {
		return Topic{}, WrapError(CodeInternal, "read topic groups", err)
	}
	defer rows.Close()
	for rows.Next() {
		var group TopicGroup
		if err := rows.Scan(&group.Name, &group.Algorithm); err != nil {
			return Topic{}, WrapError(CodeInternal, "scan topic group", err)
		}
		topic.Groups = append(topic.Groups, group)
	}
	if err := rows.Err(); err != nil {
		return Topic{}, WrapError(CodeInternal, "iterate topic groups", err)
	}
	return topic, nil
}

func sameTopic(existing Topic, request CreateTopicRequest) bool {
	if existing.Partitions != request.Partitions || len(existing.Groups) != len(request.Groups) {
		return false
	}
	existingGroups := make(map[string]string, len(existing.Groups))
	for _, group := range existing.Groups {
		existingGroups[group.Name] = string(group.Algorithm)
	}
	for _, group := range request.Groups {
		if existingGroups[group.Name] != group.Algorithm {
			return false
		}
	}
	return true
}

func groupsFromRequest(groups []GroupSpec) []TopicGroup {
	result := make([]TopicGroup, 0, len(groups))
	for _, group := range groups {
		result = append(result, TopicGroup{Name: group.Name, Algorithm: agg.Algorithm(group.Algorithm)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}
