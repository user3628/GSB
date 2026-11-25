package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"local/eventbus/internal/agg"
	"local/eventbus/internal/bus"
	"local/eventbus/internal/httpapi"
	"local/eventbus/internal/sample"
)

type apiEnvelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type apiClient struct {
	baseURL string
	counter atomic.Int64
	client  *http.Client
}

func (c *apiClient) newID(prefix string) string {
	return fmt.Sprintf("%s-%06d", prefix, c.counter.Add(1))
}

func (c *apiClient) request(method, path string, payload any) (json.RawMessage, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var envelope apiEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	if !envelope.OK {
		return nil, &apiError{status: response.StatusCode, code: envelope.Error.Code, message: envelope.Error.Message}
	}
	return envelope.Result, nil
}

func (c *apiClient) post(path string, payload, result any) error {
	raw, err := c.request(http.MethodPost, path, payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, result)
}

func (c *apiClient) get(path string, result any) error {
	raw, err := c.request(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, result)
}

type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("HTTP %d %s: %s", e.status, e.code, e.message)
}

type partitionReport struct {
	Partition int   `json:"partition"`
	Events    int   `json:"events"`
	FirstSeq  int64 `json:"first_seq"`
	LastSeq   int64 `json:"last_seq"`
}

type algorithmReport struct {
	Algorithm     agg.Algorithm `json:"algorithm"`
	SplitDigest   string        `json:"split_digest"`
	BulkDigest    string        `json:"bulk_digest"`
	Equivalent    bool          `json:"split_bulk_bitwise_equivalent"`
	EventCount    int64         `json:"event_count"`
	SumAbs        float64       `json:"sum_abs"`
	AbsoluteBound float64       `json:"absolute_error_bound"`
	RelativeBound float64       `json:"relative_error_bound"`
}

type rewindReport struct {
	Partition             int    `json:"partition"`
	TargetSeq             int64  `json:"target_seq"`
	FullOffset            int64  `json:"full_offset"`
	RevertedBatches       int    `json:"reverted_batches"`
	BaselineDigest        string `json:"baseline_digest_at_target"`
	AfterRewindDigest     string `json:"digest_after_rewind"`
	AfterReplayDigest     string `json:"digest_after_replay"`
	MatchesDirectAtTarget bool   `json:"matches_direct_consumption_at_target"`
	MatchesFullBefore     bool   `json:"matches_pre_rewind_full_state"`
}

type demoReport struct {
	Seed                   int64             `json:"seed"`
	EventCount             int               `json:"event_count"`
	Partitions             []partitionReport `json:"partitions"`
	Algorithms             []algorithmReport `json:"algorithms"`
	Rewind                 rewindReport      `json:"rewind"`
	ErrorLedgerBatches     int               `json:"error_ledger_batches"`
	LedgerAbsoluteBound    float64           `json:"ledger_absolute_bound"`
	RecoveryPendingScanned int               `json:"recovery_pending_scanned"`
	ValidationPassed       bool              `json:"validation_passed"`
}

func main() {
	seed := flag.Int64("seed", 20261125, "deterministic sample seed")
	eventCount := flag.Int("events", 64, "number of sample events")
	flag.Parse()

	if err := run(*seed, *eventCount); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

func run(seed int64, eventCount int) error {
	tempDir, err := os.MkdirTemp("", "eventbus-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	service, err := bus.Open(context.Background(), bus.ServiceOptions{
		DBPath: filepath.Join(tempDir, "demo.sqlite"), LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	defer service.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	httpServer := &http.Server{Handler: httpapi.New(service)}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Shutdown(context.Background())

	client := &apiClient{
		baseURL: "http://" + listener.Addr().String(),
		client:  &http.Client{Timeout: 15 * time.Second},
	}
	groups := []bus.GroupSpec{
		{Name: "naive-split", Algorithm: string(agg.Naive)},
		{Name: "naive-bulk", Algorithm: string(agg.Naive)},
		{Name: "kahan-split", Algorithm: string(agg.Kahan)},
		{Name: "kahan-bulk", Algorithm: string(agg.Kahan)},
		{Name: "magnitude-split", Algorithm: string(agg.Magnitude)},
		{Name: "magnitude-bulk", Algorithm: string(agg.Magnitude)},
		{Name: "baseline-magnitude", Algorithm: string(agg.Magnitude)},
	}
	var topic bus.Topic
	if err := client.post("/v1/topics", bus.CreateTopicRequest{
		RequestID: client.newID("topic"), Topic: "demo", Partitions: 2, Groups: groups,
	}, &topic); err != nil {
		return err
	}

	specs := sample.Generate(seed, eventCount, 8)
	perPartition := make(map[int][]bus.PublishResult)
	for _, spec := range specs {
		var published bus.PublishResult
		if err := client.post("/v1/events/publish", sample.ToPublishRequest(client.newID("publish"), "demo", spec), &published); err != nil {
			return err
		}
		perPartition[published.Partition] = append(perPartition[published.Partition], published)
	}
	var partitionReports []partitionReport
	for partition := 0; partition < topic.Partitions; partition++ {
		items := perPartition[partition]
		if len(items) == 0 {
			partitionReports = append(partitionReports, partitionReport{Partition: partition})
			continue
		}
		partitionReports = append(partitionReports, partitionReport{
			Partition: partition, Events: len(items), FirstSeq: items[0].Seq, LastSeq: items[len(items)-1].Seq,
		})
	}

	for _, group := range []string{"naive-split", "kahan-split", "magnitude-split"} {
		for partition := 0; partition < topic.Partitions; partition++ {
			if _, err := consumeAll(client, group, partition, 1); err != nil {
				return fmt.Errorf("consume %s/%d: %w", group, partition, err)
			}
		}
	}
	for _, group := range []string{"naive-bulk", "kahan-bulk", "magnitude-bulk"} {
		for partition := 0; partition < topic.Partitions; partition++ {
			if _, err := consumeAll(client, group, partition, 256); err != nil {
				return fmt.Errorf("consume %s/%d: %w", group, partition, err)
			}
		}
	}

	report := demoReport{
		Seed: seed, EventCount: eventCount, Partitions: partitionReports, ValidationPassed: true,
	}
	for _, pair := range []struct {
		algorithm agg.Algorithm
		split     string
		bulk      string
	}{
		{agg.Naive, "naive-split", "naive-bulk"},
		{agg.Kahan, "kahan-split", "kahan-bulk"},
		{agg.Magnitude, "magnitude-split", "magnitude-bulk"},
	} {
		split, err := aggregate(client, pair.split, 0)
		if err != nil {
			return err
		}
		bulk, err := aggregate(client, pair.bulk, 0)
		if err != nil {
			return err
		}
		equivalent := split.StateDigest == bulk.StateDigest
		if !equivalent {
			report.ValidationPassed = false
		}
		report.Algorithms = append(report.Algorithms, algorithmReport{
			Algorithm: pair.algorithm, SplitDigest: split.StateDigest, BulkDigest: bulk.StateDigest,
			Equivalent: equivalent, EventCount: split.EventCount, SumAbs: split.SumAbs,
			AbsoluteBound: split.AbsErrorBound, RelativeBound: split.RelativeBound,
		})
	}

	magnitudeFull, err := aggregate(client, "magnitude-split", 0)
	if err != nil {
		return err
	}
	if magnitudeFull.Offset < 2 {
		return fmt.Errorf("partition 0 has too few events for rewind demonstration")
	}
	target := magnitudeFull.Offset / 2
	if _, err := consumeExactly(client, "baseline-magnitude", 0, target); err != nil {
		return err
	}
	baselineAtTarget, err := aggregate(client, "baseline-magnitude", 0)
	if err != nil {
		return err
	}
	var rewind bus.RewindResult
	if err := client.post("/v1/rewind", bus.RewindRequest{
		RequestID: client.newID("rewind"), Topic: "demo", Group: "magnitude-split", Partition: 0, TargetSeq: target,
	}, &rewind); err != nil {
		return err
	}
	afterRewind, err := aggregate(client, "magnitude-split", 0)
	if err != nil {
		return err
	}
	if _, err := consumeAll(client, "magnitude-split", 0, 2); err != nil {
		return err
	}
	afterReplay, err := aggregate(client, "magnitude-split", 0)
	if err != nil {
		return err
	}
	report.Rewind = rewindReport{
		Partition: 0, TargetSeq: target, FullOffset: magnitudeFull.Offset,
		RevertedBatches: rewind.RevertedBatches,
		BaselineDigest:  baselineAtTarget.StateDigest, AfterRewindDigest: afterRewind.StateDigest,
		AfterReplayDigest:     afterReplay.StateDigest,
		MatchesDirectAtTarget: baselineAtTarget.StateDigest == afterRewind.StateDigest,
		MatchesFullBefore:     afterReplay.StateDigest == magnitudeFull.StateDigest,
	}
	if !report.Rewind.MatchesDirectAtTarget || !report.Rewind.MatchesFullBefore {
		report.ValidationPassed = false
	}

	var ledger bus.ErrorLedger
	if err := client.get("/v1/errors?topic=demo&group=magnitude-split&partition=0", &ledger); err != nil {
		return err
	}
	report.ErrorLedgerBatches = len(ledger.Batches)
	report.LedgerAbsoluteBound = ledger.LedgerAbsBound
	var recovery bus.RecoveryReport
	if err := client.get("/v1/recovery", &recovery); err != nil {
		return err
	}
	report.RecoveryPendingScanned = recovery.PendingScanned

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	if !report.ValidationPassed {
		return errors.New("demo validation failed")
	}
	return nil
}

func consumeAll(client *apiClient, group string, partition, batchSize int) (int, error) {
	total := 0
	for {
		pulled, err := pull(client, group, partition, batchSize)
		if err != nil {
			var apiErr *apiError
			if errors.As(err, &apiErr) && apiErr.code == bus.CodeNoEvent {
				return total, nil
			}
			return total, err
		}
		seqs := make([]int64, 0, len(pulled.Events))
		for _, event := range pulled.Events {
			seqs = append(seqs, event.Seq)
		}
		if err := ack(client, group, partition, pulled.LeaseToken, seqs); err != nil {
			return total, err
		}
		total += len(seqs)
	}
}

func consumeExactly(client *apiClient, group string, partition int, count int64) (int, error) {
	var total int64
	for total < count {
		size := int(count - total)
		if size > 16 {
			size = 16
		}
		pulled, err := pull(client, group, partition, size)
		if err != nil {
			return int(total), err
		}
		seqs := make([]int64, 0, len(pulled.Events))
		for _, event := range pulled.Events {
			seqs = append(seqs, event.Seq)
		}
		if int64(len(seqs)) > count-total {
			return int(total), errors.New("pull exceeded requested exact count")
		}
		if err := ack(client, group, partition, pulled.LeaseToken, seqs); err != nil {
			return int(total), err
		}
		total += int64(len(seqs))
	}
	return int(total), nil
}

func pull(client *apiClient, group string, partition, batchSize int) (bus.PullResult, error) {
	var result bus.PullResult
	err := client.post("/v1/pull", bus.PullRequest{
		RequestID: client.newID("pull"), Topic: "demo", Group: group, Partition: partition,
		ConsumerID: group + "-consumer", MaxEvents: batchSize,
	}, &result)
	return result, err
}

func ack(client *apiClient, group string, partition int, leaseToken string, seqs []int64) error {
	var result bus.AckResult
	return client.post("/v1/ack", bus.AckRequest{
		RequestID: client.newID("ack"), Topic: "demo", Group: group, Partition: partition,
		ConsumerID: group + "-consumer", LeaseToken: leaseToken, Seqs: seqs,
		AckBatchID: client.newID("batch"),
	}, &result)
}

func aggregate(client *apiClient, group string, partition int) (bus.AggregateResult, error) {
	var result bus.AggregateResult
	path := fmt.Sprintf("/v1/aggregate?topic=demo&group=%s&partition=%d", group, partition)
	err := client.get(path, &result)
	return result, err
}
