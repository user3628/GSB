package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"h2diag/internal/engine"
	"h2diag/internal/model"
	"h2diag/internal/store"
)

func TestFullSampleDuplicateAndBusinessErrors(t *testing.T) {
	server, dataStore := newTestServer(t)

	baseline := readSample(t, "baseline.json")
	response, status := postJSON(t, server.URL+"/v1/samples", baseline)
	if status != http.StatusCreated {
		t.Fatalf("baseline status=%d body=%s", status, response)
	}
	var submitted model.SubmitResponse
	if err := json.Unmarshal(response, &submitted); err != nil {
		t.Fatal(err)
	}
	if submitted.Summary.Frames != 200 || submitted.Summary.SchedulingRounds != 60 ||
		submitted.Summary.DATAIntentFrames != 96 || submitted.Version != 65 {
		t.Fatalf("unexpected baseline summary: %#v", submitted)
	}
	for _, stream := range submitted.Snapshot.Streams {
		if stream.PendingBytes != 0 {
			t.Fatalf("pending bytes remained after successful schedule: %#v", stream)
		}
	}
	for _, connection := range submitted.Snapshot.Connections {
		if connection.WindowRemaining != 3_997_822 {
			t.Fatalf("unexpected final connection window: %#v", connection)
		}
	}
	if _, status := getJSON(t, server.URL+"/v1/samples/baseline-v1/verify"); status != http.StatusOK {
		t.Fatalf("verify status=%d", status)
	}

	duplicateBody, duplicateStatus := postJSON(t, server.URL+"/v1/samples", baseline)
	if duplicateStatus != http.StatusOK {
		t.Fatalf("duplicate status=%d body=%s", duplicateStatus, duplicateBody)
	}
	var duplicate model.SubmitResponse
	if err := json.Unmarshal(duplicateBody, &duplicate); err != nil {
		t.Fatal(err)
	}
	if !duplicate.Reused || duplicate.Version != submitted.Version || duplicate.CanonicalHash != submitted.CanonicalHash {
		t.Fatalf("duplicate did not reuse committed result: %#v", duplicate)
	}
	commitsBefore, _ := dataStore.CommitCount(context.Background(), "baseline-v1")
	if _, status := postJSON(t, server.URL+"/v1/samples", baseline); status != http.StatusOK {
		t.Fatalf("third duplicate status=%d", status)
	}
	commitsAfter, _ := dataStore.CommitCount(context.Background(), "baseline-v1")
	if commitsBefore != commitsAfter {
		t.Fatalf("duplicate submission advanced commits: %d -> %d", commitsBefore, commitsAfter)
	}

	assertErrorCode(t, server.URL+"/v1/samples", "blocked.json", model.CodeFlowControlBlocked, http.StatusConflict)
	assertErrorCode(t, server.URL+"/v1/samples", "window-overflow.json", model.CodeWindowOverflow, http.StatusConflict)
	assertErrorCode(t, server.URL+"/v1/samples", "invalid-transition.json", model.CodeInvalidTransition, http.StatusBadRequest)
	assertErrorCode(t, server.URL+"/v1/samples", "invalid-frame.json", model.CodeFrameInvalid, http.StatusBadRequest)
	for _, sampleID := range []string{"blocked-v1", "window-overflow-v1", "invalid-transition-v1", "invalid-frame-v1"} {
		if _, status := getJSON(t, server.URL+"/v1/samples/"+sampleID); status != http.StatusNotFound {
			t.Fatalf("failed sample %s unexpectedly persisted, status=%d", sampleID, status)
		}
	}

	if _, status := postJSON(t, server.URL+"/v1/samples", readSample(t, "conflict-original.json")); status != http.StatusCreated {
		t.Fatalf("conflict original status=%d", status)
	}
	assertErrorCode(t, server.URL+"/v1/samples", "conflict-mutated.json", model.CodeConflictingSample, http.StatusConflict)
}

func TestBatchByStreamIsBitIdenticalToFullSubmission(t *testing.T) {
	server, _ := newTestServer(t)
	baselineBytes := readSample(t, "baseline.json")
	fullResponseBytes, status := postJSON(t, server.URL+"/v1/samples", baselineBytes)
	if status != http.StatusCreated {
		t.Fatalf("full status=%d body=%s", status, fullResponseBytes)
	}
	var fullResponse model.SubmitResponse
	if err := json.Unmarshal(fullResponseBytes, &fullResponse); err != nil {
		t.Fatal(err)
	}

	sample, biz := engine.ParseSample(baselineBytes)
	if biz != nil {
		t.Fatal(biz)
	}
	batchSampleID := "baseline-batch-v1"
	metadata := &model.SampleMetadata{
		Config:      sample.Config,
		Connections: sample.Connections,
		Streams:     sample.Streams,
	}
	groups := make(map[string][]model.Frame)
	for _, frame := range sample.Frames {
		key := fmt.Sprintf("%s/stream-%08d", frame.ConnectionID, frame.StreamID)
		groups[key] = append(groups[key], frame)
	}
	batchIDs := make([]string, 0, len(groups))
	for batchID := range groups {
		batchIDs = append(batchIDs, batchID)
	}
	sort.Strings(batchIDs)
	for index, batchID := range batchIDs {
		request := batchRequest{
			BatchID: batchID,
			Final:   index == len(batchIDs)-1,
			Frames:  groups[batchID],
		}
		if index == 0 {
			request.Metadata = metadata
		}
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		response, status := postJSON(t, server.URL+"/v1/samples/"+batchSampleID+"/batches", body)
		expected := http.StatusAccepted
		if request.Final {
			expected = http.StatusCreated
		}
		if status != expected {
			t.Fatalf("batch %s status=%d body=%s", batchID, status, response)
		}
	}

	fullSnapshot, status := getSnapshot(t, server.URL+"/v1/samples/baseline-v1")
	if status != http.StatusOK {
		t.Fatalf("full snapshot status=%d", status)
	}
	batchSnapshot, status := getSnapshot(t, server.URL+"/v1/samples/"+batchSampleID)
	if status != http.StatusOK {
		t.Fatalf("batch snapshot status=%d", status)
	}
	if fullResponse.Summary != batchSnapshotSummary(t, server.URL, batchSampleID) {
		t.Fatal("full and batch summary differ")
	}
	if fullResponse.Version != batchSnapshot.Snapshot.Version {
		t.Fatalf("version differs: %d vs %d", fullResponse.Version, batchSnapshot.Snapshot.Version)
	}
	for version := int64(0); version <= fullSnapshot.Snapshot.Version; version++ {
		fullAtVersion, status := getSnapshotAtVersion(t, server.URL+"/v1/samples/baseline-v1", version)
		if status != http.StatusOK {
			t.Fatalf("full version %d status=%d", version, status)
		}
		batchAtVersion, status := getSnapshotAtVersion(t, server.URL+"/v1/samples/"+batchSampleID, version)
		if status != http.StatusOK {
			t.Fatalf("batch version %d status=%d", version, status)
		}
		fullAtVersion.Snapshot.SampleID = ""
		batchAtVersion.Snapshot.SampleID = ""
		if !reflect.DeepEqual(fullAtVersion.Snapshot, batchAtVersion.Snapshot) {
			t.Fatalf("snapshot mismatch at version %d\nfull=%#v\nbatch=%#v", version, fullAtVersion.Snapshot, batchAtVersion.Snapshot)
		}
	}

	fullRounds := getRounds(t, server.URL+"/v1/samples/baseline-v1")
	batchRounds := getRounds(t, server.URL+"/v1/samples/"+batchSampleID)
	if !reflect.DeepEqual(fullRounds, batchRounds) {
		t.Fatal("round traces differ between full and batch submissions")
	}
}

func TestConcurrentDuplicateSubmissionAdvancesOnce(t *testing.T) {
	server, dataStore := newTestServer(t)
	body := readSample(t, "conflict-original.json")
	var wait sync.WaitGroup
	statuses := make(chan int, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, status := postJSON(t, server.URL+"/v1/samples", body)
			statuses <- status
		}()
	}
	wait.Wait()
	close(statuses)
	created, reused := 0, 0
	for status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
			reused++
		default:
			t.Fatalf("unexpected concurrent status %d", status)
		}
	}
	if created != 1 || reused != 1 {
		t.Fatalf("created=%d reused=%d, want 1/1", created, reused)
	}
	countBefore, err := dataStore.CommitCount(context.Background(), "conflict-v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, status := postJSON(t, server.URL+"/v1/samples", body); status != http.StatusOK {
		t.Fatalf("duplicate status=%d", status)
	}
	countAfter, _ := dataStore.CommitCount(context.Background(), "conflict-v1")
	if countBefore != countAfter {
		t.Fatalf("duplicate advanced commits: %d -> %d", countBefore, countAfter)
	}
}

func TestCheckpointCorruptionReturnsBusinessCode(t *testing.T) {
	server, dataStore := newTestServer(t)
	if body, status := postJSON(t, server.URL+"/v1/samples", readSample(t, "conflict-original.json")); status != http.StatusCreated {
		t.Fatalf("submit status=%d body=%s", status, body)
	}
	if _, err := dataStore.DB().Exec(`UPDATE streams SET window_remaining=window_remaining+1 WHERE sample_id='conflict-v1'`); err != nil {
		t.Fatal(err)
	}
	body, status := getJSON(t, server.URL+"/v1/samples/conflict-v1/verify")
	if status != http.StatusInternalServerError {
		t.Fatalf("verify status=%d body=%s", status, body)
	}
	var biz model.BizError
	if err := json.Unmarshal(body, &biz); err != nil {
		t.Fatal(err)
	}
	if biz.Code != model.CodeCheckpointCorrupt {
		t.Fatalf("code=%s body=%s", biz.Code, body)
	}
}

func TestSamplingSampleReportsBucketedEstimate(t *testing.T) {
	server, _ := newTestServer(t)
	body, status := postJSON(t, server.URL+"/v1/samples", readSample(t, "sampling.json"))
	if status != http.StatusCreated {
		t.Fatalf("sampling status=%d body=%s", status, body)
	}
	var response model.SubmitResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Summary.SamplingRounds == 0 || response.Summary.DataBytesSent != 917_252 {
		t.Fatalf("unexpected sampling summary: %#v", response.Summary)
	}
	for _, stream := range response.Snapshot.Streams {
		if stream.PendingBytes != 0 {
			t.Fatalf("sampling run left pending data: %#v", stream)
		}
	}
	foundActive := false
	for _, round := range getRounds(t, server.URL+"/v1/samples/sampling-v1") {
		if !round.Trace.Sampling.Active {
			continue
		}
		foundActive = true
		if round.Trace.Sampling.SamplingRatePPM >= model.FixedScalePPM ||
			round.Trace.Sampling.BucketMethod == "" ||
			round.Trace.Sampling.Confidence == "" ||
			round.Trace.Sampling.DifferenceReason == "" ||
			len(round.Trace.Sampling.ExactOrder) == 0 ||
			len(round.Trace.Sampling.EstimatedOrder) == 0 {
			t.Fatalf("incomplete sampling audit in round %d: %#v", round.RoundNo, round.Trace.Sampling)
		}
	}
	if !foundActive {
		t.Fatal("no sampling round was persisted")
	}
}

func TestQueriesObserveOnlyCommittedSnapshotsWhileAdvancing(t *testing.T) {
	server, _ := newTestServer(t)
	body := readSample(t, "baseline.json")
	done := make(chan struct {
		body   []byte
		status int
		err    error
	}, 1)
	go func() {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/samples", bytes.NewReader(body))
		if err != nil {
			done <- struct {
				body   []byte
				status int
				err    error
			}{err: err}
			return
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			done <- struct {
				body   []byte
				status int
				err    error
			}{err: err}
			return
		}
		defer response.Body.Close()
		responseBody, readErr := io.ReadAll(response.Body)
		done <- struct {
			body   []byte
			status int
			err    error
		}{body: responseBody, status: response.StatusCode, err: readErr}
	}()

	lastVersion := int64(-1)
	finalStatus := 0
	for {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatal(result.err)
			}
			finalStatus = result.status
			if result.status != http.StatusCreated {
				t.Fatalf("submit status=%d body=%s", result.status, result.body)
			}
		default:
			responseBody, status := getJSON(t, server.URL+"/v1/samples/baseline-v1")
			if status == http.StatusNotFound {
				continue
			}
			if status != http.StatusOK {
				t.Fatalf("snapshot query returned %d: %s", status, responseBody)
			}
			var response model.SnapshotResponse
			if err := json.Unmarshal(responseBody, &response); err != nil {
				t.Fatal(err)
			}
			if response.Snapshot.Version < lastVersion {
				t.Fatalf("snapshot version moved backwards: %d -> %d", lastVersion, response.Snapshot.Version)
			}
			lastVersion = response.Snapshot.Version
			continue
		}
		break
	}
	if finalStatus != http.StatusCreated {
		t.Fatalf("final submit status=%d", finalStatus)
	}
	finalSnapshot, status := getSnapshot(t, server.URL+"/v1/samples/baseline-v1")
	if status != http.StatusOK || finalSnapshot.Status != "DONE" || finalSnapshot.Snapshot.Version != 65 {
		t.Fatalf("unexpected final snapshot: status=%d response=%#v", status, finalSnapshot)
	}
}

func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "h2diag.sqlite")
	dataStore, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataStore.Close() })
	server := httptest.NewServer(New(dataStore).Handler())
	t.Cleanup(server.Close)
	return server, dataStore
}

func readSample(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "samples", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func postJSON(t *testing.T, url string, body []byte) ([]byte, int) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return responseBody, response.StatusCode
}

func getJSON(t *testing.T, url string) ([]byte, int) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body, response.StatusCode
}

func assertErrorCode(t *testing.T, url, sampleName, code string, status int) {
	t.Helper()
	body, gotStatus := postJSON(t, url, readSample(t, sampleName))
	if gotStatus != status {
		t.Fatalf("%s status=%d want=%d body=%s", sampleName, gotStatus, status, body)
	}
	var biz model.BizError
	if err := json.Unmarshal(body, &biz); err != nil {
		t.Fatal(err)
	}
	if biz.Code != code {
		t.Fatalf("%s code=%s want=%s body=%s", sampleName, biz.Code, code, body)
	}
}

func getSnapshot(t *testing.T, url string) (*model.SnapshotResponse, int) {
	t.Helper()
	body, status := getJSON(t, url)
	if status != http.StatusOK {
		return nil, status
	}
	var response model.SnapshotResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	return &response, status
}

func getSnapshotAtVersion(t *testing.T, baseURL string, version int64) (*model.SnapshotResponse, int) {
	t.Helper()
	return getSnapshot(t, baseURL+"?version="+strconv.FormatInt(version, 10))
}

func batchSnapshotSummary(t *testing.T, baseURL, sampleID string) model.RunSummary {
	t.Helper()
	body, status := getJSON(t, baseURL+"/v1/samples/"+sampleID)
	if status != http.StatusOK {
		t.Fatalf("batch sample status=%d body=%s", status, body)
	}
	var response model.SnapshotResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Summary == nil {
		t.Fatal("batch response has no summary")
	}
	return *response.Summary
}

func getRounds(t *testing.T, url string) []model.StoredRound {
	t.Helper()
	body, status := getJSON(t, url+"/rounds?limit=5000")
	if status != http.StatusOK {
		t.Fatalf("rounds status=%d body=%s", status, body)
	}
	var response struct {
		Rounds []model.StoredRound `json:"rounds"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	return response.Rounds
}
