package querysvc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/model"
	"ocrlab/internal/pipeline"
)

const (
	KindBoxes       = "boxes"
	KindRecognition = "recognition"
	KindImage       = "image"
	KindDiagnostics = "diagnostics"
)

type Service struct {
	pipeline *pipeline.Pipeline
}

type Request struct {
	Kind           string
	ImageID        string
	Group          string
	BudgetMS       *int
	ForceRecompute bool
}

type CacheKeyParts struct {
	Kind           string `json:"kind"`
	ImageID        string `json:"image_id,omitempty"`
	Group          string `json:"group,omitempty"`
	ContentSHA256  string `json:"content_sha256"`
	ConfigVersion  string `json:"config_version"`
	StageVersion   string `json:"stage_version"`
	ArgumentDigest string `json:"argument_digest"`
}

type Response struct {
	QueryID      string          `json:"query_id"`
	Kind         string          `json:"kind"`
	CacheHit     bool            `json:"cache_hit"`
	BudgetMS     int             `json:"budget_ms"`
	ElapsedMS    float64         `json:"elapsed_ms"`
	CacheKey     string          `json:"cache_key,omitempty"`
	KeyParts     CacheKeyParts   `json:"cache_key_parts"`
	Result       json.RawMessage `json:"result,omitempty"`
	ResultSHA256 string          `json:"result_sha256,omitempty"`
}

type imageQueryResult struct {
	Detection   json.RawMessage `json:"detection"`
	Recognition json.RawMessage `json:"recognition"`
	Strategy    json.RawMessage `json:"strategy"`
	Diagnostic  json.RawMessage `json:"diagnostic"`
}

type scopeDescriptor struct {
	Kind          string
	ImageID       string
	Group         string
	ContentSHA256 string
	StageVersion  string
}

func New(p *pipeline.Pipeline) *Service { return &Service{pipeline: p} }

func (s *Service) Pipeline() *pipeline.Pipeline { return s.pipeline }

func (s *Service) Execute(ctx context.Context, request Request) (Response, error) {
	start := time.Now()
	queryID := newQueryID()
	budget := s.pipeline.Config().Query.LatencyBudgetMS
	if request.BudgetMS != nil {
		budget = *request.BudgetMS
	}
	if budget < 0 || budget > s.pipeline.Config().Query.MaxBudgetMS {
		return Response{QueryID: queryID, Kind: request.Kind, BudgetMS: budget}, apperr.New(apperr.CodeInvalidArgument, "budget_ms is outside the configured range")
	}
	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(budget)*time.Millisecond)
	defer cancel()
	type outcome struct {
		response Response
		err      error
	}
	channel := make(chan outcome, 1)
	go func() {
		response, err := s.compute(queryCtx, queryID, request, budget)
		channel <- outcome{response: response, err: err}
	}()
	select {
	case result := <-channel:
		if result.response.QueryID == "" {
			result.response.QueryID = queryID
			result.response.Kind = request.Kind
			result.response.BudgetMS = budget
		}
		result.response.ElapsedMS = elapsedMS(start)
		if result.err != nil {
			return annotateError(result.response, result.err)
		}
		if budget == 0 || result.response.ElapsedMS > float64(budget) {
			latencyErr := &apperr.Error{
				Code: apperr.CodeLatencyBudget, Message: "query exceeded the configured latency budget",
				QueryID: queryID, ElapsedMS: result.response.ElapsedMS,
			}
			return Response{QueryID: queryID, Kind: request.Kind, BudgetMS: budget, ElapsedMS: result.response.ElapsedMS}, latencyErr
		}
		return result.response, nil
	case <-queryCtx.Done():
		elapsed := elapsedMS(start)
		latencyErr := &apperr.Error{
			Code: apperr.CodeLatencyBudget, Message: "query exceeded the configured latency budget before completing",
			QueryID: queryID, ElapsedMS: elapsed,
		}
		return Response{QueryID: queryID, Kind: request.Kind, BudgetMS: budget, ElapsedMS: elapsed}, latencyErr
	}
}

func (s *Service) compute(ctx context.Context, queryID string, request Request, budget int) (Response, error) {
	if request.Kind == "" {
		return Response{}, apperr.New(apperr.CodeInvalidArgument, "query kind is required")
	}
	descriptor, err := s.describe(ctx, request)
	if err != nil {
		return Response{}, err
	}
	argumentDigest, err := canonical.Digest(struct {
		Kind    string `json:"kind"`
		ImageID string `json:"image_id"`
		Group   string `json:"group"`
	}{Kind: descriptor.Kind, ImageID: descriptor.ImageID, Group: descriptor.Group})
	if err != nil {
		return Response{}, err
	}
	parts := CacheKeyParts{
		Kind: descriptor.Kind, ImageID: descriptor.ImageID, Group: descriptor.Group,
		ContentSHA256: descriptor.ContentSHA256, ConfigVersion: s.pipeline.Config().Version(),
		StageVersion: descriptor.StageVersion, ArgumentDigest: argumentDigest,
	}
	key, err := canonical.Digest(parts)
	if err != nil {
		return Response{}, err
	}
	response := Response{QueryID: queryID, Kind: request.Kind, BudgetMS: budget, CacheKey: key, KeyParts: parts}
	if s.pipeline.Config().Query.CacheEnabled && !request.ForceRecompute {
		cached, hit, err := s.pipeline.Store().CacheGet(ctx, key)
		if err != nil {
			return response, err
		}
		if hit {
			response.CacheHit = true
			response.Result = append(json.RawMessage(nil), cached...)
			response.ResultSHA256 = canonical.DigestBytes(cached)
			return response, nil
		}
	}
	result, err := s.buildResult(ctx, request)
	if err != nil {
		return response, err
	}
	if s.pipeline.Config().Query.CacheEnabled {
		_ = s.pipeline.Store().CacheSet(ctx, key, descriptor.ImageID, request.Kind, result)
	}
	response.Result = append(json.RawMessage(nil), result...)
	response.ResultSHA256 = canonical.DigestBytes(result)
	return response, nil
}

func (s *Service) describe(ctx context.Context, request Request) (scopeDescriptor, error) {
	switch request.Kind {
	case KindBoxes:
		if request.ImageID == "" {
			return scopeDescriptor{}, apperr.New(apperr.CodeInvalidArgument, "boxes query requires image_id")
		}
		var product model.DetectionProduct
		checkpoint, _, err := s.pipeline.LoadProduct(ctx, request.ImageID, model.StageDetect, &product)
		if err != nil {
			return scopeDescriptor{}, err
		}
		return scopeDescriptor{Kind: request.Kind, ImageID: request.ImageID, ContentSHA256: product.ContentSHA256, StageVersion: checkpoint.ProductDigest}, nil
	case KindRecognition:
		if request.ImageID == "" {
			return scopeDescriptor{}, apperr.New(apperr.CodeInvalidArgument, "recognition query requires image_id")
		}
		var product model.RecognitionProduct
		checkpoint, _, err := s.pipeline.LoadProduct(ctx, request.ImageID, model.StageRecognize, &product)
		if err != nil {
			return scopeDescriptor{}, err
		}
		return scopeDescriptor{Kind: request.Kind, ImageID: request.ImageID, ContentSHA256: product.ContentSHA256, StageVersion: checkpoint.ProductDigest}, nil
	case KindImage:
		if request.ImageID == "" {
			return scopeDescriptor{}, apperr.New(apperr.CodeInvalidArgument, "image query requires image_id")
		}
		record, err := s.pipeline.Store().GetImage(ctx, request.ImageID)
		if err != nil {
			return scopeDescriptor{}, err
		}
		versions := make([]string, 0, 4)
		for _, stage := range []string{model.StageDetect, model.StageRecognize, model.StageStrategy, model.StageDiagnostic} {
			var raw json.RawMessage
			_, digest, err := s.pipeline.LoadProduct(ctx, request.ImageID, stage, &raw)
			if err != nil {
				return scopeDescriptor{}, err
			}
			versions = append(versions, digest)
		}
		stageVersion, err := canonical.Digest(versions)
		if err != nil {
			return scopeDescriptor{}, err
		}
		return scopeDescriptor{Kind: request.Kind, ImageID: request.ImageID, ContentSHA256: record.ContentSHA256, StageVersion: stageVersion}, nil
	case KindDiagnostics:
		return s.describeDiagnostics(ctx, request)
	default:
		return scopeDescriptor{}, apperr.New(apperr.CodeInvalidArgument, "unknown query kind: "+request.Kind)
	}
}

func (s *Service) describeDiagnostics(ctx context.Context, request Request) (scopeDescriptor, error) {
	imageIDs := make([]string, 0)
	if request.ImageID != "" {
		imageIDs = append(imageIDs, request.ImageID)
	} else {
		for _, imageSpec := range s.pipeline.Manifest().Images {
			if request.Group != "" && imageSpec.Group != request.Group {
				continue
			}
			imageIDs = append(imageIDs, imageSpec.ImageID)
		}
	}
	if len(imageIDs) == 0 {
		return scopeDescriptor{}, apperr.New(apperr.CodeNotFound, "no images match the diagnostics query")
	}
	sort.Strings(imageIDs)
	type contentEntry struct {
		ImageID string `json:"image_id"`
		SHA256  string `json:"content_sha256"`
	}
	type versionEntry struct {
		ImageID string `json:"image_id"`
		Version string `json:"stage_version"`
	}
	contents := make([]contentEntry, 0, len(imageIDs))
	versions := make([]versionEntry, 0, len(imageIDs))
	for _, imageID := range imageIDs {
		record, err := s.pipeline.Store().GetImage(ctx, imageID)
		if err != nil {
			return scopeDescriptor{}, err
		}
		var product model.ImageDiagnostic
		checkpoint, _, err := s.pipeline.LoadProduct(ctx, imageID, model.StageDiagnostic, &product)
		if err != nil {
			return scopeDescriptor{}, err
		}
		contents = append(contents, contentEntry{ImageID: imageID, SHA256: record.ContentSHA256})
		versions = append(versions, versionEntry{ImageID: imageID, Version: checkpoint.ProductDigest})
	}
	contentDigest, err := canonical.Digest(contents)
	if err != nil {
		return scopeDescriptor{}, err
	}
	stageDigest, err := canonical.Digest(versions)
	if err != nil {
		return scopeDescriptor{}, err
	}
	return scopeDescriptor{Kind: request.Kind, ImageID: request.ImageID, Group: request.Group, ContentSHA256: contentDigest, StageVersion: stageDigest}, nil
}

func (s *Service) buildResult(ctx context.Context, request Request) ([]byte, error) {
	switch request.Kind {
	case KindBoxes:
		var product model.DetectionProduct
		_, _, err := s.pipeline.LoadProduct(ctx, request.ImageID, model.StageDetect, &product)
		if err != nil {
			return nil, err
		}
		return canonical.Marshal(product)
	case KindRecognition:
		var product model.RecognitionProduct
		_, _, err := s.pipeline.LoadProduct(ctx, request.ImageID, model.StageRecognize, &product)
		if err != nil {
			return nil, err
		}
		return canonical.Marshal(product)
	case KindImage:
		result := imageQueryResult{}
		targets := []struct {
			stage  string
			target any
		}{
			{model.StageDetect, new(model.DetectionProduct)},
			{model.StageRecognize, new(model.RecognitionProduct)},
			{model.StageStrategy, new(model.StrategyProduct)},
			{model.StageDiagnostic, new(model.ImageDiagnostic)},
		}
		raws := make([]json.RawMessage, 0, 4)
		for _, target := range targets {
			if _, _, err := s.pipeline.LoadProduct(ctx, request.ImageID, target.stage, target.target); err != nil {
				return nil, err
			}
			raw, err := canonical.Marshal(target.target)
			if err != nil {
				return nil, err
			}
			raws = append(raws, raw)
		}
		result.Detection, result.Recognition, result.Strategy, result.Diagnostic = raws[0], raws[1], raws[2], raws[3]
		return canonical.Marshal(result)
	case KindDiagnostics:
		if request.ImageID != "" {
			var product model.ImageDiagnostic
			_, _, err := s.pipeline.LoadProduct(ctx, request.ImageID, model.StageDiagnostic, &product)
			if err != nil {
				return nil, err
			}
			return canonical.Marshal(product)
		}
		groups := []string(nil)
		if request.Group != "" {
			groups = []string{request.Group}
		}
		_, reportBytes, err := s.pipeline.ReportForGroups(ctx, groups)
		return reportBytes, err
	default:
		return nil, apperr.New(apperr.CodeInvalidArgument, "unknown query kind: "+request.Kind)
	}
}

func annotateError(response Response, err error) (Response, error) {
	appErr := apperr.From(err)
	if appErr.QueryID == "" {
		appErr.QueryID = response.QueryID
	}
	if appErr.ElapsedMS == 0 {
		appErr.ElapsedMS = response.ElapsedMS
	}
	return response, appErr
}

func newQueryID() string {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "q-" + strings.Repeat("0", 24)
	}
	return "q-" + hex.EncodeToString(bytes)
}

func elapsedMS(start time.Time) float64 {
	value := float64(time.Since(start).Microseconds()) / 1000
	return math.Round(value*1000) / 1000
}
