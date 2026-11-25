package pipeline

import (
	"context"

	"imretrace/internal/features"
	"imretrace/internal/model"
	"imretrace/internal/quant"
	"imretrace/internal/search"
)

func (s *Service) SearchText(ctx context.Context, query string, k int) (*search.Result, error) {
	buildReport, berr := s.Build(ctx, BuildOptions{})
	if berr != nil {
		if biz, ok := model.AsBiz(berr); !ok || biz.Code != model.CodeNumericMismatch {
			return nil, berr
		}
	}
	_ = buildReport
	idx, err := s.LoadIndex(ctx)
	if err != nil {
		return nil, err
	}
	normalized := features.NormalizeText(query)
	if normalized == "" {
		res, qerr := idx.SearchText(query, nil, nil, nil, k)
		return res, withPartial(qerr, res)
	}
	_, terms, vec, err := features.ExtractText("adhoc-query", query)
	if err != nil {
		return nil, err
	}
	matrix := idx.Matrix
	if matrix == nil {
		matrix = quant.DeriveMatrix(idx.Policy)
	}
	raw, err := quant.Multiply("adhoc-query", "text", matrix.Text, vec, idx.Policy.ProjectionDim)
	if err != nil {
		return nil, err
	}
	ref, _, err := quant.Normalize("adhoc-query#text", raw)
	if err != nil {
		return nil, err
	}
	qv, err := quant.QuantizeVector("adhoc-query", "text", ref, idx.TextParams)
	if err != nil {
		return nil, err
	}
	res, qerr := idx.SearchText(query, terms, ref, qv, k)
	return res, withPartial(qerr, res)
}

func (s *Service) SearchImage(ctx context.Context, itemID string, k int) (*search.Result, error) {
	if _, berr := s.Build(ctx, BuildOptions{}); berr != nil {
		if biz, ok := model.AsBiz(berr); !ok || biz.Code != model.CodeNumericMismatch {
			return nil, berr
		}
	}
	idx, err := s.LoadIndex(ctx)
	if err != nil {
		return nil, err
	}
	res, qerr := idx.SearchImage(itemID, k)
	return res, withPartial(qerr, res)
}

func withPartial(err error, res *search.Result) error {
	if err == nil {
		return nil
	}
	if biz, ok := model.AsBiz(err); ok && biz.Partial == nil && res != nil {
		biz.Partial = res
	}
	return err
}

func (s *Service) ApplyPatch(ctx context.Context, patch model.Patch) (model.Policy, bool, error) {
	current, err := s.Store.CurrentPolicy(ctx)
	if err != nil {
		return model.Policy{}, false, err
	}
	if current == nil {
		p := model.DefaultPolicy()
		current = &p
	}
	next, err := current.ApplyPatch(patch)
	if err != nil {
		return model.Policy{}, false, err
	}
	saved, changed, err := s.Store.ActivatePolicy(ctx, next)
	if err != nil {
		return model.Policy{}, false, err
	}
	_ = s.WritePolicySnapshot(saved)
	return saved, changed, nil
}

func (s *Service) ReplacePolicy(ctx context.Context, p model.Policy) (model.Policy, bool, error) {
	current, err := s.Store.CurrentPolicy(ctx)
	if err != nil {
		return model.Policy{}, false, err
	}
	if current == nil {
		def := model.DefaultPolicy()
		current = &def
	}
	saved, changed, err := s.Store.ActivatePolicy(ctx, p)
	if err != nil {
		return model.Policy{}, false, err
	}
	_ = s.WritePolicySnapshot(saved)
	return saved, changed, nil
}
