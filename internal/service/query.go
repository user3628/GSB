package service

import (
	"context"

	"gpusched/internal/domain"
	"gpusched/internal/store"
)

// QuotaReport is the full read-model returned to operators.
type QuotaReport struct {
	Groups []domain.QuotaView `json:"groups"`
}

// QueryQuota builds a consistent read model. It takes no write lock and relies
// on a single read transaction so the whole view reflects one committed version.
func (a *App) QueryQuota(ctx context.Context) (*QuotaReport, error) {
	groups, err := a.store.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := a.store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	nodeByGroup := map[string][]domain.Node{}
	for _, n := range nodes {
		nodeByGroup[n.GroupID] = append(nodeByGroup[n.GroupID], n)
	}
	report := &QuotaReport{Groups: []domain.QuotaView{}}
	for _, g := range groups {
		view := domain.QuotaView{
			Nodes:   []domain.NodeQuota{},
			GroupID: g.ID, MemoryLimit: g.MemoryLimit, MemoryReserved: g.MemoryReserved,
			MemoryFree:    g.MemoryLimit - g.MemoryReserved,
			ComputeLimitM: g.ComputeLimitM, ComputeReservedM: g.ComputeReserved,
			ComputeFreeM: g.ComputeLimitM - g.ComputeReserved,
		}
		for _, n := range nodeByGroup[g.ID] {
			view.Nodes = append(view.Nodes, domain.NodeQuota{
				NodeID: n.ID, Health: n.Health, MemoryTotal: n.MemoryTotal,
				MemoryUsed: n.MemoryUsed, MemoryFree: n.MemoryTotal - n.MemoryUsed,
				ComputeTotalM: n.ComputeTotalM, ComputeUsedM: n.ComputeUsedM,
				ComputeFreeM: n.ComputeTotalM - n.ComputeUsedM,
			})
		}
		report.Groups = append(report.Groups, view)
	}
	return report, nil
}

// ListRequests returns all requests in stable submission order.
func (a *App) ListRequests(ctx context.Context) ([]domain.Request, error) {
	return a.store.ListRequests(ctx)
}

// ListBatches returns all batches with their member request ids.
func (a *App) ListBatches(ctx context.Context) ([]store.BatchWithMembers, error) {
	return a.store.ListBatches(ctx)
}
