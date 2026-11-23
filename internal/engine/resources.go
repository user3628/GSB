package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"secverify/internal/model"
)

type ConcurrencyStats struct {
	TransactionLimit        int   `json:"transaction_limit"`
	TransactionsInFlight    int64 `json:"transactions_in_flight"`
	TransactionsQueued      int64 `json:"transactions_queued"`
	PeakTransactions        int64 `json:"peak_transactions"`
	PeakTransactionQueue    int64 `json:"peak_transaction_queue"`
	ProbeLimit              int   `json:"probe_limit"`
	ProbesInFlight          int64 `json:"probes_in_flight"`
	ProbesQueued            int64 `json:"probes_queued"`
	PeakProbes              int64 `json:"peak_probes"`
	PeakProbeQueue          int64 `json:"peak_probe_queue"`
	NamespacesQueued        int64 `json:"namespaces_queued"`
	PeakNamespaceQueue      int64 `json:"peak_namespace_queue"`
	ContentionTimeouts      int64 `json:"contention_timeouts"`
	WaitTimeoutMilliseconds int64 `json:"wait_timeout_ms"`
}

type resourceManager struct {
	txLimit    int
	probeLimit int
	txSem      chan struct{}
	probeSem   chan struct{}

	txInFlight     atomic.Int64
	txQueued       atomic.Int64
	txPeak         atomic.Int64
	txQueuePeak    atomic.Int64
	probeInFlight  atomic.Int64
	probeQueued    atomic.Int64
	probePeak      atomic.Int64
	probeQueuePeak atomic.Int64
	nsQueued       atomic.Int64
	nsQueuePeak    atomic.Int64
	timeouts       atomic.Int64

	mu        sync.Mutex
	nsTokens  map[string]*namespaceToken
	waitLimit time.Duration
}

type namespaceToken struct {
	token chan struct{}
	refs  int
}

func newResourceManager(txLimit, probeLimit int, waitLimit time.Duration) *resourceManager {
	if txLimit <= 0 {
		txLimit = 2
	}
	if probeLimit <= 0 {
		probeLimit = 2
	}
	if waitLimit <= 0 {
		waitLimit = 2 * time.Second
	}
	return &resourceManager{
		txLimit:    txLimit,
		probeLimit: probeLimit,
		txSem:      make(chan struct{}, txLimit),
		probeSem:   make(chan struct{}, probeLimit),
		nsTokens:   make(map[string]*namespaceToken),
		waitLimit:  waitLimit,
	}
}

func (rm *resourceManager) acquireNamespace(ctx context.Context, namespace string, wait time.Duration) (func(), int64, *model.BizError) {
	if wait <= 0 {
		wait = rm.waitLimit
	}
	rm.mu.Lock()
	entry, ok := rm.nsTokens[namespace]
	if !ok {
		entry = &namespaceToken{token: make(chan struct{}, 1)}
		rm.nsTokens[namespace] = entry
	}
	entry.refs++
	rm.mu.Unlock()

	select {
	case entry.token <- struct{}{}:
		return func() {
			<-entry.token
			rm.mu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(rm.nsTokens, namespace)
			}
			rm.mu.Unlock()
		}, 0, nil
	default:
	}
	rm.nsQueued.Add(1)
	updatePeak(&rm.nsQueuePeak, rm.nsQueued.Load())
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case entry.token <- struct{}{}:
		rm.nsQueued.Add(-1)
		return func() {
			<-entry.token
			rm.mu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(rm.nsTokens, namespace)
			}
			rm.mu.Unlock()
		}, 1, nil
	case <-timer.C:
		rm.nsQueued.Add(-1)
		rm.timeouts.Add(1)
		rm.releaseNamespaceRef(namespace, entry)
		return nil, 1, model.NewError(model.CodeChangeContention, "", -1, "namespace wait threshold exceeded")
	case <-ctx.Done():
		rm.nsQueued.Add(-1)
		rm.timeouts.Add(1)
		rm.releaseNamespaceRef(namespace, entry)
		return nil, 1, model.NewError(model.CodeChangeContention, "", -1, "namespace wait cancelled")
	}
}

func (rm *resourceManager) releaseNamespaceRef(namespace string, entry *namespaceToken) {
	rm.mu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(rm.nsTokens, namespace)
	}
	rm.mu.Unlock()
}

func (rm *resourceManager) acquireTransaction(ctx context.Context, wait time.Duration) (func(), *model.BizError) {
	return acquireSlot(ctx, rm.txSem, rm.waitLimitOr(wait), &rm.txInFlight, &rm.txQueued, &rm.txPeak, &rm.txQueuePeak, &rm.timeouts)
}

func (rm *resourceManager) acquireProbe(ctx context.Context, wait time.Duration) (func(), *model.BizError) {
	return acquireSlot(ctx, rm.probeSem, rm.waitLimitOr(wait), &rm.probeInFlight, &rm.probeQueued, &rm.probePeak, &rm.probeQueuePeak, &rm.timeouts)
}

func (rm *resourceManager) waitLimitOr(wait time.Duration) time.Duration {
	if wait > 0 {
		return wait
	}
	return rm.waitLimit
}

func acquireSlot(ctx context.Context, sem chan struct{}, wait time.Duration, inFlight, queued, peak, queuePeak, timeouts *atomic.Int64) (func(), *model.BizError) {
	select {
	case sem <- struct{}{}:
		current := inFlight.Add(1)
		updatePeak(peak, current)
		return func() {
			inFlight.Add(-1)
			<-sem
		}, nil
	default:
	}
	queued.Add(1)
	updatePeak(queuePeak, queued.Load())
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case sem <- struct{}{}:
		queued.Add(-1)
		current := inFlight.Add(1)
		updatePeak(peak, current)
		return func() {
			inFlight.Add(-1)
			<-sem
		}, nil
	case <-timer.C:
		queued.Add(-1)
		timeouts.Add(1)
		return nil, model.NewError(model.CodeChangeContention, "", -1, "resource wait threshold exceeded")
	case <-ctx.Done():
		queued.Add(-1)
		timeouts.Add(1)
		return nil, model.NewError(model.CodeChangeContention, "", -1, "resource wait cancelled")
	}
}

func updatePeak(peak *atomic.Int64, value int64) {
	for {
		old := peak.Load()
		if value <= old || peak.CompareAndSwap(old, value) {
			return
		}
	}
}

func (rm *resourceManager) stats(wait time.Duration) ConcurrencyStats {
	return ConcurrencyStats{
		TransactionLimit:        rm.txLimit,
		TransactionsInFlight:    rm.txInFlight.Load(),
		TransactionsQueued:      rm.txQueued.Load(),
		PeakTransactions:        rm.txPeak.Load(),
		PeakTransactionQueue:    rm.txQueuePeak.Load(),
		ProbeLimit:              rm.probeLimit,
		ProbesInFlight:          rm.probeInFlight.Load(),
		ProbesQueued:            rm.probeQueued.Load(),
		PeakProbes:              rm.probePeak.Load(),
		PeakProbeQueue:          rm.probeQueuePeak.Load(),
		NamespacesQueued:        rm.nsQueued.Load(),
		PeakNamespaceQueue:      rm.nsQueuePeak.Load(),
		ContentionTimeouts:      rm.timeouts.Load(),
		WaitTimeoutMilliseconds: rm.waitLimitOr(wait).Milliseconds(),
	}
}
