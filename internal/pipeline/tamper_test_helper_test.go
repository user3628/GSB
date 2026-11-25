package pipeline

import "matbench/internal/store"

func (p *Pipeline) tamperPayload(cp *store.Checkpoint, payload []byte) error {
	p.Store.Lock()
	defer p.Store.Unlock()
	return p.Store.ReplacePayloadForTest(cp.MaterialID, cp.Stage, payload)
}
