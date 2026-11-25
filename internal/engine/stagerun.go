package engine

func (e *Engine) latestStageRun(stage string) (int, string, error) {
	return e.store.LatestStageRun(stage)
}
