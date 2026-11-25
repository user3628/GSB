package engine

import (
	"searchlab/internal/index"
	"searchlab/internal/stage"
)

const (
	StageTokenize    = stage.Tokenize
	StageStats       = stage.Stats
	StagePostings    = stage.Postings
	StageDiagnostics = stage.Diagnostics
)

var StageOrder = stage.Order

type DocStat = index.DocStat
type QuantPosting = index.QuantPosting
type TermStat = index.TermStat
type StatsArtifact = index.StatsArtifact
