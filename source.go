package dnsdetect

import (
	"context"
	"fmt"
	"os"
)

// LoadSource parses one observation file in an isolated session. A non-nil
// session is returned with errors when enough structure exists to report
// partial progress; callers must not merge a session whose err is non-nil.
func LoadSource(ctx context.Context, spec SourceSpec, options LoadOptions) (*SourceSession, error) {
	options = options.WithDefaults()
	if spec.Path == "" {
		return nil, newBusinessError(CodeIO, spec.Path, -1, "source path is empty", nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, newBusinessError(CodeIO, spec.Path, -1, "source load canceled", err)
	}
	data, err := os.ReadFile(spec.Path)
	if err != nil {
		return nil, newBusinessError(CodeIO, spec.Path, -1, "cannot read source", err)
	}
	kind := spec.EffectiveKind()
	if kind == SourceAuto {
		kind = sniffKind(data)
	}
	priority := defaultSourcePriority(kind)
	if options.SourcePriorities != nil {
		if configured, ok := options.SourcePriorities[kind]; ok {
			priority = configured
		}
	}
	if spec.Priority != nil {
		priority = *spec.Priority
	}
	builder := newSessionBuilder(spec.Path, kind, priority, spec.ObservationStart, options)
	builder.session.Bytes = int64(len(data))
	switch kind {
	case SourceBinary:
		err = parseBinary(ctx, data, builder)
	case SourceJSONL:
		err = parseJSONLines(ctx, data, builder)
	case SourceText:
		err = parseTextBlocks(ctx, data, builder)
	default:
		err = newBusinessError(CodeSourceKind, spec.Path, -1, "unsupported source kind", nil)
	}
	if err != nil {
		if business, ok := err.(*BusinessError); ok && business.Source == "" {
			business.Source = spec.Path
		}
		return builder.session, err
	}
	return builder.session, nil
}

func sniffKind(data []byte) SourceKind {
	if len(data) >= 4 && string(data[:4]) == binaryMagic {
		return SourceBinary
	}
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return SourceJSONL
		default:
			return SourceText
		}
	}
	return SourceText
}

func defaultSourcePriority(kind SourceKind) int {
	switch kind {
	case SourceBinary:
		return 30
	case SourceJSONL:
		return 20
	case SourceText:
		return 10
	default:
		return 0
	}
}

type sessionBuilder struct {
	session *SourceSession
	options LoadOptions
	haveMin bool
	minTime int64
	maxTime int64
}

func newSessionBuilder(path string, kind SourceKind, priority int, observationStart int64, options LoadOptions) *sessionBuilder {
	return &sessionBuilder{
		session: &SourceSession{
			Path:                  path,
			Kind:                  kind,
			Priority:              priority,
			ObservationStartTicks: observationStart,
		},
		options: options,
	}
}

func (b *sessionBuilder) setFormatVersion(version int) {
	b.session.FormatVersion = version
}

func (b *sessionBuilder) addEvent(ctx context.Context, event Event, conversion string) error {
	if err := ctx.Err(); err != nil {
		return newBusinessError(CodeIO, b.session.Path, event.Offset, "source parse canceled", err)
	}
	if len(b.session.Events) >= b.options.MaxEvents {
		return newBusinessError(CodeEventBudget, b.session.Path, event.Offset, fmt.Sprintf("session exceeds %d events", b.options.MaxEvents), nil)
	}
	if b.haveMin {
		nextMin, nextMax := b.minTime, b.maxTime
		if event.TimeTicks < nextMin {
			nextMin = event.TimeTicks
		}
		if event.TimeTicks > nextMax {
			nextMax = event.TimeTicks
		}
		if tickSpanExceeds(nextMin, nextMax, b.options.MaxSpanTicks) {
			return newBusinessError(CodeWindowBudget, b.session.Path, event.Offset,
				fmt.Sprintf("session time span exceeds %d ticks", b.options.MaxSpanTicks), nil)
		}
	} else {
		b.haveMin = true
		b.minTime = event.TimeTicks
		b.maxTime = event.TimeTicks
	}
	windowSpan := b.options.WindowSpanTicks
	if windowSpan <= 0 {
		windowSpan = DefaultWindowSpanTicks
	}
	windowIndex := floorDiv(event.TimeTicks-b.options.WindowAnchorTicks, windowSpan)
	event.SourcePath = b.session.Path
	event.SourceKind = b.session.Kind
	event.SourcePriority = b.session.Priority
	event.FormatVersion = b.session.FormatVersion
	event.TimeErrorTicks = TimeErrorTicks(event.TimeErrorNS)
	b.session.Events = append(b.session.Events, event)
	b.session.Ledger.Add(event.SourcePath, event.SourceKind, event.Offset, conversion, event.TimeTicks, event.TimeErrorNS, windowIndex)
	return nil
}

func wrapOffsetError(err error, source string, offset int64) error {
	if err == nil {
		return nil
	}
	if business, ok := err.(*BusinessError); ok {
		if business.Source == "" {
			business.Source = source
		}
		if business.Offset < 0 {
			business.Offset = offset
		}
		return business
	}
	return newBusinessError(CodeIO, source, offset, "source parsing failed", err)
}
