package ledger

import (
	"sort"
)

const (
	MinuteBoundaryRule = "minute = ceil(timestamp_ms / 60000); an exactly aligned timestamp stays in its minute; event effects begin at that minute; an interest interval [m,m+1) is charged when m < as_of_minute; a snapshot is effective for minute m when ceil(generated_at_ms/60000) <= m"
	RatioRule          = "fixed 6 decimal places, round-half-up from exact integer micro-share ratio"
	ToleranceRule      = "overcommit only when used_micros > floor(capacity_micros * threshold_ppm * (1000000+relative_tolerance_ppm) / 1000000000000); equality is inside tolerance"
	ReturnRule         = "repayment principal is applied to lots by ascending repayment_priority, then opened minute, then settle_seq; only principal left after all available lots is applied to unsettled interest"
)

type ProjectionMeta struct {
	LedgerVersion        int64
	LastSettledThroughMS int64
}

type ProjectionResult struct {
	State *LedgerState
	Steps []InterestStep
}

type lotRuntime struct {
	id             string
	openedMinute   int64
	original       int64
	principal      int64
	returned       int64
	interestMicros int64
	priority       int64
	carryNanos     int64
}

type tenantRuntime struct {
	tenantID        string
	poolID          string
	current         Tenant
	lots            []*lotRuntime
	principalMilli  int64
	interestMicros  int64
	borrowedMilli   int64
	returnedMilli   int64
	lastAccruedMin  int64
	lastTimestampMS int64
	rateSegments    []RateSegment
	returns         []ReturnAllocation
}

func BuildProjection(factsSnapshots []FactSnapshot, factsEvents []FactEvent, asOfMinute int64, meta ProjectionMeta, detail bool) (*ProjectionResult, error) {
	if asOfMinute < 0 {
		return nil, Biz(CodeLedgerInvalid, "as_of_minute must be non-negative", nil)
	}
	sched := NewSnapshotSchedule(factsSnapshots)
	events := make([]FactEvent, 0, len(factsEvents))
	seenSettle := make(map[string]FactEvent, len(factsEvents))
	for _, fe := range factsEvents {
		if err := ValidateEventShape(fe.Event); err != nil {
			return nil, err
		}
		if prior, ok := seenSettle[fe.Event.SettleSeq]; ok {
			if prior.ContentDigest != fe.ContentDigest {
				return nil, Biz(CodeConflictingSettle, "settle_seq has conflicting persisted content", map[string]any{"settle_seq": fe.Event.SettleSeq})
			}
			continue
		}
		seenSettle[fe.Event.SettleSeq] = fe
		if MinuteOf(fe.Event.TimestampMS) <= asOfMinute {
			events = append(events, fe)
		}
	}

	currentSnapshot, hasCurrent := sched.At(asOfMinute)
	lastSnapshotVersion := int64(0)
	for _, fs := range factsSnapshots {
		if fs.Snapshot.Version > lastSnapshotVersion {
			lastSnapshotVersion = fs.Snapshot.Version
		}
	}

	if !hasCurrent {
		if len(events) > 0 {
			return nil, Biz(CodeEventInvalid, "events reference tenants with no effective declaration snapshot", map[string]any{"as_of_minute": asOfMinute})
		}
		state := &LedgerState{
			LedgerVersion:        meta.LedgerVersion,
			AsOfMinute:           asOfMinute,
			AsOfMS:               MinuteStartMS(asOfMinute),
			LastSettledThroughMS: meta.LastSettledThroughMS,
			LastSnapshotVersion:  lastSnapshotVersion,
			SnapshotCount:        len(factsSnapshots),
			EventCount:           0,
			Tenants:              []TenantState{},
			Pools:                []PoolState{},
		}
		digest, err := StateDigest(state)
		if err != nil {
			return nil, err
		}
		state.StateDigest = digest
		return &ProjectionResult{State: state}, nil
	}

	currentPools := make(map[string]Pool, len(currentSnapshot.Snapshot.Pools))
	for _, p := range currentSnapshot.Snapshot.Pools {
		currentPools[p.ID] = p
	}
	currentTenants := make(map[string]Tenant, len(currentSnapshot.Snapshot.Tenants))
	tenantIDs := make([]string, 0, len(currentSnapshot.Snapshot.Tenants))
	for _, t := range currentSnapshot.Snapshot.Tenants {
		currentTenants[t.ID] = t
		tenantIDs = append(tenantIDs, t.ID)
	}
	sort.Strings(tenantIDs)

	byTenant := make(map[string][]FactEvent)
	for _, fe := range events {
		if _, ok := currentTenants[fe.Event.TenantID]; !ok {
			return nil, Biz(CodeEventInvalid, "event references an undeclared tenant", map[string]any{
				"settle_seq": fe.Event.SettleSeq,
				"tenant_id":  fe.Event.TenantID,
				"minute":     MinuteOf(fe.Event.TimestampMS),
			})
		}
		byTenant[fe.Event.TenantID] = append(byTenant[fe.Event.TenantID], fe)
	}

	runtimes := make(map[string]*tenantRuntime, len(tenantIDs))
	for _, id := range tenantIDs {
		t := currentTenants[id]
		p, ok := currentPools[t.PoolID]
		if !ok {
			return nil, Biz(CodeLedgerInvalid, "tenant references absent pool", map[string]any{"tenant_id": id, "pool_id": t.PoolID})
		}
		_ = p
		runtimes[id] = &tenantRuntime{
			tenantID:        id,
			poolID:          t.PoolID,
			current:         t,
			lastAccruedMin:  asOfMinute,
			lastTimestampMS: -1,
			rateSegments:    []RateSegment{},
			returns:         []ReturnAllocation{},
		}
	}

	allSteps := make([]InterestStep, 0)
	for _, tenantID := range tenantIDs {
		t := runtimes[tenantID]
		tenantEvents := byTenant[tenantID]
		sort.Slice(tenantEvents, func(i, j int) bool {
			if tenantEvents[i].Event.TimestampMS != tenantEvents[j].Event.TimestampMS {
				return tenantEvents[i].Event.TimestampMS < tenantEvents[j].Event.TimestampMS
			}
			return tenantEvents[i].Event.SettleSeq < tenantEvents[j].Event.SettleSeq
		})
		if len(tenantEvents) > 0 {
			t.lastAccruedMin = MinuteOf(tenantEvents[0].Event.TimestampMS)
		}
		for _, fe := range tenantEvents {
			ev := fe.Event
			evMinute := MinuteOf(ev.TimestampMS)
			if ev.TimestampMS < t.lastTimestampMS {
				return nil, Biz(CodeEventInvalid, "tenant event timestamp regressed", map[string]any{
					"tenant_id":             tenantID,
					"settle_seq":            ev.SettleSeq,
					"timestamp_ms":          ev.TimestampMS,
					"previous_timestamp_ms": t.lastTimestampMS,
				})
			}
			decl, ok := tenantDeclarationAt(sched, evMinute, tenantID)
			if !ok {
				return nil, Biz(CodeEventInvalid, "event references a tenant not declared at its effective minute", map[string]any{
					"tenant_id":  tenantID,
					"settle_seq": ev.SettleSeq,
					"minute":     evMinute,
				})
			}
			if err := accrueTenantTo(t, evMinute, sched, detail, &allSteps); err != nil {
				return nil, err
			}
			switch ev.Type {
			case "borrow":
				if err := applyBorrow(t, decl, ev, evMinute); err != nil {
					return nil, err
				}
			case "return":
				if err := applyReturn(t, ev, evMinute); err != nil {
					return nil, err
				}
			default:
				return nil, Biz(CodeEventInvalid, "unsupported event type", map[string]any{"type": ev.Type})
			}
			t.lastTimestampMS = ev.TimestampMS
		}
		if err := accrueTenantTo(t, asOfMinute, sched, detail, &allSteps); err != nil {
			return nil, err
		}
	}

	state := &LedgerState{
		LedgerVersion:        meta.LedgerVersion,
		AsOfMinute:           asOfMinute,
		AsOfMS:               MinuteStartMS(asOfMinute),
		LastSettledThroughMS: meta.LastSettledThroughMS,
		LastSnapshotVersion:  lastSnapshotVersion,
		SnapshotCount:        len(factsSnapshots),
		EventCount:           len(events),
		Tenants:              make([]TenantState, 0, len(tenantIDs)),
		Pools:                make([]PoolState, 0, len(currentPools)),
	}
	if d, err := SnapshotDigest(currentSnapshot.Snapshot); err == nil {
		state.SnapshotDigest = d
	}

	for _, tenantID := range tenantIDs {
		t := runtimes[tenantID]
		principalMicros, err := checkedSharesToMicros(t.principalMilli, "tenant principal micros")
		if err != nil {
			return nil, err
		}
		usedMicros, err := checkedAddInt64(principalMicros, t.interestMicros, "tenant used micros")
		if err != nil {
			return nil, err
		}
		creditMicros, err := checkedSharesToMicros(t.current.CreditLimitMilli, "tenant credit micros")
		if err != nil {
			return nil, err
		}
		available := creditMicros - usedMicros
		if available < 0 {
			available = 0
		}
		var rounding int64
		lots := make([]LotState, 0, len(t.lots))
		for _, lot := range sortedLots(t.lots) {
			rounding, err = checkedAddInt64(rounding, lot.carryNanos, "tenant rounding error")
			if err != nil {
				return nil, err
			}
			lots = append(lots, LotState{
				LotID:                     lot.id,
				OpenedMinute:              lot.openedMinute,
				OriginalPrincipalMilli:    lot.original,
				OutstandingPrincipalMilli: lot.principal,
				ReturnedPrincipalMilli:    lot.returned,
				InterestMicros:            lot.interestMicros,
				RepaymentPriority:         lot.priority,
				RoundingErrorNanos:        lot.carryNanos,
			})
		}
		segments := append([]RateSegment(nil), t.rateSegments...)
		returns := append([]ReturnAllocation(nil), t.returns...)
		state.Tenants = append(state.Tenants, TenantState{
			TenantID:                   tenantID,
			PoolID:                     t.poolID,
			DeclarationSnapshotVersion: currentSnapshot.Snapshot.Version,
			BaseSharesMilli:            t.current.BaseSharesMilli,
			CreditLimitMilli:           t.current.CreditLimitMilli,
			RateTier:                   t.current.RateTier,
			RepaymentPriority:          t.current.RepaymentPriority,
			PrincipalOutstandingMilli:  t.principalMilli,
			UnsettledInterestMicros:    t.interestMicros,
			AvailableCreditMicros:      available,
			BorrowedTotalMilli:         t.borrowedMilli,
			ReturnedTotalMilli:         t.returnedMilli,
			RoundingErrorNanos:         rounding,
			InterestRoundingMethod:     InterestScale,
			OutstandingLots:            lots,
			RateSegments:               segments,
			ReturnAllocations:          returns,
		})
	}
	pools, err := buildPoolStates(currentPools, state.Tenants, currentSnapshot.Snapshot)
	if err != nil {
		return nil, err
	}
	state.Pools = pools
	digest, err := StateDigest(state)
	if err != nil {
		return nil, err
	}
	state.StateDigest = digest
	return &ProjectionResult{State: state, Steps: allSteps}, nil
}

func tenantDeclarationAt(s *SnapshotSchedule, minute int64, tenantID string) (Tenant, bool) {
	fs, ok := s.At(minute)
	if !ok {
		return Tenant{}, false
	}
	for _, t := range fs.Snapshot.Tenants {
		if t.ID == tenantID {
			return t, true
		}
	}
	return Tenant{}, false
}

func rateAt(s *SnapshotSchedule, minute int64, tenantID string) (string, int64, error) {
	fs, ok := s.At(minute)
	if !ok {
		return "", 0, Biz(CodeLedgerInvalid, "no snapshot effective at interest minute", map[string]any{"minute": minute, "tenant_id": tenantID})
	}
	var decl *Tenant
	for i := range fs.Snapshot.Tenants {
		if fs.Snapshot.Tenants[i].ID == tenantID {
			decl = &fs.Snapshot.Tenants[i]
			break
		}
	}
	if decl == nil {
		return "", 0, Biz(CodeLedgerInvalid, "tenant is absent during interest accrual", map[string]any{"minute": minute, "tenant_id": tenantID})
	}
	rt, ok := fs.Snapshot.RateTiers[decl.RateTier]
	if !ok {
		return "", 0, Biz(CodeLedgerInvalid, "tenant rate tier is absent during interest accrual", map[string]any{"minute": minute, "tenant_id": tenantID, "rate_tier": decl.RateTier})
	}
	return decl.RateTier, rt.RatePPM, nil
}

func accrueTenantTo(t *tenantRuntime, targetMinute int64, sched *SnapshotSchedule, detail bool, allSteps *[]InterestStep) error {
	if targetMinute < t.lastAccruedMin {
		return internalErrorf("interest accrual target regressed for tenant %s", t.tenantID)
	}
	for minute := t.lastAccruedMin; minute < targetMinute; minute++ {
		active := make([]*lotRuntime, 0, len(t.lots))
		var principalSum int64
		for _, lot := range t.lots {
			if lot.principal > 0 {
				active = append(active, lot)
				v, err := checkedAddInt64(principalSum, lot.principal, "principal sum")
				if err != nil {
					return err
				}
				principalSum = v
			}
		}
		if len(active) == 0 {
			continue
		}
		rateTier, ratePPM, err := rateAt(sched, minute, t.tenantID)
		if err != nil {
			return err
		}
		sortLots(active)
		segIdx := -1
		if len(t.rateSegments) > 0 {
			last := &t.rateSegments[len(t.rateSegments)-1]
			if last.RateTier == rateTier && last.RatePPM == ratePPM && last.EndMinuteExclusive == minute {
				segIdx = len(t.rateSegments) - 1
			}
		}
		if segIdx < 0 {
			t.rateSegments = append(t.rateSegments, RateSegment{
				RateTier:           rateTier,
				RatePPM:            ratePPM,
				StartMinute:        minute,
				EndMinuteExclusive: minute + 1,
			})
			segIdx = len(t.rateSegments) - 1
		}
		seg := &t.rateSegments[segIdx]
		seg.Minutes++
		nextPMM, err := checkedAddInt64(seg.PrincipalMilliMinutes, principalSum, "segment principal-minutes")
		if err != nil {
			return err
		}
		seg.PrincipalMilliMinutes = nextPMM
		for _, lot := range active {
			exact, err := checkedMulInt64(lot.principal, ratePPM, "minute interest numerator")
			if err != nil {
				return err
			}
			numerator, err := checkedAddInt64(lot.carryNanos, exact, "interest carry numerator")
			if err != nil {
				return err
			}
			delta := numerator / NanoPerMicro
			carry := numerator % NanoPerMicro
			lot.carryNanos = carry
			lot.interestMicros, err = checkedAddInt64(lot.interestMicros, delta, "lot interest")
			if err != nil {
				return err
			}
			t.interestMicros, err = checkedAddInt64(t.interestMicros, delta, "tenant interest")
			if err != nil {
				return err
			}
			seg.ExactInterestNanos, err = checkedAddInt64(seg.ExactInterestNanos, exact, "segment exact interest")
			if err != nil {
				return err
			}
			seg.InterestMicros, err = checkedAddInt64(seg.InterestMicros, delta, "segment interest")
			if err != nil {
				return err
			}
			if detail {
				*allSteps = append(*allSteps, InterestStep{
					TenantID:       t.tenantID,
					LotID:          lot.id,
					Minute:         minute,
					RateTier:       rateTier,
					RatePPM:        ratePPM,
					PrincipalMilli: lot.principal,
					ExactNanos:     exact,
					DeltaMicros:    delta,
					CarryNanos:     carry,
					Rounding:       "floor(exact_nanos + prior_carry, /1000); residual carried to the next minute for this lot",
				})
			}
		}
		seg.EndMinuteExclusive = minute + 1
		seg.RoundingErrorNanos = seg.ExactInterestNanos - seg.InterestMicros*NanoPerMicro
	}
	t.lastAccruedMin = targetMinute
	return nil
}

func applyBorrow(t *tenantRuntime, decl Tenant, ev Event, evMinute int64) error {
	usedMicros, err := checkedSharesToMicros(t.principalMilli, "borrow principal")
	if err != nil {
		return err
	}
	usedMicros, err = checkedAddInt64(usedMicros, t.interestMicros, "borrow used")
	if err != nil {
		return err
	}
	creditMicros, err := checkedSharesToMicros(decl.CreditLimitMilli, "borrow credit")
	if err != nil {
		return err
	}
	requestMicros, err := checkedSharesToMicros(ev.SharesMilli, "borrow request")
	if err != nil {
		return err
	}
	remaining := creditMicros - usedMicros
	if remaining < 0 {
		remaining = 0
	}
	if requestMicros > remaining {
		return Biz(CodeCreditExceeded, "borrow exceeds remaining tenant credit", map[string]any{
			"tenant_id":               decl.ID,
			"settle_seq":              ev.SettleSeq,
			"requested_micros":        requestMicros,
			"remaining_credit_micros": remaining,
			"remaining_credit_milli":  remaining / MicroPerMilli,
			"at_minute":               evMinute,
		})
	}
	t.lots = append(t.lots, &lotRuntime{
		id:           ev.SettleSeq,
		openedMinute: evMinute,
		original:     ev.SharesMilli,
		principal:    ev.SharesMilli,
		priority:     decl.RepaymentPriority,
	})
	t.principalMilli, err = checkedAddInt64(t.principalMilli, ev.SharesMilli, "tenant principal")
	if err != nil {
		return err
	}
	t.borrowedMilli, err = checkedAddInt64(t.borrowedMilli, ev.SharesMilli, "tenant borrowed total")
	if err != nil {
		return err
	}
	return nil
}

func applyReturn(t *tenantRuntime, ev Event, evMinute int64) error {
	principalMicros, err := checkedSharesToMicros(t.principalMilli, "return principal")
	if err != nil {
		return err
	}
	debtMicros, err := checkedAddInt64(principalMicros, t.interestMicros, "return debt")
	if err != nil {
		return err
	}
	amountMicros, err := checkedSharesToMicros(ev.SharesMilli, "return amount")
	if err != nil {
		return err
	}
	if amountMicros > debtMicros {
		return Biz(CodeEventInvalid, "return exceeds outstanding principal plus unsettled interest", map[string]any{
			"tenant_id":          ev.TenantID,
			"settle_seq":         ev.SettleSeq,
			"requested_micros":   amountMicros,
			"outstanding_micros": debtMicros,
			"at_minute":          evMinute,
		})
	}
	alloc := ReturnAllocation{
		EventSettleSeq:       ev.SettleSeq,
		AtMinute:             evMinute,
		RequestedAmountMilli: ev.SharesMilli,
		Payments:             []LotPayment{},
	}
	remainingPrincipalMilli := ev.SharesMilli
	for _, lot := range sortedLots(t.lots) {
		if remainingPrincipalMilli == 0 {
			break
		}
		if lot.principal == 0 {
			continue
		}
		pay := remainingPrincipalMilli
		if pay > lot.principal {
			pay = lot.principal
		}
		if pay == 0 {
			continue
		}
		lot.principal -= pay
		lot.returned += pay
		t.principalMilli -= pay
		t.returnedMilli += pay
		remainingPrincipalMilli -= pay
		alloc.PrincipalPaidMilli += pay
		alloc.Payments = append(alloc.Payments, LotPayment{LotID: lot.id, PrincipalPaidMilli: pay})
	}
	remainingMicros := remainingPrincipalMilli * MicroPerMilli
	if remainingMicros > 0 {
		for _, lot := range sortedLots(t.lots) {
			if remainingMicros == 0 {
				break
			}
			if lot.interestMicros == 0 {
				continue
			}
			pay := remainingMicros
			if pay > lot.interestMicros {
				pay = lot.interestMicros
			}
			lot.interestMicros -= pay
			t.interestMicros -= pay
			remainingMicros -= pay
			alloc.InterestPaidMicros += pay
			found := false
			for i := range alloc.Payments {
				if alloc.Payments[i].LotID == lot.id {
					alloc.Payments[i].InterestPaidMicros += pay
					found = true
					break
				}
			}
			if !found {
				alloc.Payments = append(alloc.Payments, LotPayment{LotID: lot.id, InterestPaidMicros: pay})
			}
		}
	}
	if remainingPrincipalMilli > 0 {
		// Every remaining unit must have been interest; prior debt check guarantees it was paid.
		if remainingMicros != 0 {
			return internalErrorf("unable to allocate full return for %s", ev.SettleSeq)
		}
	}
	t.returns = append(t.returns, alloc)
	return nil
}

func sortedLots(lots []*lotRuntime) []*lotRuntime {
	out := append([]*lotRuntime(nil), lots...)
	sortLots(out)
	return out
}

func sortLots(lots []*lotRuntime) {
	sort.Slice(lots, func(i, j int) bool {
		if lots[i].priority != lots[j].priority {
			return lots[i].priority < lots[j].priority
		}
		if lots[i].openedMinute != lots[j].openedMinute {
			return lots[i].openedMinute < lots[j].openedMinute
		}
		return lots[i].id < lots[j].id
	})
}

func buildPoolStates(pools map[string]Pool, tenants []TenantState, snap Snapshot) ([]PoolState, error) {
	poolIDs := make([]string, 0, len(pools))
	for id := range pools {
		poolIDs = append(poolIDs, id)
	}
	sort.Strings(poolIDs)
	byPool := make(map[string][]TenantState)
	for _, t := range tenants {
		byPool[t.PoolID] = append(byPool[t.PoolID], t)
	}
	out := make([]PoolState, 0, len(poolIDs))
	for _, poolID := range poolIDs {
		p := pools[poolID]
		ps := PoolState{
			PoolID:                 poolID,
			TotalSharesMilli:       p.TotalSharesMilli,
			ReserveSharesMilli:     p.ReserveSharesMilli,
			UsableCapacityMilli:    p.TotalSharesMilli - p.ReserveSharesMilli,
			OvercommitThresholdPPM: p.OvercommitThresholdPPM,
			RelativeTolerancePPM:   p.RelativeTolerancePPM,
			RatioRounding:          RatioRule,
			ToleranceRule:          ToleranceRule,
		}
		capacityMicros, err := checkedSharesToMicros(ps.UsableCapacityMilli, "pool capacity micros")
		if err != nil {
			return nil, err
		}
		ps.ThresholdLimitMicros, err = thresholdLimitMicros(capacityMicros, p.OvercommitThresholdPPM)
		if err != nil {
			return nil, err
		}
		ps.ToleranceLimitMicros, err = toleranceLimitMicros(capacityMicros, p.OvercommitThresholdPPM, p.RelativeTolerancePPM)
		if err != nil {
			return nil, err
		}
		var principalMilli, interestMicros, usedMicros, crossExposure, availableCredit int64
		var triggerExposure int64 = -1
		var triggerTenant string
		var neumaier neumaierAccumulator
		for _, t := range byPool[poolID] {
			ps.TenantCount++
			principalMilli, err = checkedAddInt64(principalMilli, t.PrincipalOutstandingMilli, "pool principal")
			if err != nil {
				return nil, err
			}
			interestMicros, err = checkedAddInt64(interestMicros, t.UnsettledInterestMicros, "pool interest")
			if err != nil {
				return nil, err
			}
			principalMicros, err := checkedSharesToMicros(t.PrincipalOutstandingMilli, "pool tenant principal micros")
			if err != nil {
				return nil, err
			}
			exposure, err := checkedAddInt64(principalMicros, t.UnsettledInterestMicros, "pool tenant exposure")
			if err != nil {
				return nil, err
			}
			usedMicros, err = checkedAddInt64(usedMicros, exposure, "pool used")
			if err != nil {
				return nil, err
			}
			baseMicros, err := checkedSharesToMicros(t.BaseSharesMilli, "pool tenant base")
			if err != nil {
				return nil, err
			}
			cross := exposure - baseMicros
			if cross > 0 {
				crossExposure, err = checkedAddInt64(crossExposure, cross, "cross tenant exposure")
				if err != nil {
					return nil, err
				}
			}
			credit, err := checkedSharesToMicros(t.CreditLimitMilli, "pool tenant credit")
			if err != nil {
				return nil, err
			}
			avail := credit - exposure
			if avail < 0 {
				avail = 0
			}
			availableCredit, err = checkedAddInt64(availableCredit, avail, "available credit")
			if err != nil {
				return nil, err
			}
			neumaier.Add(float64(exposure))
			if exposure > triggerExposure {
				triggerExposure = exposure
				triggerTenant = t.TenantID
			} else if exposure == triggerExposure && t.TenantID < triggerTenant {
				triggerTenant = t.TenantID
			}
		}
		ps.PrincipalOutstandingMilli = principalMilli
		ps.UnsettledInterestMicros = interestMicros
		ps.UsedMicros = usedMicros
		ps.CrossTenantExposureMicros = crossExposure
		ps.AvailableCreditMicros = availableCredit
		ps.UsedRatioFixed, err = fixedRatio(usedMicros, capacityMicros, 6)
		if err != nil {
			return nil, err
		}
		ps.UsedRatioPPM, err = fixedRatioPPM(usedMicros, capacityMicros)
		if err != nil {
			return nil, err
		}
		ps.OvercommitRisk = usedMicros > ps.ToleranceLimitMicros
		if ps.OvercommitRisk {
			ps.ExcessOverThresholdMicros = maxInt64(0, usedMicros-ps.ThresholdLimitMicros)
			ps.TriggerTenantID = triggerTenant
		}
		ps.CompensatedSum = neumaier.Result()
		out = append(out, ps)
	}
	return out, nil
}

func BuildSettlementReport(req SettlementRequest, state *LedgerState, result *ProjectionResult) *SettlementReport {
	stepsByTenant := make(map[string][]InterestStep)
	for _, step := range result.Steps {
		stepsByTenant[step.TenantID] = append(stepsByTenant[step.TenantID], step)
	}
	details := make([]TenantSettlementDetail, 0, len(state.Tenants))
	for _, t := range state.Tenants {
		details = append(details, TenantSettlementDetail{
			TenantID:                  t.TenantID,
			PrincipalOutstandingMilli: t.PrincipalOutstandingMilli,
			UnsettledInterestMicros:   t.UnsettledInterestMicros,
			AvailableCreditMicros:     t.AvailableCreditMicros,
			CumulativeRoundingNanos:   t.RoundingErrorNanos,
			RateSegments:              t.RateSegments,
			InterestSteps:             stepsByTenant[t.TenantID],
		})
	}
	report := &SettlementReport{
		RequestID:          req.RequestID,
		ThroughMS:          req.ThroughMS,
		ThroughMinute:      MinuteOf(req.ThroughMS),
		LedgerVersion:      state.LedgerVersion,
		StateDigest:        state.StateDigest,
		MinuteBoundaryRule: MinuteBoundaryRule,
		InterestScale:      InterestScale,
		Segments:           details,
		Pools:              state.Pools,
	}
	calculation := struct {
		ThroughMinute      int64                    `json:"through_minute"`
		MinuteBoundaryRule string                   `json:"minute_boundary_rule"`
		InterestScale      string                   `json:"interest_scale"`
		Segments           []TenantSettlementDetail `json:"tenant_segments"`
		Pools              []PoolState              `json:"pools"`
	}{report.ThroughMinute, report.MinuteBoundaryRule, report.InterestScale, report.Segments, report.Pools}
	if digest, err := CanonicalJSONDigest(calculation); err == nil {
		report.CalculationDigest = digest
	}
	return report
}

func FindPool(state *LedgerState, poolID string) (*PoolState, bool) {
	for i := range state.Pools {
		if state.Pools[i].PoolID == poolID {
			return &state.Pools[i], true
		}
	}
	return nil, false
}

func FindTenant(state *LedgerState, tenantID string) (*TenantState, bool) {
	for i := range state.Tenants {
		if state.Tenants[i].TenantID == tenantID {
			return &state.Tenants[i], true
		}
	}
	return nil, false
}
