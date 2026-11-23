package engine

import "sync"

type devCtx struct {
	claim        *DeviceClaim
	rep          *DeviceReport
	score        int64
	scoreVersion int64
	schedCode    Code
	authCode     Code
	cred         *Credential
}

// RunRound executes one full round: registration, trust scoring,
// risk-weighted scheduling, concurrent evaluation, credential issuance,
// continuous verification and (when the round is clean) a single-transaction
// snapshot commit. It returns the deterministic round report.
func (e *Engine) RunRound(claims []DeviceClaim, now, verifyAt int64) *Report {
	e.round++
	rep := &Report{
		Schema:   ReportSchema,
		TimeMark: TimeMark,
		Round:    e.round,
		Now:      now,
		Params:   e.params,
		Fault:    e.fault,
	}

	var devs []*devCtx
	var eligible []*devCtx
	seenDev := map[string]bool{}
	seenFP := map[string]string{}

	for i := range claims {
		c := &claims[i]
		dr := &DeviceReport{
			TenantID:   c.TenantID,
			DeviceID:   c.DeviceID,
			Code:       CodeOK,
			Register:   RegisterSummary{DeviceID: c.DeviceID, Fingerprint: c.Fingerprint, Caps: len(c.Capabilities), Events: len(c.Events)},
			ScoreChain: []ScoreStep{},
			Schedule:   SchedEntry{DeviceID: c.DeviceID, QueuePos: -1},
		}
		dc := &devCtx{claim: c, rep: dr, schedCode: CodeOK, authCode: CodeOK}
		devs = append(devs, dc)

		if verr := e.validateClaim(c, seenDev, seenFP); verr != nil {
			dr.Code = verr.Code
			dr.Auth.Decision = verr.Code
			dr.Schedule.Code = verr.Code
			continue
		}
		dr.Register.Registered = true

		score, steps, serr := Accumulate(c.Events)
		dr.ScoreChain = steps
		if serr != nil {
			dr.Code = serr.(*Error).Code
			dr.Auth.Decision = dr.Code
			dr.Schedule.Code = dr.Code
			continue
		}
		points, trunc := Truncate(score)
		dr.Truncation = trunc
		policy := e.policies[c.TenantID]
		dr.Auth.Threshold = policy.TrustThreshold
		dr.Auth.Score = score
		dr.Auth.ScorePoints = points

		dc.score = score
		dc.scoreVersion = 1
		if st, ok := e.states[deviceKey(c.TenantID, c.DeviceID)]; ok {
			dc.scoreVersion = st.ScoreVersion + 1
		}
		eligible = append(eligible, dc)
	}

	schedClaims := make([]*DeviceClaim, len(eligible))
	for i, dc := range eligible {
		schedClaims[i] = dc.claim
	}
	plan := PlanSchedule(schedClaims, e.params)
	for i, dc := range eligible {
		dc.rep.Schedule = plan.Entries[i]
		dc.schedCode = plan.Entries[i].Code
	}

	// Concurrent evaluation: worker pool bounded by runtime workers and the
	// in-flight semaphore. Results are written per index, so output does
	// not depend on goroutine interleaving.
	workers := e.params.Workers
	if workers <= 0 {
		workers = e.params.Concurrency
	}
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	jobs := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				dc := eligible[idx]
				if dc.schedCode != CodeOK {
					continue
				}
				e.sem <- struct{}{}
				sess := e.NewSession(dc.claim.TenantID)
				sess.Record(dc.claim.DeviceID, dc.score)
				dc.cred, dc.authCode = e.Authorize(dc.claim, dc.score, dc.scoreVersion, now)
				<-e.sem
			}
		}()
	}
	for idx := range eligible {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()

	for _, dc := range eligible {
		dr := dc.rep
		switch {
		case dc.schedCode != CodeOK:
			dr.Code = dc.schedCode
			dr.Auth.Decision = dc.schedCode
		case dc.authCode != CodeOK:
			dr.Code = dc.authCode
			dr.Auth.Decision = dc.authCode
		default:
			dr.Code = CodeOK
			dr.Auth.Decision = CodeOK
			policy := e.policies[dc.claim.TenantID]
			vcode := VerifyCredential(dc.cred, e.key, policy, dc.scoreVersion, verifyAt)
			dr.Cred = &CredStatus{
				Digest:       Digest(dc.cred),
				ScoreVersion: dc.cred.ScoreVersion,
				IssuedAt:     dc.cred.IssuedAt,
				ExpiresAt:    dc.cred.ExpiresAt,
				VerifyAt:     verifyAt,
				VerifyCode:   vcode,
			}
		}
	}

	allOK := true
	for _, dc := range devs {
		if dc.rep.Code != CodeOK {
			allOK = false
			break
		}
		if dc.rep.Cred != nil && dc.rep.Cred.VerifyCode != CodeOK {
			allOK = false
			break
		}
	}

	if allOK {
		newStates := make(map[string]*DeviceState, len(e.states)+len(eligible))
		for k, v := range e.states {
			cp := *v
			newStates[k] = &cp
		}
		for _, dc := range eligible {
			policy := e.policies[dc.claim.TenantID]
			newStates[deviceKey(dc.claim.TenantID, dc.claim.DeviceID)] = &DeviceState{
				TenantID:      dc.claim.TenantID,
				DeviceID:      dc.claim.DeviceID,
				Fingerprint:   dc.claim.Fingerprint,
				Capabilities:  append([]string(nil), dc.claim.Capabilities...),
				Score:         dc.score,
				ScoreVersion:  dc.scoreVersion,
				CredDigest:    Digest(dc.cred),
				PolicyVersion: policy.PolicyVersion,
			}
		}
		snap := &Snapshot{
			Version:  e.latestContinuous + 1,
			PrevHash: e.headHash,
			Devices:  SortedStates(newStates),
		}
		if err := e.store.CommitSnapshot(snap); err == nil {
			e.states = newStates
			e.headHash = snap.Hash()
			e.latestContinuous = snap.Version
			e.chain = append(e.chain, SnapshotRecord{
				Version:     snap.Version,
				PrevHash:    snap.PrevHash,
				ContentHash: snap.Hash(),
				Continuous:  true,
			})
		}
	}

	rep.SnapshotVersion = e.latestContinuous
	rep.Devices = make([]DeviceReport, 0, len(devs))
	for _, dc := range devs {
		rep.Devices = append(rep.Devices, *dc.rep)
	}
	rep.Scheduling = SchedulingReport{
		Concurrency:  e.params.Concurrency,
		MaxInFlight:  e.params.MaxInFlight,
		WaitQueueMax: e.params.WaitQueueMax,
		PeakInFlight: plan.PeakInFlight,
		Runnable:     plan.Runnable,
		Waiting:      plan.Waiting,
		Degraded:     plan.Degraded,
		CPUUsed:      plan.CPUUsed,
		CPUBudget:    e.params.GlobalCPU,
		MemUsed:      plan.MemUsed,
		MemBudget:    e.params.GlobalMem,
	}
	rep.Snapshots = append([]SnapshotRecord(nil), e.chain...)
	counts := map[Code]int{}
	for _, d := range rep.Devices {
		counts[d.Code]++
	}
	rep.Totals = Totals{Devices: len(rep.Devices), ByCode: make([]CodeCount, 0, len(counts))}
	for code, n := range counts {
		rep.Totals.ByCode = append(rep.Totals.ByCode, CodeCount{Code: code, Count: n})
	}
	return rep
}
