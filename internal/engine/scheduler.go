package engine

import "sort"

// SchedEntry is the deterministic scheduling record of one device.
type SchedEntry struct {
	DeviceID string `json:"device_id"`
	Risk     int64  `json:"risk"`
	QueuePos int    `json:"queue_pos"`
	Waited   bool   `json:"waited"`
	Code     Code   `json:"code"`
	CPUUsed  int64  `json:"cpu_used"`
	MemUsed  int64  `json:"mem_used"`
}

// SchedPlan is the deterministic result of risk-weighted scheduling.
type SchedPlan struct {
	Entries      []SchedEntry
	CPUUsed      int64
	MemUsed      int64
	Runnable     int
	Waiting      int
	Degraded     int
	PeakInFlight int
}

// PlanSchedule orders devices by risk (descending, device id ascending as
// tie-break) and assigns budget in that order. Tasks beyond the concurrency
// limit are marked as waiting (queued, not rejected). A device whose
// scoring time exceeds the per-device budget is marked E_DEVICE_BUDGET.
// Once the global CPU or memory budget cannot cover a device, that device
// and every remaining (lower-risk) device is degraded with
// E_GLOBAL_BUDGET_EXHAUSTED; degraded devices stay in the plan so they
// appear in report counts.
func PlanSchedule(claims []*DeviceClaim, p Params) *SchedPlan {
	sorted := make([]*DeviceClaim, len(claims))
	copy(sorted, claims)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Risk != sorted[j].Risk {
			return sorted[i].Risk > sorted[j].Risk
		}
		return sorted[i].DeviceID < sorted[j].DeviceID
	})
	plan := &SchedPlan{Entries: make([]SchedEntry, 0, len(sorted))}
	var cpu, mem int64
	exhausted := false
	for i, c := range sorted {
		entry := SchedEntry{
			DeviceID: c.DeviceID,
			Risk:     c.Risk,
			QueuePos: i,
			Waited:   i >= p.Concurrency,
			Code:     CodeOK,
		}
		if entry.Waited {
			plan.Waiting++
		}
		switch {
		case exhausted:
			entry.Code = CodeGlobalBudget
		case c.NeedTime > p.PerDeviceTime:
			entry.Code = CodeDeviceBudget
		case cpu+c.NeedCPU > p.GlobalCPU || mem+c.NeedMem > p.GlobalMem:
			exhausted = true
			entry.Code = CodeGlobalBudget
		default:
			cpu += c.NeedCPU
			mem += c.NeedMem
			entry.CPUUsed = c.NeedCPU
			entry.MemUsed = c.NeedMem
			plan.Runnable++
		}
		if entry.Code == CodeDeviceBudget || entry.Code == CodeGlobalBudget {
			plan.Degraded++
		}
		plan.Entries = append(plan.Entries, entry)
	}
	plan.CPUUsed = cpu
	plan.MemUsed = mem
	plan.PeakInFlight = min(p.MaxInFlight, plan.Runnable)
	return plan
}
