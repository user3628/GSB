package privilege

import "svcworkbench/internal/domain"

func Build(account domain.AccountKind, start domain.StartType, capabilities []domain.LogonCapability) domain.PrivilegeProfile {
	accountLevel := domain.AccountLevel(account)
	startupLevel := startupLevel(start)
	logonLevel := logonLevel(capabilities)
	active := start != domain.StartDisabled && !deniedOnly(capabilities)
	score := accountLevel*100 + startupLevel*10 + logonLevel
	profile := domain.PrivilegeProfile{
		AccountKind:       account,
		AccountLevel:      accountLevel,
		StartType:         start,
		StartupLevel:      startupLevel,
		LogonCapabilities: domain.SortLogonCapabilities(capabilities),
		LogonLevel:        logonLevel,
		EffectiveScore:    score,
		EffectiveTier:     tier(score, active),
		HighPrivilege:     accountLevel >= domain.AccountLevel(domain.AccountNetworkService) && active,
		Active:            active,
	}
	return profile
}

func startupLevel(start domain.StartType) int {
	switch start {
	case domain.StartBoot, domain.StartSystem:
		return 3
	case domain.StartAutomatic:
		return 2
	case domain.StartManual:
		return 1
	default:
		return 0
	}
}

func logonLevel(capabilities []domain.LogonCapability) int {
	best := 0
	for _, capability := range capabilities {
		level := 0
		switch capability {
		case domain.LogonInteractive:
			level = 4
		case domain.LogonNetwork:
			level = 3
		case domain.LogonService:
			level = 2
		case domain.LogonBatch:
			level = 1
		}
		if level > best {
			best = level
		}
	}
	return best
}

func deniedOnly(capabilities []domain.LogonCapability) bool {
	return len(capabilities) == 1 && capabilities[0] == domain.LogonDenied
}

func tier(score int, active bool) string {
	if !active {
		return "dormant"
	}
	switch {
	case score >= 420:
		return "system"
	case score >= 320:
		return "privileged"
	case score >= 220:
		return "elevated"
	case score >= 100:
		return "limited"
	default:
		return "unknown"
	}
}
