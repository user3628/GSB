package privilege

import (
	"testing"

	"svcworkbench/internal/domain"
)

func TestAccountLevelsAreTransitive(t *testing.T) {
	levels := []int{
		domain.AccountLevel(domain.AccountUnknown),
		domain.AccountLevel(domain.AccountCustom),
		domain.AccountLevel(domain.AccountLocalService),
		domain.AccountLevel(domain.AccountNetworkService),
		domain.AccountLevel(domain.AccountLocalSystem),
	}
	for index := 1; index < len(levels); index++ {
		if levels[index] <= levels[index-1] {
			t.Fatalf("levels are not strictly ordered: %#v", levels)
		}
	}
}

func TestProfileCombinesAccountStartAndLogon(t *testing.T) {
	profile := Build(
		domain.AccountLocalSystem,
		domain.StartAutomatic,
		[]domain.LogonCapability{domain.LogonService, domain.LogonInteractive},
	)
	if !profile.HighPrivilege || !profile.Active {
		t.Fatalf("expected active high-privilege profile: %+v", profile)
	}
	if profile.AccountLevel != 4 || profile.StartupLevel != 2 || profile.LogonLevel != 4 {
		t.Fatalf("unexpected levels: %+v", profile)
	}
	if profile.EffectiveScore != 424 || profile.EffectiveTier != "system" {
		t.Fatalf("unexpected effective profile: %+v", profile)
	}
}

func TestDisabledServiceIsDormant(t *testing.T) {
	profile := Build(domain.AccountLocalSystem, domain.StartDisabled, []domain.LogonCapability{domain.LogonService})
	if profile.Active || profile.HighPrivilege || profile.EffectiveTier != "dormant" {
		t.Fatalf("disabled service should be dormant: %+v", profile)
	}
}

func TestDeniedOnlyIsDormant(t *testing.T) {
	profile := Build(domain.AccountNetworkService, domain.StartAutomatic, []domain.LogonCapability{domain.LogonDenied})
	if profile.Active || profile.HighPrivilege {
		t.Fatalf("denied-only logon should be inactive: %+v", profile)
	}
}
