package scenario

import mfaengine "mfaengine"

const DemoClock int64 = 1_700_000_000

func DemoConfig() mfaengine.Config {
	return mfaengine.Config{
		ClockUnix: DemoClock,
		Limits: mfaengine.Limits{
			MaxConcurrency:    2,
			MaxPendingPerUser: 1,
			FailureThreshold:  2,
			TOTPStepSeconds:   30,
			TOTPWindow:        1,
		},
		Tenants: []mfaengine.TenantDeclaration{
			{ID: "acme", Name: "Acme"},
			{ID: "delta", Name: "Delta"},
			{ID: "epsilon", Name: "Epsilon"},
			{ID: "eta", Name: "Eta"},
			{ID: "gamma", Name: "Gamma"},
			{ID: "iota", Name: "Iota"},
			{ID: "kappa", Name: "Kappa"},
			{ID: "theta", Name: "Theta"},
			{ID: "zeta", Name: "Zeta"},
		},
		Users: []mfaengine.UserDeclaration{
			{ID: "alice", TenantID: "acme", Name: "Alice"},
			{ID: "dave", TenantID: "delta", Name: "Dave"},
			{ID: "erin", TenantID: "epsilon", Name: "Erin"},
			{ID: "eva", TenantID: "eta", Name: "Eva"},
			{ID: "gina", TenantID: "gamma", Name: "Gina"},
			{ID: "ian", TenantID: "iota", Name: "Ian"},
			{ID: "kay", TenantID: "kappa", Name: "Kay"},
			{ID: "tina", TenantID: "theta", Name: "Tina"},
			{ID: "zoe", TenantID: "zeta", Name: "Zoe"},
		},
		Policies: []mfaengine.PolicyDeclaration{
			{TenantID: "acme", MinStrength: 3, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityHigh},
				{Type: mfaengine.FactorRecovery, MinSensitivity: mfaengine.SensitivityHigh},
			}},
			{TenantID: "delta", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityMedium},
			}},
			{TenantID: "epsilon", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorWebAuthn, MinSensitivity: mfaengine.SensitivityHigh},
			}},
			{TenantID: "eta", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityMedium},
			}},
			{TenantID: "gamma", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityMedium},
			}},
			{TenantID: "iota", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorWebAuthn, MinSensitivity: mfaengine.SensitivityMedium},
			}},
			{TenantID: "kappa", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityMedium},
			}},
			{TenantID: "theta", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityMedium},
			}},
			{TenantID: "zeta", MinStrength: 2, RequiredFactors: []mfaengine.PolicyRequiredFactor{
				{Type: mfaengine.FactorTOTP, MinSensitivity: mfaengine.SensitivityMedium},
			}},
		},
		Factors: []mfaengine.FactorDeclaration{
			{ID: "f-alice-totp", TenantID: "acme", UserID: "alice", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityHigh, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-alice-recovery", TenantID: "acme", UserID: "alice", Type: mfaengine.FactorRecovery, Sensitivity: mfaengine.SensitivityHigh, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true, RecoveryCodeCount: 3},
			{ID: "f-alice-webauthn", TenantID: "acme", UserID: "alice", Type: mfaengine.FactorWebAuthn, Sensitivity: mfaengine.SensitivityHigh, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-dave-totp", TenantID: "delta", UserID: "dave", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-erin-webauthn", TenantID: "epsilon", UserID: "erin", Type: mfaengine.FactorWebAuthn, Sensitivity: mfaengine.SensitivityHigh, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-eva-totp", TenantID: "eta", UserID: "eva", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true, ExpiresAtUnix: DemoClock - 1},
			{ID: "f-gina-totp", TenantID: "gamma", UserID: "gina", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-ian-webauthn", TenantID: "iota", UserID: "ian", Type: mfaengine.FactorWebAuthn, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-kay-totp", TenantID: "kappa", UserID: "kay", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusPending, Rebindable: true},
			{ID: "f-tina-totp", TenantID: "theta", UserID: "tina", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
			{ID: "f-zoe-totp", TenantID: "zeta", UserID: "zoe", Type: mfaengine.FactorTOTP, Sensitivity: mfaengine.SensitivityMedium, Status: mfaengine.StatusActive, Rebindable: true, LastActivated: true},
		},
	}
}
