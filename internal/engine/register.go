package engine

import "regexp"

var (
	deviceIDRe    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,63}$`)
	tenantIDRe    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,63}$`)
	fingerprintRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
	capabilityRe  = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,31}$`)
)

// validateClaim checks a device declaration without writing any device
// state. Only fully valid claims are recorded in the seen maps, so a
// rejected claim leaves no trace.
func (e *Engine) validateClaim(c *DeviceClaim, seenDev map[string]bool, seenFP map[string]string) *Error {
	if !deviceIDRe.MatchString(c.DeviceID) {
		return errf(CodeDeviceInvalid, "malformed device id %q", c.DeviceID)
	}
	if !tenantIDRe.MatchString(c.TenantID) {
		return errf(CodeDeviceInvalid, "malformed tenant id %q", c.TenantID)
	}
	if _, ok := e.policies[c.TenantID]; !ok {
		return errf(CodeDeviceInvalid, "tenant %q not resolvable", c.TenantID)
	}
	key := deviceKey(c.TenantID, c.DeviceID)
	if seenDev[key] {
		return errf(CodeDeviceInvalid, "duplicate device id %q in batch", key)
	}
	if !fingerprintRe.MatchString(c.Fingerprint) {
		return errf(CodeDeviceInvalid, "malformed fingerprint")
	}
	if prev, dup := seenFP[c.Fingerprint]; dup {
		return errf(CodeDeviceInvalid, "fingerprint duplicates %s", prev)
	}
	for otherKey, st := range e.states {
		if otherKey != key && st.Fingerprint == c.Fingerprint {
			return errf(CodeDeviceInvalid, "fingerprint already owned by %s", otherKey)
		}
	}
	if len(c.Capabilities) == 0 {
		return errf(CodeDeviceInvalid, "no capabilities declared")
	}
	for _, cap := range c.Capabilities {
		if !capabilityRe.MatchString(cap) {
			return errf(CodeDeviceInvalid, "malformed capability %q", cap)
		}
	}
	seenSeq := map[int64]bool{}
	for _, ev := range c.Events {
		if ev.Seq < 0 {
			return errf(CodeDeviceInvalid, "negative event seq %d", ev.Seq)
		}
		if seenSeq[ev.Seq] {
			return errf(CodeDeviceInvalid, "duplicate event seq %d", ev.Seq)
		}
		seenSeq[ev.Seq] = true
	}
	seenDev[key] = true
	seenFP[c.Fingerprint] = key
	return nil
}
