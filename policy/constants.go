package policy

const (
	KindDirectory    = "directory"
	KindProcessGroup = "process_group"
	KindRegistryPath = "registry_path"
	KindRule         = "rule"

	EffectAllow = "allow"
	EffectDeny  = "deny"

	ActionFileRead      = "file.read"
	ActionFileWrite     = "file.write"
	ActionProcessStart  = "process.start"
	ActionRegistryWrite = "registry.write"

	OpAdd    = "add"
	OpModify = "modify"
	OpDelete = "delete"
)

var actionKind = map[string]string{
	ActionFileRead:      KindDirectory,
	ActionFileWrite:     KindDirectory,
	ActionProcessStart:  KindProcessGroup,
	ActionRegistryWrite: KindRegistryPath,
}

var validAccess = map[string]struct{}{
	"read":  {},
	"write": {},
}
