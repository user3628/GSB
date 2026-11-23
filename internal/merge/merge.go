package merge

import (
	"fmt"
	"sort"
	"strings"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/privilege"
)

type Options struct {
	Priorities []domain.SourceKind
}

type Adjudication struct {
	Services []domain.ServiceVerdict
	Issues   []domain.Issue
}

func Adjudicate(results []domain.SourceResult, options Options) Adjudication {
	priorities := options.Priorities
	if len(priorities) == 0 {
		priorities = domain.DefaultSourcePriorities
	}
	priorityIndex := make(map[domain.SourceKind]int, len(priorities))
	for index, kind := range priorities {
		priorityIndex[kind] = index
	}
	grouped := make(map[string][]domain.Declaration)
	for _, result := range results {
		for _, declaration := range result.Declarations {
			grouped[declaration.NormalizedServiceName] = append(grouped[declaration.NormalizedServiceName], declaration)
		}
	}
	serviceNames := make([]string, 0, len(grouped))
	for name := range grouped {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)
	adjudication := Adjudication{Services: make([]domain.ServiceVerdict, 0, len(serviceNames))}
	allIssues := collectIssues(results)
	for _, serviceName := range serviceNames {
		declarations := grouped[serviceName]
		sortDeclarations(declarations, priorityIndex)
		verdict, ok := adjudicateService(serviceName, declarations, priorityIndex)
		if !ok {
			continue
		}
		verdict.Issues = append(verdict.Issues, serviceIssues(allIssues, serviceName)...)
		sortIssues(verdict.Issues)
		codes := make([]string, 0, len(verdict.Issues))
		for _, issue := range verdict.Issues {
			codes = append(codes, string(issue.Code))
		}
		verdict.Codes = domain.SortedStrings(codes)
		adjudication.Services = append(adjudication.Services, verdict)
		for _, issue := range verdict.Issues {
			adjudication.Issues = append(adjudication.Issues, issue)
		}
	}
	sortIssues(adjudication.Issues)
	return adjudication
}

func adjudicateService(serviceName string, declarations []domain.Declaration, priorityIndex map[domain.SourceKind]int) (domain.ServiceVerdict, bool) {
	candidates := make([]domain.Declaration, 0, len(declarations))
	views := make([]domain.DeclarationView, 0, len(declarations))
	for _, declaration := range declarations {
		views = append(views, declarationView(declaration))
		if declaration.Usable {
			if _, configured := priorityIndex[declaration.Source.Kind]; configured {
				candidates = append(candidates, declaration)
			}
		}
	}
	if len(candidates) == 0 {
		return domain.ServiceVerdict{}, false
	}
	authority := candidates[0]
	accountDeclaration := authority
	if !authority.AccountPresent {
		for _, candidate := range candidates[1:] {
			if candidate.AccountPresent {
				accountDeclaration = candidate
				break
			}
		}
	}
	logonDeclaration := authority
	if !authority.LogonPresent {
		for _, candidate := range candidates[1:] {
			if candidate.LogonPresent {
				logonDeclaration = candidate
				break
			}
		}
	}
	profile := privilege.Build(accountDeclaration.AccountKind, authority.StartType, logonDeclaration.LogonCapabilities)
	conflicts := findConflicts(authority, candidates)
	riskFlags := make([]string, 0, 4)
	if profile.HighPrivilege {
		riskFlags = append(riskFlags, "HIGH_PRIVILEGE_ACCOUNT")
	}
	unquoted := false
	writable := false
	for _, candidate := range candidates {
		if candidate.Path.Unquoted && candidate.Path.HasSpace {
			unquoted = true
		}
		if candidate.Path.WritableDirectory {
			writable = true
		}
	}
	if unquoted {
		riskFlags = append(riskFlags, "UNQUOTED_IMAGE_PATH")
	}
	if writable {
		riskFlags = append(riskFlags, "WRITABLE_IMAGE_DIRECTORY")
	}
	issues := make([]domain.Issue, 0, len(conflicts)+2)
	for _, conflict := range conflicts {
		issues = append(issues, conflictIssue(serviceName, conflict))
	}
	if profile.HighPrivilege && writable {
		riskFlags = append(riskFlags, "PRIVILEGE_EXPOSURE")
		issues = append(issues, domain.Issue{
			Code:        domain.CodePrivilegeExposure,
			SourceID:    authority.Source.SourceID,
			SourceKind:  authority.Source.Kind,
			Offset:      authority.Source.Offset,
			End:         authority.Source.End,
			ServiceName: serviceName,
			Field:       "privilege_profile",
			Detail:      fmt.Sprintf("high-privilege account=%s start=%s image=%s writable=true", profile.AccountKind, profile.StartType, authority.Path.Normalized),
		})
	}
	considered := make([]domain.SourceRef, 0, len(candidates))
	for _, candidate := range candidates {
		considered = append(considered, candidate.Source)
	}
	fieldSources := []domain.FieldSource{
		{Field: "service_account", Source: accountDeclaration.Source, Value: string(accountDeclaration.AccountKind), Implicit: !accountDeclaration.AccountPresent},
		{Field: "start_type", Source: authority.Source, Value: string(authority.StartType), Implicit: false},
		{Field: "image_path", Source: authority.Source, Value: authority.Path.Normalized, Implicit: false},
		{Field: "logon_capabilities", Source: logonDeclaration.Source, Value: joinCapabilities(logonDeclaration.LogonCapabilities), Implicit: !logonDeclaration.LogonPresent},
	}
	verdict := domain.ServiceVerdict{
		ServiceName:  serviceName,
		DisplayName:  authority.DisplayName,
		Authority:    authority.Source,
		Declarations: views,
		Merge: domain.MergeSummary{
			Authority:    authority.Source,
			Considered:   considered,
			FieldSources: fieldSources,
			Conflicts:    conflicts,
		},
		ImagePath: authority.Path,
		Privilege: profile,
		RiskFlags: domain.SortedStrings(riskFlags),
		Issues:    issues,
	}
	return verdict, true
}

func findConflicts(authority domain.Declaration, candidates []domain.Declaration) []domain.Conflict {
	conflicts := make([]domain.Conflict, 0)
	for _, other := range candidates[1:] {
		if authority.StartType != other.StartType {
			conflicts = append(conflicts, domain.Conflict{
				Field:      "start_type",
				Authority:  conflictSide(authority.Source, string(authority.StartType), string(authority.StartType)),
				Challenger: conflictSide(other.Source, string(other.StartType), string(other.StartType)),
			})
		}
		if authority.Path.ImagePathKey != other.Path.ImagePathKey {
			conflicts = append(conflicts, domain.Conflict{
				Field:      "image_path",
				Authority:  conflictSide(authority.Source, authority.ImagePath, authority.Path.Normalized),
				Challenger: conflictSide(other.Source, other.ImagePath, other.Path.Normalized),
			})
		}
		if authority.AccountPresent && other.AccountPresent && accountKey(authority) != accountKey(other) {
			conflicts = append(conflicts, domain.Conflict{
				Field:      "service_account",
				Authority:  conflictSide(authority.Source, authority.AccountRaw, accountKey(authority)),
				Challenger: conflictSide(other.Source, other.AccountRaw, accountKey(other)),
			})
		}
	}
	sort.SliceStable(conflicts, func(i, j int) bool {
		left := conflictFieldOrder(conflicts[i].Field)
		right := conflictFieldOrder(conflicts[j].Field)
		if left != right {
			return left < right
		}
		if conflicts[i].Challenger.Source.SourceID != conflicts[j].Challenger.Source.SourceID {
			return conflicts[i].Challenger.Source.SourceID < conflicts[j].Challenger.Source.SourceID
		}
		return conflicts[i].Challenger.Source.Offset < conflicts[j].Challenger.Source.Offset
	})
	return conflicts
}

func conflictFieldOrder(field string) int {
	switch field {
	case "start_type":
		return 0
	case "image_path":
		return 1
	case "service_account":
		return 2
	default:
		return 3
	}
}

func conflictSide(ref domain.SourceRef, raw, normalized string) domain.ConflictSide {
	return domain.ConflictSide{Source: ref, Raw: raw, Normalized: normalized}
}

func conflictIssue(serviceName string, conflict domain.Conflict) domain.Issue {
	detail := fmt.Sprintf("field=%s authority_source=%s authority_offset=%d authority_raw=%q authority_normalized=%q challenger_source=%s challenger_offset=%d challenger_raw=%q challenger_normalized=%q",
		conflict.Field,
		conflict.Authority.Source.SourceID,
		conflict.Authority.Source.Offset,
		conflict.Authority.Raw,
		conflict.Authority.Normalized,
		conflict.Challenger.Source.SourceID,
		conflict.Challenger.Source.Offset,
		conflict.Challenger.Raw,
		conflict.Challenger.Normalized,
	)
	return domain.Issue{
		Code:        domain.CodeDeclarationConflict,
		SourceID:    conflict.Challenger.Source.SourceID,
		SourceKind:  conflict.Challenger.Source.Kind,
		Offset:      conflict.Challenger.Source.Offset,
		End:         conflict.Challenger.Source.End,
		ServiceName: serviceName,
		Field:       conflict.Field,
		Detail:      detail,
	}
}

func declarationView(declaration domain.Declaration) domain.DeclarationView {
	return domain.DeclarationView{
		Source:            declaration.Source,
		DisplayName:       declaration.DisplayName,
		StartType:         declaration.StartType,
		ImagePath:         declaration.ImagePath,
		Path:              declaration.Path,
		AccountRaw:        declaration.AccountRaw,
		AccountKind:       declaration.AccountKind,
		AccountPresent:    declaration.AccountPresent,
		LogonCapabilities: declaration.LogonCapabilities,
		LogonPresent:      declaration.LogonPresent,
		Privilege:         declaration.Privilege,
		Usable:            declaration.Usable,
		Codes:             domain.SortedStrings(declaration.Codes),
	}
}

func accountKey(declaration domain.Declaration) string {
	if declaration.AccountKind == domain.AccountCustom {
		return string(declaration.AccountKind) + ":" + domain.NormalizeWindowsToken(declaration.AccountRaw)
	}
	return string(declaration.AccountKind)
}

func joinCapabilities(capabilities []domain.LogonCapability) string {
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return strings.Join(values, ",")
}

func sortDeclarations(declarations []domain.Declaration, priorityIndex map[domain.SourceKind]int) {
	sort.SliceStable(declarations, func(i, j int) bool {
		leftPriority, leftOK := priorityIndex[declarations[i].Source.Kind]
		if !leftOK {
			leftPriority = len(priorityIndex) + 100
		}
		rightPriority, rightOK := priorityIndex[declarations[j].Source.Kind]
		if !rightOK {
			rightPriority = len(priorityIndex) + 100
		}
		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		if declarations[i].Source.SourceID != declarations[j].Source.SourceID {
			return declarations[i].Source.SourceID < declarations[j].Source.SourceID
		}
		return declarations[i].Source.Offset < declarations[j].Source.Offset
	})
}

func collectIssues(results []domain.SourceResult) []domain.Issue {
	issues := make([]domain.Issue, 0)
	for _, result := range results {
		issues = append(issues, result.Issues...)
	}
	sortIssues(issues)
	return issues
}

func serviceIssues(issues []domain.Issue, serviceName string) []domain.Issue {
	out := make([]domain.Issue, 0)
	for _, issue := range issues {
		if domain.NormalizeServiceName(issue.ServiceName) == serviceName {
			out = append(out, issue)
		}
	}
	return out
}

func sortIssues(issues []domain.Issue) {
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].ServiceName != issues[j].ServiceName {
			return issues[i].ServiceName < issues[j].ServiceName
		}
		if issues[i].Code != issues[j].Code {
			return issues[i].Code < issues[j].Code
		}
		if issues[i].SourceID != issues[j].SourceID {
			return issues[i].SourceID < issues[j].SourceID
		}
		if issues[i].Offset != issues[j].Offset {
			return issues[i].Offset < issues[j].Offset
		}
		if issues[i].Detail != issues[j].Detail {
			return issues[i].Detail < issues[j].Detail
		}
		return issues[i].End < issues[j].End
	})
}

func SortIssues(issues []domain.Issue) {
	sortIssues(issues)
}
