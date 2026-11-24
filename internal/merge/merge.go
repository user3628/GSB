package merge

import (
	"fmt"
	"sort"

	"supplyaudit/internal/model"
	"supplyaudit/internal/normalize"
)

// Config defines source authority by format. The first format has the highest
// authority. Input collection order never affects the result.
type Config struct {
	Priority []model.SourceFormat `json:"priority"`
}

func (c Config) Rank(format model.SourceFormat) int {
	for index, candidate := range c.Priority {
		if candidate == format {
			return index
		}
	}
	return len(c.Priority) + formatFallbackRank(format)
}

func formatFallbackRank(format model.SourceFormat) int {
	switch format {
	case model.FormatBinary:
		return 0
	case model.FormatJSONL:
		return 1
	case model.FormatTextBlock:
		return 2
	default:
		return 3
	}
}

type VersionGroup struct {
	MergeKey      string               `json:"merge_key"`
	ComponentKey  string               `json:"component_key"`
	Version       model.VersionView    `json:"version"`
	AuthorityID   string               `json:"authority_id"`
	Authority     model.Declaration    `json:"authority"`
	Declarations  []model.Declaration  `json:"declarations"`
	Codes         []string             `json:"codes"`
	SourceFormats []model.SourceFormat `json:"source_formats"`
}

type ComponentView struct {
	ComponentKey     string            `json:"component_key"`
	HighestVersion   model.VersionView `json:"highest_version"`
	Versions         []VersionGroup    `json:"versions"`
	Codes            []string          `json:"codes"`
	DeclarationCount int               `json:"declaration_count"`
	SourceCount      int               `json:"source_count"`
}

type SourceCoverage struct {
	SourceID     string             `json:"source_id"`
	SourcePath   string             `json:"source_path"`
	SourceFormat model.SourceFormat `json:"source_format"`
	Declarations int                `json:"declarations"`
	Accounted    int                `json:"accounted"`
	Complete     bool               `json:"complete"`
}

type Result struct {
	Components         []ComponentView  `json:"components"`
	Findings           []model.Finding  `json:"findings"`
	DeclarationCount   int              `json:"declaration_count"`
	MergeGroupCount    int              `json:"merge_group_count"`
	ComponentCount     int              `json:"component_count"`
	SourceCoverage     []SourceCoverage `json:"source_coverage"`
	SelfConsistent     bool             `json:"self_consistent"`
	ConsistencyMessage string           `json:"consistency_message"`
}

func Run(sources []*model.SourceResult, config Config) Result {
	type keyedDeclaration struct {
		decl model.Declaration
	}

	all := make([]keyedDeclaration, 0)
	coverage := make([]SourceCoverage, 0, len(sources))
	for _, source := range sources {
		coverage = append(coverage, SourceCoverage{
			SourceID:     source.Spec.ID,
			SourcePath:   source.Spec.ReportPath,
			SourceFormat: source.Spec.Format,
			Declarations: len(source.Declarations),
		})
		for _, decl := range source.Declarations {
			if decl.ID == "" {
				decl.ID = stableDeclarationID(decl)
			}
			decl.Codes = model.NormalizeCodes(decl.Codes)
			all = append(all, keyedDeclaration{decl: decl})
		}
	}

	byComponent := make(map[string][]model.Declaration)
	for _, item := range all {
		byComponent[item.decl.ComponentKey] = append(byComponent[item.decl.ComponentKey], item.decl)
	}
	componentKeys := make([]string, 0, len(byComponent))
	for key := range byComponent {
		componentKeys = append(componentKeys, key)
	}
	sort.Strings(componentKeys)

	result := Result{
		Components:       make([]ComponentView, 0, len(componentKeys)),
		Findings:         []model.Finding{},
		DeclarationCount: len(all),
	}
	accountedBySource := make(map[string]int)
	seenIDs := make(map[string]struct{})
	for _, componentKey := range componentKeys {
		declarations := byComponent[componentKey]
		groups := make(map[string][]model.Declaration)
		for _, decl := range declarations {
			mergeKey := model.CanonicalJoin(decl.ComponentKey, decl.Version.Canonical)
			groups[mergeKey] = append(groups[mergeKey], decl)
		}
		mergeKeys := make([]string, 0, len(groups))
		for key := range groups {
			mergeKeys = append(mergeKeys, key)
		}
		sort.Slice(mergeKeys, func(i, j int) bool {
			left, right := groups[mergeKeys[i]][0].Version, groups[mergeKeys[j]][0].Version
			if comparison := normalize.CompareVersions(left, right); comparison != 0 {
				return comparison > 0
			}
			return mergeKeys[i] < mergeKeys[j]
		})

		component := ComponentView{
			ComponentKey: componentKey,
			Versions:     make([]VersionGroup, 0, len(mergeKeys)),
			Codes:        []string{},
		}
		component.HighestVersion = groups[mergeKeys[0]][0].Version
		maxEffectiveBits := 0
		sourceSet := make(map[string]struct{})
		for _, declarations := range groups {
			for _, decl := range declarations {
				if decl.Digest.EffectiveBits > maxEffectiveBits {
					maxEffectiveBits = decl.Digest.EffectiveBits
				}
				sourceSet[decl.SourcePath] = struct{}{}
			}
		}

		for _, mergeKey := range mergeKeys {
			groupDeclarations := append([]model.Declaration(nil), groups[mergeKey]...)
			sort.SliceStable(groupDeclarations, func(i, j int) bool {
				left, right := groupDeclarations[i], groupDeclarations[j]
				leftRank, rightRank := config.Rank(left.SourceFormat), config.Rank(right.SourceFormat)
				if leftRank != rightRank {
					return leftRank < rightRank
				}
				if left.SourcePath != right.SourcePath {
					return left.SourcePath < right.SourcePath
				}
				if left.Offset != right.Offset {
					return left.Offset < right.Offset
				}
				return left.ID < right.ID
			})
			authority := groupDeclarations[0]
			group := VersionGroup{
				MergeKey:     mergeKey,
				ComponentKey: componentKey,
				Version:      authority.Version,
				AuthorityID:  authority.ID,
				Authority:    authority,
				Declarations: groupDeclarations,
				Codes:        []string{},
			}
			formatSet := make(map[model.SourceFormat]struct{})
			for index := range group.Declarations {
				formatSet[group.Declarations[index].SourceFormat] = struct{}{}
				accountedBySource[group.Declarations[index].SourceID]++
				if _, exists := seenIDs[group.Declarations[index].ID]; exists {
					result.SelfConsistent = false
					result.ConsistencyMessage = "duplicate declaration id " + group.Declarations[index].ID
				}
				seenIDs[group.Declarations[index].ID] = struct{}{}
			}
			formats := make([]model.SourceFormat, 0, len(formatSet))
			for format := range formatSet {
				formats = append(formats, format)
			}
			sort.Slice(formats, func(i, j int) bool { return formats[i] < formats[j] })
			group.SourceFormats = formats

			// Digest conflicts are scoped to the same normalized merge key. The
			// priority-selected authority is never displaced by collection order.
			for index := 1; index < len(group.Declarations); index++ {
				other := group.Declarations[index]
				if authority.Digest.Equal(other.Digest) {
					continue
				}
				finding := model.Finding{
					Code:         model.CodeDigestConflict,
					ComponentKey: componentKey,
					Version:      group.Version.Canonical,
					Message:      fmt.Sprintf("authoritative %s digest differs from %s digest", authority.SourcePath, other.SourcePath),
					Primary:      evidence(authority),
					Secondary:    evidencePtr(other),
				}
				result.Findings = append(result.Findings, finding)
				group.Codes = model.AppendUniqueCode(group.Codes, model.CodeDigestConflict)
				group.Declarations[0].Codes = model.AppendUniqueCode(group.Declarations[0].Codes, model.CodeDigestConflict)
				group.Declarations[index].Codes = model.AppendUniqueCode(group.Declarations[index].Codes, model.CodeDigestConflict)
			}

			// A declaration is weak when its effective digest width is below the
			// strongest declaration retained for this component.
			for index := range group.Declarations {
				decl := &group.Declarations[index]
				if decl.Digest.EffectiveBits >= maxEffectiveBits {
					continue
				}
				finding := model.Finding{
					Code:         model.CodeWeakDigest,
					ComponentKey: componentKey,
					Version:      group.Version.Canonical,
					Message:      fmt.Sprintf("%s digest has %d effective bits, component maximum is %d", decl.Digest.Algorithm, decl.Digest.EffectiveBits, maxEffectiveBits),
					Primary:      evidence(*decl),
				}
				result.Findings = append(result.Findings, finding)
				group.Codes = model.AppendUniqueCode(group.Codes, model.CodeWeakDigest)
				decl.Codes = model.AppendUniqueCode(decl.Codes, model.CodeWeakDigest)
			}

			if normalize.CompareVersions(group.Version, component.HighestVersion) < 0 {
				finding := model.Finding{
					Code:         model.CodeVersionDowngrade,
					ComponentKey: componentKey,
					Version:      group.Version.Canonical,
					Message:      fmt.Sprintf("declared version %s is below component maximum %s", group.Version.Canonical, component.HighestVersion.Canonical),
					Primary:      evidence(authority),
				}
				result.Findings = append(result.Findings, finding)
				group.Codes = model.AppendUniqueCode(group.Codes, model.CodeVersionDowngrade)
				for index := range group.Declarations {
					group.Declarations[index].Codes = model.AppendUniqueCode(group.Declarations[index].Codes, model.CodeVersionDowngrade)
				}
			}

			group.Codes = model.NormalizeCodes(group.Codes)
			for index := range group.Declarations {
				group.Declarations[index].Codes = model.NormalizeCodes(group.Declarations[index].Codes)
				component.Codes = append(component.Codes, group.Declarations[index].Codes...)
			}
			group.Authority = group.Declarations[0]
			component.Codes = append(component.Codes, group.Codes...)
			component.DeclarationCount += len(group.Declarations)
			component.Versions = append(component.Versions, group)
		}
		component.Codes = model.NormalizeCodes(component.Codes)
		component.SourceCount = len(sourceSet)
		result.Components = append(result.Components, component)
	}
	result.ComponentCount = len(result.Components)
	result.MergeGroupCount = 0
	for _, component := range result.Components {
		result.MergeGroupCount += len(component.Versions)
	}
	for index := range coverage {
		coverage[index].Accounted = accountedBySource[coverage[index].SourceID]
		coverage[index].Complete = coverage[index].Accounted == coverage[index].Declarations
	}
	sort.Slice(coverage, func(i, j int) bool {
		if coverage[i].SourcePath != coverage[j].SourcePath {
			return coverage[i].SourcePath < coverage[j].SourcePath
		}
		return coverage[i].SourceID < coverage[j].SourceID
	})
	result.SourceCoverage = coverage
	if len(seenIDs) != len(all) {
		result.SelfConsistent = false
		if result.ConsistencyMessage == "" {
			result.ConsistencyMessage = "not every source declaration was merged exactly once"
		}
	}
	if result.ConsistencyMessage == "" {
		result.SelfConsistent = true
		result.ConsistencyMessage = "every source declaration is accounted for exactly once"
	}
	sortFindings(result.Findings)
	return result
}

func stableDeclarationID(decl model.Declaration) string {
	return fmt.Sprintf("%s#%d#%d", decl.SourceID, decl.Offset, decl.RecordIndex)
}

func evidence(decl model.Declaration) model.Evidence {
	return model.Evidence{
		DeclarationID: decl.ID,
		SourceID:      decl.SourceID,
		SourcePath:    decl.SourcePath,
		SourceFormat:  decl.SourceFormat,
		Offset:        decl.Offset,
		ComponentKey:  decl.ComponentKey,
		Version:       decl.Version.Canonical,
		Algorithm:     decl.Digest.Algorithm,
		DigestHex:     decl.Digest.Hex,
		RawDigest:     decl.RawDigest,
	}
}

func evidencePtr(decl model.Declaration) *model.Evidence {
	value := evidence(decl)
	return &value
}

func sortFindings(findings []model.Finding) {
	sort.Slice(findings, func(i, j int) bool {
		left, right := findings[i], findings[j]
		if left.ComponentKey != right.ComponentKey {
			return left.ComponentKey < right.ComponentKey
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		if left.Primary.SourcePath != right.Primary.SourcePath {
			return left.Primary.SourcePath < right.Primary.SourcePath
		}
		if left.Primary.Offset != right.Primary.Offset {
			return left.Primary.Offset < right.Primary.Offset
		}
		if left.Secondary != nil && right.Secondary != nil {
			if left.Secondary.SourcePath != right.Secondary.SourcePath {
				return left.Secondary.SourcePath < right.Secondary.SourcePath
			}
			if left.Secondary.Offset != right.Secondary.Offset {
				return left.Secondary.Offset < right.Secondary.Offset
			}
		}
		if left.Version != right.Version {
			return left.Version < right.Version
		}
		return left.Message < right.Message
	})
}

func ContainsCode(findings []model.Finding, code string) bool {
	return model.FindingsHaveCode(findings, code)
}
