package sample

import (
	"fmt"
	"math/rand"

	"netpolcompiler/internal/model"
)

const (
	DefaultSeed    int64 = 327
	SampleVersion  int64 = 1
	FixedTimestamp       = "2026-11-26T00:00:00Z"
)

func intPtr(value int) *int { return &value }

func GenerateSnapshot(seed int64) model.Snapshot {
	if seed == 0 {
		seed = DefaultSeed
	}
	rng := rand.New(rand.NewSource(seed))
	rules := []model.Rule{
		{
			Namespace: "demo", ID: "demo-allow-edge", DeclarationOrder: intPtr(1),
			Action: model.ActionAllow, Priority: 100, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "edge"}, Exists: []string{"env"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "web", "tier": "frontend"}},
			Ports:       []model.PortRange{{Start: 8080, End: 8090}},
		},
		{
			Namespace: "demo", ID: "demo-deny-overlap", DeclarationOrder: intPtr(2),
			Action: model.ActionDeny, Priority: 100, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "edge"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "web", "tier": "frontend"}},
			Ports:       []model.PortRange{{Start: 8085, End: 8095}},
		},
		{
			Namespace: "demo", ID: "demo-allow-web-broad", DeclarationOrder: intPtr(3),
			Action: model.ActionAllow, Priority: 10, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Ports:       []model.PortRange{{Start: 443, End: 443}},
		},
		{
			Namespace: "demo", ID: "demo-deny-db", DeclarationOrder: intPtr(4),
			Action: model.ActionDeny, Priority: 200, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "analytics"}, NotIn: []model.NotInRequirement{{Key: "breakglass", Values: []string{"true"}}}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "db"}},
			Ports:       []model.PortRange{{Start: 5432, End: 5432}},
		},
		{
			Namespace: "demo", ID: "demo-allow-db-readonly", DeclarationOrder: intPtr(5),
			Action: model.ActionAllow, Priority: 50, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "analytics"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "db", "mode": "ro"}},
			Ports:       []model.PortRange{{Start: 5432, End: 5432}},
		},
		{
			Namespace: "demo", ID: "demo-allow-dns", DeclarationOrder: intPtr(6),
			Action: model.ActionAllow, Priority: 20, Protocol: model.ProtocolUDP,
			Source:      model.LabelSelector{Exists: []string{"pod"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "dns"}},
			Ports:       []model.PortRange{{Start: 53, End: 53}},
		},
		{
			Namespace: "demo", ID: "demo-deny-dns-zone", DeclarationOrder: intPtr(7),
			Action: model.ActionDeny, Priority: 30, Protocol: model.ProtocolUDP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"pod": "zone-updater"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "dns"}},
			Ports:       []model.PortRange{{Start: 53, End: 54}},
		},
		{
			Namespace: "demo", ID: "demo-allow-metrics-a", DeclarationOrder: intPtr(8),
			Action: model.ActionAllow, Priority: 40, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "metrics"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "collector"}},
			Ports:       []model.PortRange{{Start: 9090, End: 9095}},
		},
		{
			Namespace: "demo", ID: "demo-allow-metrics-b", DeclarationOrder: intPtr(9),
			Action: model.ActionAllow, Priority: 40, Protocol: model.ProtocolTCP,
			Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "metrics"}},
			Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "collector"}},
			Ports:       []model.PortRange{{Start: 9096, End: 9100}},
		},
	}

	namespaces := []string{"alpha", "beta", "gamma"}
	for nsIndex, namespace := range namespaces {
		for i := 0; i < 5; i++ {
			action := model.ActionAllow
			if rng.Intn(100) < 35+nsIndex*10 {
				action = model.ActionDeny
			}
			app := []string{"api", "cache", "queue"}[rng.Intn(3)]
			rules = append(rules, model.Rule{
				Namespace:        namespace,
				ID:               fmt.Sprintf("%s-rule-%02d", namespace, i+1),
				DeclarationOrder: intPtr(i + 1),
				Action:           action,
				Priority:         10 * (1 + rng.Intn(20)),
				Protocol:         model.ProtocolTCP,
				Source: model.LabelSelector{
					MatchLabels: map[string]string{"team": namespace, "tier": fmt.Sprintf("t%d", rng.Intn(3))},
					Exists:      []string{"pod"},
				},
				Destination: model.LabelSelector{
					MatchLabels: map[string]string{"app": app},
				},
				Ports: []model.PortRange{{Start: 8000 + rng.Intn(20), End: 8020 + rng.Intn(80)}},
			})
		}
	}

	snapshot := model.Snapshot{
		Version:     SampleVersion,
		GeneratedAt: FixedTimestamp,
		Rules:       rules,
	}
	snapshot.Digest = model.DigestSnapshot(snapshot)
	return snapshot
}

func GenerateIncrement(baseVersion int64, requestID string) model.Increment {
	if baseVersion <= 0 {
		baseVersion = SampleVersion
	}
	changedOrder := 101
	return model.Increment{
		RequestID:   requestID,
		BaseVersion: baseVersion,
		GeneratedAt: FixedTimestamp,
		Changes: []model.RuleChange{
			{
				Op: "upsert",
				Rule: &model.Rule{
					Namespace: "demo", ID: "demo-allow-edge", DeclarationOrder: &changedOrder,
					Action: model.ActionAllow, Priority: 120, Protocol: model.ProtocolTCP,
					Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "edge"}, Exists: []string{"env"}},
					Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "web", "tier": "frontend"}},
					Ports:       []model.PortRange{{Start: 8080, End: 8092}},
				},
			},
			{
				Op: "upsert",
				Rule: &model.Rule{
					Namespace: "demo", ID: "demo-added-by-increment", DeclarationOrder: intPtr(20),
					Action: model.ActionDeny, Priority: 150, Protocol: model.ProtocolTCP,
					Source:      model.LabelSelector{MatchLabels: map[string]string{"role": "batch"}},
					Destination: model.LabelSelector{MatchLabels: map[string]string{"app": "billing"}},
					Ports:       []model.PortRange{{Start: 9000, End: 9010}},
				},
			},
		},
	}
}

func ShuffleRulesForTest(rules []model.Rule, seed int64) []model.Rule {
	rng := rand.New(rand.NewSource(seed + 999))
	shuffled := append([]model.Rule(nil), rules...)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return shuffled
}
