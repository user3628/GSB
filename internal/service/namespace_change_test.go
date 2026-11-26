package service_test

import (
	"context"
	"testing"

	"netpolcompiler/internal/model"
	"netpolcompiler/internal/service"
)

func TestReplaceNamespaceIncrementRebuildsEmptyAndNewRules(t *testing.T) {
	svc := newTestService(t, service.Hooks{})
	seedSample(t, svc)

	order := 1
	increment := model.Increment{
		RequestID:   "replace-namespace",
		BaseVersion: 1,
		Changes: []model.RuleChange{{
			Op:        "replace_namespace",
			Namespace: "scratch",
			Rules: []model.Rule{{
				ID: "only-rule", Namespace: "scratch", Action: model.ActionAllow,
				Source: model.LabelSelector{}, Destination: model.LabelSelector{},
				Ports: []model.PortRange{{Start: 443, End: 443}}, Protocol: model.ProtocolTCP,
				Priority: 10, DeclarationOrder: &order,
			}},
		}},
	}
	result := svc.SubmitIncrement(context.Background(), increment)
	if result.Status != 200 {
		t.Fatalf("replace namespace failed: %s", string(result.Body))
	}
	view, bizError := svc.GetView("scratch", 2)
	if bizError != nil {
		t.Fatalf("get rebuilt namespace: %+v", bizError)
	}
	if view.UsedVersion != 2 || view.View.RuleCount != 1 {
		t.Fatalf("unexpected rebuilt view version=%d rules=%d", view.UsedVersion, view.View.RuleCount)
	}
}
