package recipe_test

import (
	"testing"

	"github.com/wyw14/cry-149/internal/recipe"
)

type absentPolicyResolver struct{}

func (absentPolicyResolver) Optional(string) *recipe.OptionalProbePolicy { return nil }

func TestAbsentOptionalProbePolicyDoesNotBecomeCallableNil(t *testing.T) {
	registry := recipe.NewRegistry()
	plan := recipe.DefaultPlan()
	if err := registry.Put(plan); err != nil {
		t.Fatal(err)
	}
	coordinator := recipe.NewCoordinator(registry, absentPolicyResolver{})
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("optional policy caused panic: %v", recovered)
		}
	}()
	expanded, err := coordinator.Expand("B-NO-FOAM", plan.ID, map[string]float64{})
	if err != nil {
		t.Fatal(err)
	}
	if len(expanded.Steps) != len(plan.Steps) {
		t.Fatalf("absent optional policy changed plan length to %d", len(expanded.Steps))
	}
}
