package recipe

import "testing"

func TestRegistryValidatesAndReturnsPlan(t *testing.T) {
	registry := NewRegistry()
	plan := DefaultPlan()
	if err := registry.Put(plan); err != nil {
		t.Fatal(err)
	}
	stored, err := registry.Get(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ID != plan.ID || len(stored.Steps) != len(plan.Steps) {
		t.Fatalf("unexpected stored plan: %#v", stored)
	}
}
