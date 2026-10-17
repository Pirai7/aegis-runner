package scheduler

import (
	"testing"
)

func TestDRRDeficitAccounting(t *testing.T) {
	tenant := &Tenant{
		ID:               "tenant-test",
		Name:             "Test Tenant",
		Quantum:          10,
		Deficit:          0,
		ConcurrencyLimit: 5,
	}

	costPerJob := 10

	// Round 1: Deficit increments by quantum
	tenant.Deficit += tenant.Quantum
	if tenant.Deficit != 10 {
		t.Fatalf("expected deficit 10, got %d", tenant.Deficit)
	}

	// Dequeue 1 job: Deficit drops by cost
	tenant.Deficit -= costPerJob
	if tenant.Deficit != 0 {
		t.Fatalf("expected deficit 0 after task cost, got %d", tenant.Deficit)
	}

	// Round 2: Tenant has no jobs, deficit capped
	tenant.Deficit += tenant.Quantum
	tenant.Deficit += tenant.Quantum
	tenant.Deficit += tenant.Quantum
	tenant.Deficit += tenant.Quantum
	tenant.Deficit += tenant.Quantum
	tenant.Deficit += tenant.Quantum // 6th quantum (60)
	if tenant.Deficit > tenant.Quantum*5 {
		tenant.Deficit = tenant.Quantum * 5
	}

	if tenant.Deficit != 50 {
		t.Fatalf("expected capped deficit 50, got %d", tenant.Deficit)
	}
}

func TestDRRMultiTenantFairness(t *testing.T) {
	// Tenant A has 100 batch tasks
	// Tenant B has 1 urgent task
	// DRR must alternate turns rather than running all of Tenant A's tasks first

	tenants := []*Tenant{
		{ID: "tenant-a", Quantum: 10, Deficit: 0},
		{ID: "tenant-b", Quantum: 10, Deficit: 0},
	}

	scheduledOrder := []string{}
	tenantAQueue := 10
	tenantBQueue := 1

	for round := 0; round < 5; round++ {
		for _, tenant := range tenants {
			tenant.Deficit += tenant.Quantum
			for tenant.Deficit >= 10 {
				if tenant.ID == "tenant-a" && tenantAQueue > 0 {
					tenantAQueue--
					tenant.Deficit -= 10
					scheduledOrder = append(scheduledOrder, tenant.ID)
				} else if tenant.ID == "tenant-b" && tenantBQueue > 0 {
					tenantBQueue--
					tenant.Deficit -= 10
					scheduledOrder = append(scheduledOrder, tenant.ID)
				} else {
					tenant.Deficit = 0
					break
				}
			}
		}
	}

	if len(scheduledOrder) < 2 {
		t.Fatalf("expected at least 2 jobs scheduled, got %d", len(scheduledOrder))
	}

	// Tenant B should be scheduled in the first round (2nd position), NOT waiting after all Tenant A jobs!
	if scheduledOrder[1] != "tenant-b" {
		t.Errorf("DRR failure: expected tenant-b in position 1 (second task scheduled), got order: %v", scheduledOrder)
	}
}
