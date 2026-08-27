package registry

import "testing"

func TestTypeHelpers(t *testing.T) {
	t.Parallel()
	if !TypeHTTP2GW.IsDataPlaneType() || !TypeLogic.IsDataPlaneType() {
		t.Fatal("data plane")
	}
	if TypeMaster.IsDataPlaneType() || TypeHTTP2Client.IsDataPlaneType() || TypePerformance.IsDataPlaneType() {
		t.Fatal("control types must not be data-plane")
	}
	if !TypeLogic.IsLogicBackend() || TypeHTTP2GW.IsLogicBackend() {
		t.Fatal("logic backend")
	}
	if TypeMaster.String() != "master" || TypePerformance.String() != "performance" {
		t.Fatalf("strings %s %s", TypeMaster, TypePerformance)
	}
	if NodeStateStopping.String() != "stopping" {
		t.Fatal(NodeStateStopping)
	}
}
