package fwprovider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/types"
)

func TestWithoutImplicitRoleActions(t *testing.T) {
	audit := roleActionModel{Action: types.StringValue("can_read"), Resource: types.StringValue("Audit Logs")}
	read := []roleActionModel{audit, implicitRoleAction}

	if got := withoutImplicitRoleActions(read, []roleActionModel{audit}); len(got) != 1 || got[0] != audit {
		t.Errorf("not tracked in prior state: got %v, want only %v", got, audit)
	}
	if got := withoutImplicitRoleActions(read, []roleActionModel{audit, implicitRoleAction}); len(got) != 2 {
		t.Errorf("tracked in prior state: got %v, want both actions kept", got)
	}
	if got := withoutImplicitRoleActions([]roleActionModel{implicitRoleAction}, nil); got != nil {
		t.Errorf("only implicit action: got %v, want nil", got)
	}
}
