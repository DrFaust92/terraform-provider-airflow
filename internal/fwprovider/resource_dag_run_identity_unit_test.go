package fwprovider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/types"
)

func TestDagRunIdentity(t *testing.T) {
	got := dagRunIdentity(types.StringValue("example_bash_operator:manual__2026-01-01T00:00:00+00:00"))
	if got.DagID.ValueString() != "example_bash_operator" {
		t.Errorf("dag_id = %s, want example_bash_operator", got.DagID)
	}
	// Run IDs may contain ':'; only the first separator splits.
	if got.DagRunID.ValueString() != "manual__2026-01-01T00:00:00+00:00" {
		t.Errorf("dag_run_id = %s, want manual__2026-01-01T00:00:00+00:00", got.DagRunID)
	}
}
