package fwprovider

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/querycheck"
	"github.com/hashicorp/terraform-plugin-testing/tfversion"
)

func TestAccAirflowRole_list(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resource.Test(t, resource.TestCase{
		TerraformVersionChecks: []tfversion.TerraformVersionCheck{
			tfversion.SkipBelow(tfversion.Version1_14_0),
		},
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowRoleCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowRoleConfigBasic(rName, "can_read", "Audit Logs"),
			},
			{
				Query: true,
				Config: `
provider "airflow" {}

list "airflow_role" "test" {
  provider = airflow
}
`,
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectIdentity("airflow_role.test", map[string]knownvalue.Check{
						"name": knownvalue.StringExact(rName),
					}),
				},
			},
		},
	})
}

func TestAccAirflowUser_list(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resource.Test(t, resource.TestCase{
		TerraformVersionChecks: []tfversion.TerraformVersionCheck{
			tfversion.SkipBelow(tfversion.Version1_14_0),
		},
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowUserCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowUserConfigBasic(rName, rName),
			},
			{
				Query: true,
				Config: `
provider "airflow" {}

list "airflow_user" "test" {
  provider = airflow
}
`,
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectIdentity("airflow_user.test", map[string]knownvalue.Check{
						"username": knownvalue.StringExact(rName),
					}),
				},
			},
		},
	})
}

func TestAccAirflowDag_list(t *testing.T) {
	// Separate from SKIP_AIRFLOW_DAG_TESTS: needs only a parsed "tutorial"
	// example DAG, which the Airflow 3.2 CI job provides.
	if os.Getenv("SKIP_AIRFLOW_DAG_LIST_TESTS") == "true" {
		t.Skip("Skipping Airflow DAG list tests (SKIP_AIRFLOW_DAG_LIST_TESTS)")
	}

	resource.Test(t, resource.TestCase{
		TerraformVersionChecks: []tfversion.TerraformVersionCheck{
			tfversion.SkipBelow(tfversion.Version1_14_0),
		},
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowDagCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowDagConfigBasic(true),
			},
			{
				Query: true,
				Config: `
provider "airflow" {}

list "airflow_dag" "test" {
  provider = airflow
}
`,
				// The example "tutorial" DAG, also used by the airflow_dag tests.
				QueryResultChecks: []querycheck.QueryResultCheck{
					querycheck.ExpectIdentity("airflow_dag.test", map[string]knownvalue.Check{
						"dag_id": knownvalue.StringExact("tutorial"),
					}),
				},
			},
		},
	})
}
