package fwprovider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
	"github.com/hashicorp/terraform-plugin-testing/tfversion"
)

// Import by identity (Terraform 1.12+) for the resources whose identity is the
// resource id.

func TestAccAirflowPool_identity(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	testAccIdentityImport(t, "airflow_pool.test", testAccAirflowPoolConfigBasic(rName, 2), testAccCheckAirflowPoolCheckDestroy)
}

func TestAccAirflowVariable_identity(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	testAccIdentityImport(t, "airflow_variable.test", testAccAirflowVariableConfigBasic(rName, rName), testAccCheckAirflowVariableCheckDestroy)
}

func TestAccAirflowConnection_identity(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	testAccIdentityImport(t, "airflow_connection.test", testAccAirflowConnectionConfigBasic(rName), testAccCheckAirflowConnectionCheckDestroy)
}

func testAccIdentityImport(t *testing.T, resourceName, config string, checkDestroy resource.TestCheckFunc) {
	t.Helper()
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             checkDestroy,
		TerraformVersionChecks: []tfversion.TerraformVersionCheck{
			tfversion.SkipBelow(tfversion.Version1_12_0),
		},
		Steps: []resource.TestStep{
			{
				Config: config,
				ConfigStateChecks: []statecheck.StateCheck{
					statecheck.ExpectIdentityValueMatchesState(resourceName, tfjsonpath.New("id")),
				},
			},
			{
				ResourceName:    resourceName,
				ImportState:     true,
				ImportStateKind: resource.ImportBlockWithResourceIdentity,
			},
		},
	})
}
