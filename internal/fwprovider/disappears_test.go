package fwprovider

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// disappearsDeleteFunc deletes the object behind a resource directly via the
// Airflow API, simulating out-of-band deletion.
type disappearsDeleteFunc func(cfg client.ProviderConfig, id string) (*http.Response, error)

// testAccCheckDisappears deletes the resource's Airflow object out-of-band. Used
// with ExpectNonEmptyPlan to assert the provider handles the deletion gracefully:
// the next refresh must drop it from state (404 -> RemoveResource, no error) so
// Terraform plans to recreate it, rather than erroring.
func testAccCheckDisappears(resourceName string, del disappearsDeleteFunc) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("resource not found in state: %s", resourceName)
		}
		cfg, err := testAccProviderConfig()
		if err != nil {
			return err
		}
		if _, err := del(cfg, rs.Primary.ID); err != nil {
			return fmt.Errorf("out-of-band delete of %s (%s) failed: %w", resourceName, rs.Primary.ID, err)
		}
		return nil
	}
}

func TestAccAirflowVariable_disappears(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{{
			Config: testAccAirflowVariableConfigBasic(rName, "value"),
			Check: testAccCheckDisappears("airflow_variable.test", func(cfg client.ProviderConfig, id string) (*http.Response, error) {
				return cfg.ApiClient.VariableApi.DeleteVariable(cfg.AuthContext, id).Execute()
			}),
			ExpectNonEmptyPlan: true,
		}},
	})
}

func TestAccAirflowConnection_disappears(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{{
			Config: testAccAirflowConnectionConfigBasic(rName),
			Check: testAccCheckDisappears("airflow_connection.test", func(cfg client.ProviderConfig, id string) (*http.Response, error) {
				return cfg.ApiClient.ConnectionApi.DeleteConnection(cfg.AuthContext, id).Execute()
			}),
			ExpectNonEmptyPlan: true,
		}},
	})
}

func TestAccAirflowPool_disappears(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{{
			Config: testAccAirflowPoolConfigBasic(rName, 1),
			Check: testAccCheckDisappears("airflow_pool.test", func(cfg client.ProviderConfig, id string) (*http.Response, error) {
				return cfg.ApiClient.PoolApi.DeletePool(cfg.AuthContext, id).Execute()
			}),
			ExpectNonEmptyPlan: true,
		}},
	})
}

func TestAccAirflowUser_disappears(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{{
			Config: testAccAirflowUserConfigBasic(rName, rName),
			Check: testAccCheckDisappears("airflow_user.test", func(cfg client.ProviderConfig, id string) (*http.Response, error) {
				return cfg.ApiClient.UserApi.DeleteUser(cfg.AuthContext, id).Execute()
			}),
			ExpectNonEmptyPlan: true,
		}},
	})
}

func TestAccAirflowRole_disappears(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{{
			Config: testAccAirflowRoleConfigBasic(rName, "can_read", "Audit Logs"),
			Check: testAccCheckDisappears("airflow_role.test", func(cfg client.ProviderConfig, id string) (*http.Response, error) {
				return cfg.ApiClient.RoleApi.DeleteRole(cfg.AuthContext, id).Execute()
			}),
			ExpectNonEmptyPlan: true,
		}},
	})
}
