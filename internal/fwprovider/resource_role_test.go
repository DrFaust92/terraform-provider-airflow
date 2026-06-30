package fwprovider

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccAirflowRole_basic(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_role.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowRoleCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowRoleConfigBasic(rName, "can_read", "Audit Logs"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "action.#", "1"),
					resource.TestCheckTypeSetElemNestedAttrs(resourceName, "action.*", map[string]string{
						"action":   "can_read",
						"resource": "Audit Logs",
					}),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
			// { // Disabled due to issue with Airflow API that prevents updating role actions
			// 	Config: testAccAirflowRoleConfigBasic(rName, "can_edit", "Connections"),
			// 	Check: resource.ComposeTestCheckFunc(
			// 		resource.TestCheckResourceAttr(resourceName, "name", rName),
			// 		resource.TestCheckResourceAttr(resourceName, "action.#", "1"),
			// 		resource.TestCheckTypeSetElemNestedAttrs(resourceName, "action.*", map[string]string{
			// 			"action":   "can_edit",
			// 			"resource": "Connections",
			// 		}),
			// 	),
			// },
		},
	})
}

func testAccCheckAirflowRoleCheckDestroy(s *terraform.State) error {
	cfg, err := testAccProviderConfig()
	if err != nil {
		return err
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_role" {
			continue
		}

		role, res, err := cfg.ApiClient.RoleApi.GetRole(cfg.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if role.GetName() == rs.Primary.ID {
				return fmt.Errorf("Airflow Role (%s) still exists.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

func testAccAirflowRoleConfigBasic(rName, action, resource string) string {
	return fmt.Sprintf(`
resource "airflow_role" "test" {
  name = %[1]q

  action {
    action   = %[2]q
    resource = %[3]q
  }
}
`, rName, action, resource)
}
