package fwprovider

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// NOTE: airflow_user is still served by the SDKv2 provider, but its acceptance
// test config also creates an airflow_role (now framework-served), so it must
// run through the muxed provider. The test lives here until airflow_user is
// itself migrated to the framework.

func TestAccAirflowUser_basic(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")
	rNameUpdated := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_user.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowUserCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowUserConfigBasic(rName, rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "email", rName),
					resource.TestCheckResourceAttr(resourceName, "first_name", rName),
					resource.TestCheckResourceAttr(resourceName, "last_name", rName),
					resource.TestCheckResourceAttr(resourceName, "username", rName),
					resource.TestCheckResourceAttr(resourceName, "password", rName),
					resource.TestCheckResourceAttr(resourceName, "active", "true"),
					resource.TestCheckResourceAttr(resourceName, "roles.#", "1"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "roles.*", "airflow_role.test", "name"),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"password"},
			},
			{
				Config: testAccAirflowUserConfigBasic(rName, rNameUpdated),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "email", rName),
					resource.TestCheckResourceAttr(resourceName, "first_name", rNameUpdated),
					resource.TestCheckResourceAttr(resourceName, "last_name", rName),
					resource.TestCheckResourceAttr(resourceName, "username", rName),
					resource.TestCheckResourceAttr(resourceName, "password", rName),
					resource.TestCheckResourceAttr(resourceName, "active", "true"),
					resource.TestCheckResourceAttr(resourceName, "roles.#", "1"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "roles.*", "airflow_role.test", "name"),
				),
			},
		},
	})
}

func testAccCheckAirflowUserCheckDestroy(s *terraform.State) error {
	cfg, err := testAccProviderConfig()
	if err != nil {
		return err
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_user" {
			continue
		}

		user, res, err := cfg.ApiClient.UserApi.GetUser(cfg.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if user.GetUsername() == rs.Primary.ID {
				return fmt.Errorf("Airflow User (%s) still exists.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

func testAccAirflowUserConfigBasic(rName, fName string) string {
	return fmt.Sprintf(`
resource "airflow_role" "test" {
  name = %[1]q

  action {
    action   = "can_read"
    resource = "Audit Logs"
  }
}

resource "airflow_user" "test" {
  email      = %[1]q
  first_name = %[2]q
  last_name  = %[1]q
  username   = %[1]q
  password   = %[1]q
  roles      = [airflow_role.test.name]
}
`, rName, fName)
}
