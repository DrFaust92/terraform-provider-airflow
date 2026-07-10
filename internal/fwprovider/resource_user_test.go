package fwprovider

import (
	"fmt"
	"os"
	"regexp"
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

// TestAccAirflowUser_passwordWO creates a user with the write-only password_wo,
// asserts the secret is never persisted to state, and that bumping
// password_wo_version rotates it (triggers an update).
func TestAccAirflowUser_passwordWO(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "airflow_user.test"

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowUserCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowUserConfigPasswordWO(rName, "Mustbe8characters", 1),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "username", rName),
					resource.TestCheckResourceAttr(resourceName, "password_wo_version", "1"),
					// write-only value must never be persisted to state
					resource.TestCheckNoResourceAttr(resourceName, "password_wo"),
				),
			},
			{
				Config: testAccAirflowUserConfigPasswordWO(rName, "Mustbe8charactersupdated", 2),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "password_wo_version", "2"),
					resource.TestCheckNoResourceAttr(resourceName, "password_wo"),
				),
			},
		},
	})
}

// TestAccAirflowUser_validation covers the config-time validators: exactly one
// of password / password_wo must be set.
func TestAccAirflowUser_validation(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`
resource "airflow_user" "test" {
  email               = %[1]q
  first_name          = %[1]q
  last_name           = %[1]q
  username            = %[1]q
  password            = "p"
  password_wo         = "w"
  password_wo_version = "1"
  roles               = ["Admin"]
}
`, rName),
				ExpectError: regexp.MustCompile(`Invalid Attribute Combination`),
			},
			{
				Config: fmt.Sprintf(`
resource "airflow_user" "test" {
  email      = %[1]q
  first_name = %[1]q
  last_name  = %[1]q
  username   = %[1]q
  roles      = ["Admin"]
}
`, rName),
				ExpectError: regexp.MustCompile(`Exactly one of these attributes must be configured`),
			},
		},
	})
}

func testAccAirflowUserConfigPasswordWO(rName, password string, version int) string {
	return fmt.Sprintf(`
resource "airflow_role" "test" {
  name = %[1]q

  action {
    action   = "can_read"
    resource = "Audit Logs"
  }
}

resource "airflow_user" "test" {
  email               = %[1]q
  first_name          = %[1]q
  last_name           = %[1]q
  username            = %[1]q
  password_wo         = %[2]q
  password_wo_version = %[3]d
  roles               = [airflow_role.test.name]
}
`, rName, password, version)
}

// TestAccAirflowUser_upgradeFromSDKv2 guards the SDKv2 -> framework upgrade path
// for airflow_user (and the airflow_role it references).
func TestAccAirflowUser_upgradeFromSDKv2(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_USER_ROLES_TESTS") == "true" {
		t.Skip("Skipping Airflow Roles and User Tests")
	}
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "airflow_user.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckAirflowUserCheckDestroy,
		Steps: []resource.TestStep{
			{
				ExternalProviders: map[string]resource.ExternalProvider{
					"airflow": {VersionConstraint: "1.0.2", Source: "DrFaust92/airflow"},
				},
				Config: testAccAirflowUserConfigBasic(rName, rName),
				Check:  resource.TestCheckResourceAttr(resourceName, "username", rName),
			},
			{
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				Config:                   testAccAirflowUserConfigBasic(rName, rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "username", rName),
					resource.TestCheckResourceAttr(resourceName, "roles.#", "1"),
				),
			},
		},
	})
}
