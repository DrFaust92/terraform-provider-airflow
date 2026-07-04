package fwprovider

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// TestAccAirflowPool_errorSurfacesAPIMessage verifies that an Airflow API
// failure surfaces as a clear, attributed error. "default_pool" always exists,
// so creating it conflicts, exercising the client-error parsing path.
func TestAccAirflowPool_errorSurfacesAPIMessage(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: `resource "airflow_pool" "err" {
  name  = "default_pool"
  slots = 1
}`,
				ExpectError: regexp.MustCompile(`(?i)failed to create.*default_pool`),
			},
		},
	})
}

func TestAccAirflowPool_basic(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_pool.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowPoolCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowPoolConfigBasic(rName, 2),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "open_slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "occupied_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "queued_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "running_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "deferred_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "scheduled_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "include_deferred", "false"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: testAccAirflowPoolConfigBasic(rName, 3),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "3"),
					resource.TestCheckResourceAttr(resourceName, "open_slots", "3"),
					resource.TestCheckResourceAttr(resourceName, "occupied_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "queued_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "running_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "deferred_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "scheduled_slots", "0"),
					resource.TestCheckResourceAttr(resourceName, "include_deferred", "false"),
				),
			},
		},
	})
}

func TestAccAirflowPool_description(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_pool.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowPoolCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowPoolConfigDescription(rName, 2, "Test description"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "description", "Test description"),
				),
			},
			{
				Config: testAccAirflowPoolConfigDescription(rName, 2, "Updated description"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "description", "Updated description"),
				),
			},
		},
	})
}

func TestAccAirflowPool_include_deferred(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_pool.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowPoolCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowPoolConfigIncludeDeferred(rName, 2, true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "include_deferred", "true"),
				),
			},
			{
				Config: testAccAirflowPoolConfigIncludeDeferred(rName, 2, false),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "include_deferred", "false"),
				),
			},
			{
				Config: testAccAirflowPoolConfigIncludeDeferred(rName, 2, true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
					resource.TestCheckResourceAttr(resourceName, "include_deferred", "true"),
				),
			},
		},
	})
}

func testAccCheckAirflowPoolCheckDestroy(s *terraform.State) error {
	cfg, err := testAccProviderConfig()
	if err != nil {
		return err
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_pool" {
			continue
		}

		pool, res, err := cfg.ApiClient.PoolApi.GetPool(cfg.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if pool.GetName() == rs.Primary.ID {
				return fmt.Errorf("Airflow Pool (%s) still exists.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

func testAccAirflowPoolConfigBasic(rName string, slots int) string {
	return fmt.Sprintf(`
resource "airflow_pool" "test" {
  name   = %[1]q
  slots  = %[2]d
}
`, rName, slots)
}

func testAccAirflowPoolConfigDescription(rName string, slots int, description string) string {
	return fmt.Sprintf(`
resource "airflow_pool" "test" {
  name        = %[1]q
  slots       = %[2]d
  description = %[3]q
}
`, rName, slots, description)
}

func testAccAirflowPoolConfigIncludeDeferred(rName string, slots int, includeDeferred bool) string {
	return fmt.Sprintf(`
resource "airflow_pool" "test" {
  name             = %[1]q
  slots            = %[2]d
  include_deferred = %[3]t
}
`, rName, slots, includeDeferred)
}

// TestAccAirflowPool_upgradeFromSDKv2 ensures a pool created by the SDKv2
// provider plans/applies cleanly under the current framework provider.
func TestAccAirflowPool_upgradeFromSDKv2(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "airflow_pool.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckAirflowPoolCheckDestroy,
		Steps: []resource.TestStep{
			{
				ExternalProviders: map[string]resource.ExternalProvider{
					"airflow": {VersionConstraint: "1.0.2", Source: "DrFaust92/airflow"},
				},
				Config: testAccAirflowPoolConfigBasic(rName, 2),
				Check:  resource.TestCheckResourceAttr(resourceName, "name", rName),
			},
			{
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				Config:                   testAccAirflowPoolConfigBasic(rName, 2),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "name", rName),
					resource.TestCheckResourceAttr(resourceName, "slots", "2"),
				),
			},
		},
	})
}
