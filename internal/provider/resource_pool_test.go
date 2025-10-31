package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccAirflowPool_basic(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_pool.test"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowPoolCheckDestroy,
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
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowPoolCheckDestroy,
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
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowPoolCheckDestroy,
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
	client := testAccProvider.Meta().(ProviderConfig)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_pool" {
			continue
		}

		variable, res, err := client.ApiClient.PoolApi.GetPool(client.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if *variable.Name == rs.Primary.ID {
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
