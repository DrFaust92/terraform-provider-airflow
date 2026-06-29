package fwprovider

import (
	"cmp"
	"fmt"
	"os"
	"testing"

	"github.com/drfaust92/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccAirflowVariable_basic(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	rNameUpdated := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_variable.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowVariableCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowVariableConfigBasic(rName, rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "key", rName),
					resource.TestCheckResourceAttr(resourceName, "value", rName),
					resource.TestCheckResourceAttr(resourceName, "description", ""),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: testAccAirflowVariableConfigBasic(rName, rNameUpdated),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "key", rName),
					resource.TestCheckResourceAttr(resourceName, "value", rNameUpdated),
				),
			},
		},
	})
}

func TestAccAirflowVariable_desc(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	rNameUpdated := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_variable.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowVariableCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowVariableConfigDesc(rName, rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "key", rName),
					resource.TestCheckResourceAttr(resourceName, "value", rName),
					resource.TestCheckResourceAttr(resourceName, "description", rName),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: testAccAirflowVariableConfigDesc(rName, rNameUpdated),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "key", rName),
					resource.TestCheckResourceAttr(resourceName, "value", rNameUpdated),
					resource.TestCheckResourceAttr(resourceName, "description", rNameUpdated),
				),
			},
			{
				Config: testAccAirflowVariableConfigBasic(rName, rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "key", rName),
					resource.TestCheckResourceAttr(resourceName, "value", rName),
					resource.TestCheckResourceAttr(resourceName, "description", ""),
				),
			},
		},
	})
}

func testAccCheckAirflowVariableCheckDestroy(s *terraform.State) error {
	cfg, err := testAccProviderConfig()
	if err != nil {
		return err
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_variable" {
			continue
		}

		variable, res, err := cfg.ApiClient.VariableApi.GetVariable(cfg.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if variable.GetKey() == rs.Primary.ID {
				return fmt.Errorf("Airflow Variable (%s) still exists.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

// testAccProviderConfig builds an Airflow client from the acceptance-test
// environment, for use in CheckDestroy outside the muxed provider lifecycle.
func testAccProviderConfig() (provider.ProviderConfig, error) {
	return provider.NewProviderConfig(
		os.Getenv("AIRFLOW_BASE_ENDPOINT"),
		os.Getenv("AIRFLOW_OAUTH2_TOKEN"),
		os.Getenv("AIRFLOW_API_USERNAME"),
		os.Getenv("AIRFLOW_API_PASSWORD"),
		false,
		cmp.Or(os.Getenv("AIRFLOW_API_BASE_PATH"), defaultBasePath),
		os.Getenv("AIRFLOW_SESSION_COOKIE"),
	)
}

func testAccAirflowVariableConfigBasic(rName, value string) string {
	return fmt.Sprintf(`
resource "airflow_variable" "test" {
  key    = %[1]q
  value  = %[2]q
}
`, rName, value)
}

func testAccAirflowVariableConfigDesc(rName, value string) string {
	return fmt.Sprintf(`
resource "airflow_variable" "test" {
  key          = %[1]q
  value        = %[2]q
  description  = %[2]q
}
`, rName, value)
}
