package provider

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccAirflowDag_basic(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_DAG_TESTS") == "true" {
		t.Skip("Skipping Airflow DAG tests")
	}

	resourceName := "airflow_dag.test"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowDagCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowDagConfigBasic(true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "dag_id", "tutorial"),
					resource.TestCheckResourceAttr(resourceName, "is_paused", "true"),
					resource.TestCheckResourceAttrSet(resourceName, "is_active"),
					resource.TestCheckResourceAttrSet(resourceName, "is_subdag"),
					resource.TestCheckResourceAttrSet(resourceName, "description"),
					resource.TestCheckResourceAttrSet(resourceName, "file_token"),
					resource.TestCheckResourceAttrSet(resourceName, "fileloc"),
					resource.TestCheckResourceAttr(resourceName, "root_dag_id", ""),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"delete_dag"},
			},
			{
				Config: testAccAirflowDagConfigBasic(false),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "dag_id", "tutorial"),
					resource.TestCheckResourceAttr(resourceName, "is_paused", "false"),
				),
			},
			{
				Config: testAccAirflowDagConfigBasic(true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "dag_id", "tutorial"),
					resource.TestCheckResourceAttr(resourceName, "is_paused", "true"),
				),
			},
		},
	})
}

func testAccCheckAirflowDagCheckDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(ProviderConfig)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_dag" {
			continue
		}

		dag, res, err := client.ApiClient.DAGApi.GetDag(client.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			deleteDag, _ := strconv.ParseBool(rs.Primary.Attributes["delete_dag"])

			if deleteDag {
				if *dag.DagId == rs.Primary.ID {
					return fmt.Errorf("Airflow Dag (%s) still exists.", rs.Primary.ID)
				}
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

func testAccAirflowDagConfigBasic(paused bool) string {
	return fmt.Sprintf(`
resource "airflow_dag" "test" {
  dag_id    = "tutorial"
  is_paused = %[1]t
}
`, paused)
}
