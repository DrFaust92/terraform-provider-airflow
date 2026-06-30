package fwprovider

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

var dagId = "example_bash_operator"

func TestAccAirflowDagRun_basic(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_DAG_TESTS") == "true" {
		t.Skip("Skipping Airflow DAG tests")
	}

	resourceName := "airflow_dag_run.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowDagRunCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowDagRunConfigBasic(dagId),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "dag_id", dagId),
					resource.TestCheckResourceAttrSet(resourceName, "dag_run_id"),
					resource.TestCheckResourceAttr(resourceName, "conf.%", "0"),
					resource.TestCheckResourceAttrSet(resourceName, "state"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAccAirflowDagRun_dagRunId(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_DAG_TESTS") == "true" {
		t.Skip("Skipping Airflow DAG tests")
	}

	dagRunId := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_dag_run.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowDagRunCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowDagRunConfigRunId(dagId, dagRunId),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "dag_id", dagId),
					resource.TestCheckResourceAttr(resourceName, "dag_run_id", dagRunId),
					resource.TestCheckResourceAttr(resourceName, "conf.%", "0"),
					resource.TestCheckResourceAttrSet(resourceName, "state"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAccAirflowDagRun_conf(t *testing.T) {
	if os.Getenv("SKIP_AIRFLOW_DAG_TESTS") == "true" {
		t.Skip("Skipping Airflow DAG tests")
	}
	resourceName := "airflow_dag_run.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowDagRunCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowDagRunConfigConf(dagId),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "dag_id", dagId),
					resource.TestCheckResourceAttrSet(resourceName, "dag_run_id"),
					resource.TestCheckResourceAttr(resourceName, "conf.%", "1"),
					resource.TestCheckResourceAttr(resourceName, fmt.Sprintf("conf.%s", dagId), dagId),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testAccCheckAirflowDagRunCheckDestroy(s *terraform.State) error {
	cfg, err := testAccProviderConfig()
	if err != nil {
		return err
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_dag_run" {
			continue
		}

		dagID, dagRunID, err := parseDagRunID(rs.Primary.ID)
		if err != nil {
			return err
		}

		dagRun, res, err := cfg.ApiClient.DAGRunApi.GetDagRun(cfg.AuthContext, dagID, dagRunID).Execute()
		if err == nil {
			if dagRun.GetDagRunId() == dagRunID {
				return fmt.Errorf("Airflow DagRun (%s) still exists.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

func testAccAirflowDagRunConfigBasic(dagId string) string {
	return fmt.Sprintf(`
resource "airflow_dag" "test" {
  dag_id    = %[1]q
  is_paused = false
}

resource "airflow_dag_run" "test" {
  dag_id = airflow_dag.test.dag_id
}
`, dagId)
}

func testAccAirflowDagRunConfigRunId(dagId, dagRunId string) string {
	return fmt.Sprintf(`
resource "airflow_dag" "test" {
  dag_id    = %[1]q
  is_paused = false
}

resource "airflow_dag_run" "test" {
  dag_id     = airflow_dag.test.dag_id
  dag_run_id = %[2]q
}
`, dagId, dagRunId)
}

func testAccAirflowDagRunConfigConf(dagId string) string {
	return fmt.Sprintf(`
resource "airflow_dag" "test" {
  dag_id    = %[1]q
  is_paused = false
}

resource "airflow_dag_run" "test" {
  dag_id = airflow_dag.test.dag_id

  conf = {
    %[1]q = %[1]q
  }
}
`, dagId)
}
