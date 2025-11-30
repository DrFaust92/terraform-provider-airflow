package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccAirflowConnection_basic(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_connection.test"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowConnectionConfigBasic(rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
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

func TestAccAirflowConnection_passwordWO(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_connection.test"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowConnectionConfigPasswordWO(rName, "Mustbe8characters", 1),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "password_wo_version", "1"),
				),
			},
			{
				Config: testAccAirflowConnectionConfigPasswordWO(rName, "Mustbe8charactersupdated", 2),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "password_wo_version", "2"),
				),
			},
		},
	})
}

func TestAccAirflowConnection_full(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	rNameUpdated := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_connection.test"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowConnectionConfigFull(rName, rName, "test", 443),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "host", rName),
					resource.TestCheckResourceAttr(resourceName, "description", rName),
					resource.TestCheckResourceAttr(resourceName, "login", rName),
					resource.TestCheckResourceAttr(resourceName, "schema", rName),
					resource.TestCheckResourceAttr(resourceName, "port", "443"),
					resource.TestCheckResourceAttr(resourceName, "extra", fmt.Sprintf("{\"%s\":\"%s\"}", "test", "test")),
					resource.TestCheckResourceAttr(resourceName, "password", rName),
				),
			},
			{
				ResourceName:            resourceName,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"password", "extra"},
			},
			{
				Config: testAccAirflowConnectionConfigFull(rName, rNameUpdated, "test2", 80),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "host", rNameUpdated),
					resource.TestCheckResourceAttr(resourceName, "login", rNameUpdated),
					resource.TestCheckResourceAttr(resourceName, "schema", rNameUpdated),
					resource.TestCheckResourceAttr(resourceName, "port", "80"),
					resource.TestCheckResourceAttr(resourceName, "extra", fmt.Sprintf("{\"%s\":\"%s\"}", "test2", "test2")),
					resource.TestCheckResourceAttr(resourceName, "password", rNameUpdated),
					resource.TestCheckResourceAttr(resourceName, "description", rNameUpdated),
				),
			},
			{
				Config: testAccAirflowConnectionConfigBasic(rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "extra", ""),
					resource.TestCheckResourceAttr(resourceName, "description", ""),
					resource.TestCheckResourceAttr(resourceName, "port", "0"),
					resource.TestCheckResourceAttr(resourceName, "schema", ""),
					resource.TestCheckResourceAttr(resourceName, "login", ""),
					resource.TestCheckResourceAttr(resourceName, "host", ""),
				),
			},
		},
	})
}

func testAccCheckAirflowConnectionCheckDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(ProviderConfig)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_connection" {
			continue
		}

		conn, res, err := client.ApiClient.ConnectionApi.GetConnection(client.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if *conn.ConnectionId == rs.Primary.ID {
				return fmt.Errorf("Airflow Connection (%s) still exists.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}

	return nil
}

func testAccAirflowConnectionConfigBasic(rName string) string {
	return fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id = %[1]q
  conn_type     = "http"
}
`, rName)
}

func testAccAirflowConnectionConfigFull(rName, rName2, extra string, port int) string {
	return fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id = %[1]q
  conn_type     = "http"
  host          = %[2]q
  description   = %[2]q  
  login         = %[2]q
  schema        = %[2]q
  port          = %[4]d
  password      = %[2]q
  extra         = jsonencode({ %[3]q = %[3]q })
}
`, rName, rName2, extra, port)
}

func testAccAirflowConnectionConfigPasswordWO(rName, password string, passwordVersion int) string {
	return fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id 	  = %[1]q
  conn_type     	  = "http"
  password_wo   	  = %[2]q
  password_wo_version = %[3]d
}
`, rName, password, passwordVersion)
}
