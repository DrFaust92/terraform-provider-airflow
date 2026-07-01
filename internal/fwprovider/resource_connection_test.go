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

func TestAccAirflowConnection_basic(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_connection.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowConnectionCheckDestroy,
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
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowConnectionConfigPasswordWO(rName, "Mustbe8characters", 1),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "password_wo_version", "1"),
					testAccCheckAirflowConnectionPasswordSet(resourceName),
				),
			},
			{
				Config: testAccAirflowConnectionConfigPasswordWO(rName, "Mustbe8charactersupdated", 2),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckResourceAttr(resourceName, "password_wo_version", "2"),
					testAccCheckAirflowConnectionPasswordSet(resourceName),
				),
			},
		},
	})
}

func testAccCheckAirflowConnectionPasswordSet(resourceName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("resource not found: %s", resourceName)
		}
		cfg, err := testAccProviderConfig()
		if err != nil {
			return err
		}
		conn, _, err := cfg.ApiClient.ConnectionApi.GetConnection(cfg.AuthContext, rs.Primary.ID).Execute()
		if err != nil {
			return fmt.Errorf("failed to get connection %s: %s", rs.Primary.ID, err)
		}
		if pw := conn.GetPassword(); pw == "" {
			// Airflow v1 API (/api/v1) does not return the password field in GET
			// responses, so we can't verify it there. Skip for v1, enforce for v2+.
			if os.Getenv("AIRFLOW_API_BASE_PATH") == "" {
				return nil
			}
			return fmt.Errorf("expected password to be set on connection %s, got empty string", rs.Primary.ID)
		}
		return nil
	}
}

func TestAccAirflowConnection_full(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	rNameUpdated := acctest.RandomWithPrefix("tf-acc-test")

	resourceName := "airflow_connection.test"
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowConnectionCheckDestroy,
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
				// Removing the optional fields clears them. Under the framework
				// these become null (absent) rather than the SDKv2 zero values,
				// so assert their absence.
				Config: testAccAirflowConnectionConfigBasic(rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
					resource.TestCheckNoResourceAttr(resourceName, "extra"),
					resource.TestCheckNoResourceAttr(resourceName, "description"),
					resource.TestCheckNoResourceAttr(resourceName, "port"),
					resource.TestCheckNoResourceAttr(resourceName, "schema"),
					resource.TestCheckNoResourceAttr(resourceName, "login"),
					resource.TestCheckNoResourceAttr(resourceName, "host"),
				),
			},
		},
	})
}

func testAccCheckAirflowConnectionCheckDestroy(s *terraform.State) error {
	cfg, err := testAccProviderConfig()
	if err != nil {
		return err
	}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_connection" {
			continue
		}

		conn, res, err := cfg.ApiClient.ConnectionApi.GetConnection(cfg.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if conn.GetConnectionId() == rs.Primary.ID {
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
  connection_id       = %[1]q
  conn_type           = "http"
  password_wo         = %[2]q
  password_wo_version = %[3]d
}
`, rName, password, passwordVersion)
}

// TestAccAirflowConnection_upgradeFromSDKv2 reproduces the regression where a
// connection created by the SDKv2 provider (which stored an unset `extra` as "")
// failed under the framework provider with "Invalid JSON String Value". Step 1
// creates the connection with the last SDKv2 release; step 2 applies it with the
// current (framework) provider and must succeed and converge to a stable plan.
func TestAccAirflowConnection_upgradeFromSDKv2(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "airflow_connection.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		CheckDestroy: testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				ExternalProviders: map[string]resource.ExternalProvider{
					"airflow": {VersionConstraint: "1.0.2", Source: "DrFaust92/airflow"},
				},
				Config: testAccAirflowConnectionConfigBasic(rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
				),
			},
			{
				// Plan + apply with the current (framework) provider. Previously
				// this errored with "Invalid JSON String Value" while reading the
				// SDKv2-written extra="". It now applies (normalizing the SDKv2
				// ""/0 representations of unset optionals to null) and the
				// built-in post-apply plan check confirms it converges.
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				Config:                   testAccAirflowConnectionConfigBasic(rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "conn_type", "http"),
				),
			},
		},
	})
}

func testAccAirflowConnectionConfigExtra(rName, extra string) string {
	return fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id = %[1]q
  conn_type     = "http"
  extra         = %[2]q
}
`, rName, extra)
}

// TestAccAirflowConnection_extraEquivalentJSON verifies that the configured
// `extra` JSON is preserved verbatim and that a semantically-equivalent but
// reformatted value (whitespace + key order) produces no diff.
func TestAccAirflowConnection_extraEquivalentJSON(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "airflow_connection.test"

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowConnectionConfigExtra(rName, `{"a":"b","c":"d"}`),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					resource.TestCheckResourceAttr(resourceName, "extra", `{"a":"b","c":"d"}`),
				),
			},
			{
				// Same JSON, reformatted (whitespace + key order): must be a no-op.
				Config:   testAccAirflowConnectionConfigExtra(rName, "{\n  \"c\": \"d\",\n  \"a\": \"b\"\n}"),
				PlanOnly: true,
			},
		},
	})
}

// TestAccAirflowConnection_extraMaskedSecret verifies idempotency when the
// `extra` JSON contains a secret-like key (e.g. "api_key") that Airflow's
// SecretsMasker returns masked as "***" on read. The real configured value must
// be preserved in state so a subsequent plan is empty. See GH issue #34.
func TestAccAirflowConnection_extraMaskedSecret(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "airflow_connection.test"

	extra := `{"api_key":"mysecretkey"}`

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAirflowConnectionCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowConnectionConfigExtra(rName, extra),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "connection_id", rName),
					// The real secret value must be kept, not the masked "***".
					resource.TestCheckResourceAttr(resourceName, "extra", extra),
				),
			},
			{
				// Re-plan with the same config: the API returns the masked value,
				// but the provider must not surface a diff.
				Config:   testAccAirflowConnectionConfigExtra(rName, extra),
				PlanOnly: true,
			},
		},
	})
}

// TestAccAirflowConnection_validation covers the framework validators on the
// connection resource (config-time, no API calls): password/password_wo
// conflict, the password_wo/password_wo_version co-requirement, and the port
// range bound.
func TestAccAirflowConnection_validation(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-acc-test")

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id       = %[1]q
  conn_type           = "http"
  password            = "p"
  password_wo         = "w"
  password_wo_version = "1"
}
`, rName),
				ExpectError: regexp.MustCompile(`Invalid Attribute Combination`),
			},
			{
				Config: fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id = %[1]q
  conn_type     = "http"
  password_wo   = "w"
}
`, rName),
				ExpectError: regexp.MustCompile(`Invalid Attribute Combination`),
			},
			{
				Config: fmt.Sprintf(`
resource "airflow_connection" "test" {
  connection_id = %[1]q
  conn_type     = "http"
  port          = 70000
}
`, rName),
				ExpectError: regexp.MustCompile(`Invalid Attribute Value`),
			},
		},
	})
}
