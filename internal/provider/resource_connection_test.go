package provider

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestSuppressSameJsonDiff(t *testing.T) {
	cases := []struct {
		name string
		old  string
		new  string
		want bool
	}{
		{
			name: "identical json",
			old:  `{"api_key":"myapikey"}`,
			new:  `{"api_key":"myapikey"}`,
			want: true,
		},
		{
			name: "same json different formatting",
			old:  `{"api_key":"myapikey","host":"h"}`,
			new:  `{ "host": "h", "api_key": "myapikey" }`,
			want: true,
		},
		{
			// API returns the masked placeholder for a secret-like key.
			// State holds the real value, so the diff must be suppressed.
			name: "masked value in new matches real value in old",
			old:  `{"api_key":"myapikey"}`,
			new:  `{"api_key":"***"}`,
			want: true,
		},
		{
			name: "masked value nested in object",
			old:  `{"conn":{"api_key":"myapikey","host":"h"}}`,
			new:  `{"conn":{"api_key":"***","host":"h"}}`,
			want: true,
		},
		{
			name: "masked value inside array",
			old:  `{"keys":["myapikey","other"]}`,
			new:  `{"keys":["***","other"]}`,
			want: true,
		},
		{
			// User changes the real secret: state and config both hold real
			// (differing) values, so the diff must NOT be suppressed.
			name: "real value change is not suppressed",
			old:  `{"api_key":"oldkey"}`,
			new:  `{"api_key":"newkey"}`,
			want: false,
		},
		{
			// A non-secret value change alongside an unrelated key must show.
			name: "non-masked change is not suppressed",
			old:  `{"api_key":"myapikey","host":"old"}`,
			new:  `{"api_key":"***","host":"new"}`,
			want: false,
		},
		{
			name: "empty vs empty object equivalent",
			old:  ``,
			new:  `{}`,
			want: true,
		},
		{
			name: "empty object vs null equivalent",
			old:  `{}`,
			new:  `null`,
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := suppressSameJsonDiff("extra", tc.old, tc.new, nil)
			if got != tc.want {
				t.Errorf("suppressSameJsonDiff(old=%q, new=%q) = %v, want %v", tc.old, tc.new, got, tc.want)
			}
		})
	}
}

func TestConnectionPasswordDiffSuppress(t *testing.T) {
	suppress := resourceConnection().Schema["password"].DiffSuppressFunc
	if suppress == nil {
		t.Fatal("expected password to have a DiffSuppressFunc")
	}

	cases := []struct {
		name string
		old  string
		new  string
		want bool
	}{
		{
			// API returns masked placeholder, state has a real password:
			// suppress so Terraform does not try to write "***" back.
			name: "masked new with existing old",
			old:  "realpass",
			new:  "***",
			want: true,
		},
		{
			// No prior value: nothing to preserve, so do not suppress.
			name: "masked new with empty old",
			old:  "",
			new:  "***",
			want: false,
		},
		{
			// User sets a real password: must not be suppressed.
			name: "real new value",
			old:  "***",
			new:  "newpass",
			want: false,
		},
		{
			name: "identical real values",
			old:  "samepass",
			new:  "samepass",
			want: false,
		},
		{
			name: "empty new value",
			old:  "realpass",
			new:  "",
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := suppress("password", tc.old, tc.new, nil)
			if got != tc.want {
				t.Errorf("password DiffSuppressFunc(old=%q, new=%q) = %v, want %v", tc.old, tc.new, got, tc.want)
			}
		})
	}
}

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
		client := testAccProvider.Meta().(ProviderConfig)
		conn, _, err := client.ApiClient.ConnectionApi.GetConnection(client.AuthContext, rs.Primary.ID).Execute()
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
