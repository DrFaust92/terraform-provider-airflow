package fwprovider

import (
	"os"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// TestAccAirflow_authError verifies that an authentication failure surfaces as a
// clear error rather than a confusing one. It overrides the provider's token
// with an invalid value, so the first API call is rejected. Gated to token
// (OAuth2) auth -- i.e. Airflow 3 -- to avoid clashing with basic-auth env.
func TestAccAirflow_authError(t *testing.T) {
	if os.Getenv("AIRFLOW_OAUTH2_TOKEN") == "" {
		t.Skip("auth-error test runs only with OAuth2 token auth (Airflow 3)")
	}
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: `
provider "airflow" {
  oauth2_token = "invalid-token-for-auth-error-test"
}

resource "airflow_variable" "auth_err" {
  key   = "tf-acc-auth-error"
  value = "x"
}
`,
				ExpectError: regexp.MustCompile(`(?i)(401|unauthor|not authenticated)`),
			},
		},
	})
}
