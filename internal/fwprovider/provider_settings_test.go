package fwprovider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// Invalid TLS and timeout settings fail before the provider connects.
func TestProvider_invalidSettings(t *testing.T) {
	cases := map[string]struct {
		settings string
		want     string
	}{
		"client key without certificate": {`client_key = "key"`, `client_certificate`},
		"ca_certificate is a path":       {`ca_certificate = "/etc/ssl/ca.pem"`, `Invalid PEM certificate`},
		"timeout format":                 {`timeout = "soon"`, `Invalid duration`},
		"timeout not positive":           {`timeout = "0s"`, `Invalid duration`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resource.UnitTest(t, resource.TestCase{
				ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
				Steps: []resource.TestStep{
					{
						Config: `
provider "airflow" {
  base_endpoint = "http://127.0.0.1:1"
  ` + tc.settings + `
}

data "airflow_variable" "v" {
  key = "x"
}
`,
						ExpectError: regexp.MustCompile(tc.want),
					},
				},
			})
		})
	}
}
