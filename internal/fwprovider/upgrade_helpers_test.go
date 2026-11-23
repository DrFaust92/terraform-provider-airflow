package fwprovider

import (
	"os"
	"strings"
	"testing"
)

// testAccPreCheckSDKv2Upgrade adapts the environment for upgrade tests that
// run the last SDKv2 release (1.0.2) of the provider.
func testAccPreCheckSDKv2Upgrade(t *testing.T, usesAuthManager bool) {
	t.Helper()
	if usesAuthManager && strings.TrimSuffix(os.Getenv("AIRFLOW_API_BASE_PATH"), "/") == "/api/v2" {
		t.Skip("the SDKv2 provider cannot reach the Airflow 3 auth manager API")
	}
	// The SDKv2 provider rejects oauth2_token together with username/password;
	// with a token set, the core API resources only need the token.
	if os.Getenv("AIRFLOW_OAUTH2_TOKEN") != "" {
		t.Setenv("AIRFLOW_API_USERNAME", "")
		t.Setenv("AIRFLOW_API_PASSWORD", "")
	}
}
