package client

import (
	"testing"

	"github.com/apache/airflow-client-go/airflow"
)

func TestNewProviderConfig_authManagerBasePath(t *testing.T) {
	t.Run("unset reuses the core client", func(t *testing.T) {
		cfg, err := NewProviderConfig("https://airflow.example.com", "token", "", "", false, "/api/v1", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthManagerApiClient != cfg.ApiClient {
			t.Fatal("expected AuthManagerApiClient to be the core ApiClient when auth_manager_base_path is unset")
		}
	})

	t.Run("unset defaults to FAB on api v2", func(t *testing.T) {
		cfg, err := NewProviderConfig("https://airflow.example.com", "token", "", "", false, "/api/v2", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := cfg.AuthManagerApiClient.GetConfig().Servers[0].URL, DefaultAuthManagerBasePathV3; got != want {
			t.Errorf("auth manager server URL = %q, want %q", got, want)
		}
	})

	t.Run("set uses its own server URL", func(t *testing.T) {
		cfg, err := NewProviderConfig("https://airflow.example.com/prefix/", "token", "", "", false, "/api/v2", "", "/auth/fab/v1")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := cfg.ApiClient.GetConfig().Servers[0].URL, "/prefix/api/v2"; got != want {
			t.Errorf("core server URL = %q, want %q", got, want)
		}
		if got, want := cfg.AuthManagerApiClient.GetConfig().Servers[0].URL, "/prefix/auth/fab/v1"; got != want {
			t.Errorf("auth manager server URL = %q, want %q", got, want)
		}
	})
}

func TestNewProviderConfig_authContexts(t *testing.T) {
	cfg, err := NewProviderConfig("https://airflow.example.com", "token", "admin", "secret", false, "/api/v2", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.AuthContext.Value(airflow.ContextBasicAuth).(airflow.BasicAuth); ok {
		t.Error("core API context must not carry basic auth when a token is set")
	}
	if got, _ := cfg.AuthContext.Value(airflow.ContextAccessToken).(string); got != "token" {
		t.Errorf("core API token = %q, want token", got)
	}
	if _, ok := cfg.AuthManagerAuthContext.Value(airflow.ContextAccessToken).(string); ok {
		t.Error("auth manager context must not carry the token when basic auth is set")
	}
	if got, _ := cfg.AuthManagerAuthContext.Value(airflow.ContextBasicAuth).(airflow.BasicAuth); got.UserName != "admin" {
		t.Errorf("auth manager basic user = %q, want admin", got.UserName)
	}
}
