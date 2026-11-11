package client

import "testing"

func TestNewProviderConfig_authManagerBasePath(t *testing.T) {
	t.Run("unset reuses the core client", func(t *testing.T) {
		cfg, err := NewProviderConfig("https://airflow.example.com", "token", "", "", false, "/api/v2", "", "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.AuthManagerApiClient != cfg.ApiClient {
			t.Fatal("expected AuthManagerApiClient to be the core ApiClient when auth_manager_base_path is unset")
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
