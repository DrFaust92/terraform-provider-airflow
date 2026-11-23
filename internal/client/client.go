// Package client builds the Apache Airflow API client used by the provider's
// resources. It has no dependency on any Terraform plugin SDK so it can be
// shared freely across packages.
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/airflow-client-go/airflow"
)

// DefaultAuthManagerBasePathV3 is the FAB auth manager API path used for users
// and roles on Airflow 3 when no auth manager base path is configured.
const DefaultAuthManagerBasePathV3 = "/auth/fab/v1"

// ProviderConfig is the configured Airflow client handed to each resource.
type ProviderConfig struct {
	ApiClient *airflow.APIClient
	// AuthManagerApiClient serves roles and users; it is ApiClient unless an
	// auth manager base path is set.
	AuthManagerApiClient *airflow.APIClient
	AuthContext          context.Context
	// AuthManagerAuthContext carries the credentials for AuthManagerApiClient.
	AuthManagerAuthContext context.Context
}

// NewProviderConfig builds the Airflow API client and auth context from the
// already-resolved provider configuration values.
func NewProviderConfig(endpoint, oauth2Token, username, password string, disableSSL bool, basePath, sessionCookie, authManagerBasePath string) (ProviderConfig, error) {
	transport := http.DefaultTransport
	if disableSSL {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	httpClient := &http.Client{Transport: transport}

	u, err := url.Parse(endpoint)
	if err != nil {
		return ProviderConfig{}, fmt.Errorf("invalid base_endpoint: %w", err)
	}

	if username != "" && password == "" {
		return ProviderConfig{}, fmt.Errorf("found username for basic auth, but password not specified")
	}

	tokenCtx := context.WithValue(context.Background(), airflow.ContextAccessToken, oauth2Token)
	basicCtx := context.WithValue(context.Background(), airflow.ContextBasicAuth, airflow.BasicAuth{
		UserName: username,
		Password: password,
	})

	// Only one Authorization header can be sent, so each API gets a single
	// credential. The core API prefers the token; the auth manager API (FAB)
	// prefers basic auth, as FAB does not accept Airflow 3 JWT tokens.
	ctx, authManagerCtx := context.Background(), context.Background()
	switch {
	case oauth2Token != "" && username != "":
		ctx, authManagerCtx = tokenCtx, basicCtx
	case oauth2Token != "":
		ctx, authManagerCtx = tokenCtx, tokenCtx
	case username != "":
		log.Printf("[DEBUG] Using API Basic Auth")
		ctx, authManagerCtx = basicCtx, basicCtx
	}

	path := strings.TrimSuffix(u.Path, "/")

	defaultHeaders := map[string]string{}
	if sessionCookie != "" {
		defaultHeaders["Cookie"] = fmt.Sprintf("session=%s", sessionCookie)
		log.Printf("[DEBUG] Using session cookie authentication")
	}

	newClient := func(apiPath, description string) *airflow.APIClient {
		return airflow.NewAPIClient(&airflow.Configuration{
			Scheme:        u.Scheme,
			Host:          u.Host,
			DefaultHeader: defaultHeaders,
			Debug:         true,
			HTTPClient:    httpClient,
			Servers: airflow.ServerConfigurations{
				{
					URL:         fmt.Sprint(path, apiPath),
					Description: description,
				},
			},
		})
	}

	// Airflow 3 serves users and roles from the FAB auth manager rather than
	// the core API, so default to it when the core API is v2.
	if authManagerBasePath == "" && strings.TrimSuffix(basePath, "/") == "/api/v2" {
		authManagerBasePath = DefaultAuthManagerBasePathV3
	}

	apiClient := newClient(basePath, "Apache Airflow Stable API.")
	authManagerClient := apiClient
	if authManagerBasePath != "" {
		authManagerClient = newClient(authManagerBasePath, "Apache Airflow auth manager API.")
	}

	return ProviderConfig{
		ApiClient:              apiClient,
		AuthManagerApiClient:   authManagerClient,
		AuthContext:            ctx,
		AuthManagerAuthContext: authManagerCtx,
	}, nil
}
