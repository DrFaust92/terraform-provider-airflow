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

// ProviderConfig is the configured Airflow client handed to each resource.
type ProviderConfig struct {
	ApiClient   *airflow.APIClient
	AuthContext context.Context
}

// NewProviderConfig builds the Airflow API client and auth context from the
// already-resolved provider configuration values.
func NewProviderConfig(endpoint, oauth2Token, username, password string, disableSSL bool, basePath, sessionCookie string) (ProviderConfig, error) {
	var transport http.RoundTripper = http.DefaultTransport
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

	ctx := context.Background()

	if oauth2Token != "" {
		ctx = context.WithValue(ctx, airflow.ContextAccessToken, oauth2Token)
	}

	if username != "" {
		if password == "" {
			return ProviderConfig{}, fmt.Errorf("found username for basic auth, but password not specified")
		}
		log.Printf("[DEBUG] Using API Basic Auth")

		ctx = context.WithValue(ctx, airflow.ContextBasicAuth, airflow.BasicAuth{
			UserName: username,
			Password: password,
		})
	}

	path := strings.TrimSuffix(u.Path, "/")

	defaultHeaders := map[string]string{}
	if sessionCookie != "" {
		defaultHeaders["Cookie"] = fmt.Sprintf("session=%s", sessionCookie)
		log.Printf("[DEBUG] Using session cookie authentication")
	}

	clientConf := &airflow.Configuration{
		Scheme:        u.Scheme,
		Host:          u.Host,
		DefaultHeader: defaultHeaders,
		Debug:         true,
		HTTPClient:    httpClient,
		Servers: airflow.ServerConfigurations{
			{
				URL:         fmt.Sprint(path, basePath),
				Description: "Apache Airflow Stable API.",
			},
		},
	}

	return ProviderConfig{
		ApiClient:   airflow.NewAPIClient(clientConf),
		AuthContext: ctx,
	}, nil
}
