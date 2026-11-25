// Package client builds the Apache Airflow API client used by the provider's
// resources. It has no dependency on any Terraform plugin SDK so it can be
// shared freely across packages.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

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

// DefaultTimeout bounds each API request so an unresponsive endpoint cannot
// hang a plan or apply indefinitely.
const DefaultTimeout = 2 * time.Minute

// HTTPOptions configures the HTTP client. The zero value uses the defaults.
type HTTPOptions struct {
	// Timeout of each request; DefaultTimeout when zero.
	Timeout time.Duration
	// CACertificate is PEM; when set it replaces the system roots.
	CACertificate string
	// ClientCertificate and ClientKey are a PEM key pair for TLS client
	// authentication; set both or neither.
	ClientCertificate string
	ClientKey         string
}

// newHTTPClient returns the client for the Airflow API. It starts from a
// clone of http.DefaultTransport, so HTTPS_PROXY, HTTP_PROXY and NO_PROXY are
// honoured with every TLS setting.
func newHTTPClient(disableSSL bool, opts HTTPOptions) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: disableSSL} //nolint:gosec // opt-in via disable_ssl_verification

	if opts.CACertificate != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(opts.CACertificate)) {
			return nil, errors.New("ca_certificate: no PEM-encoded certificate found")
		}
		tlsConfig.RootCAs = pool
	}

	if (opts.ClientCertificate == "") != (opts.ClientKey == "") {
		return nil, errors.New("client_certificate and client_key must be set together")
	}
	if opts.ClientCertificate != "" {
		pair, err := tls.X509KeyPair([]byte(opts.ClientCertificate), []byte(opts.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("client_certificate and client_key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

// NewProviderConfig builds the Airflow API client and auth context from the
// already-resolved provider configuration values.
func NewProviderConfig(endpoint, oauth2Token, username, password string, disableSSL bool, basePath, sessionCookie, authManagerBasePath string, authManagerBasicAuth bool, httpOpts HTTPOptions) (ProviderConfig, error) {
	httpClient, err := newHTTPClient(disableSSL, httpOpts)
	if err != nil {
		return ProviderConfig{}, err
	}

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
	// credential, and the token wins when both are set. The FAB auth manager
	// API accepts the Airflow 3 token from FAB provider 3.2/3.3 on; older FAB
	// releases only accept basic auth, which authManagerBasicAuth opts into.
	ctx, authManagerCtx := context.Background(), context.Background()
	switch {
	case oauth2Token != "" && username != "" && authManagerBasicAuth:
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
