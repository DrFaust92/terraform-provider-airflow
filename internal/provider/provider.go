package provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/logging"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

type ProviderConfig struct {
	ApiClient   *airflow.APIClient
	AuthContext context.Context
}

func AirflowProvider() *schema.Provider {
	provider := &schema.Provider{
		Schema: map[string]*schema.Schema{
			"base_endpoint": {
				Type:         schema.TypeString,
				Required:     true,
				DefaultFunc:  schema.EnvDefaultFunc("AIRFLOW_BASE_ENDPOINT", nil),
				ValidateFunc: validation.IsURLWithHTTPorHTTPS,
			},
			"oauth2_token": {
				Type:          schema.TypeString,
				Optional:      true,
				Sensitive:     true,
				Description:   "The oauth to use for API authentication",
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_OAUTH2_TOKEN", nil),
				ConflictsWith: []string{"username", "password"},
			},
			"username": {
				Type:          schema.TypeString,
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_API_USERNAME", nil),
				Optional:      true,
				Description:   "The username to use for API basic authentication",
				RequiredWith:  []string{"password"},
				ConflictsWith: []string{"oauth2_token"},
			},
			"password": {
				Type:          schema.TypeString,
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_API_PASSWORD", nil),
				Optional:      true,
				Sensitive:     true,
				Description:   "The password to use for API basic authentication",
				RequiredWith:  []string{"username"},
				ConflictsWith: []string{"oauth2_token"},
			},
			"disable_ssl_verification": {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "Disable SSL verification",
				Default:     false,
			},
			"base_path": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Base path for Airflow API endpoints",
				DefaultFunc: schema.EnvDefaultFunc("AIRFLOW_API_BASE_PATH", "/api/v1"),
			},
			"session_cookie": {
				Type:          schema.TypeString,
				Optional:      true,
				Sensitive:     true,
				Description:   "A session cookie value to use for authentication (sent as Cookie: session=<value>). Useful for AWS MWAA private environments.",
				DefaultFunc:   schema.EnvDefaultFunc("AIRFLOW_SESSION_COOKIE", nil),
				ConflictsWith: []string{"oauth2_token", "username", "password"},
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"airflow_connection": resourceConnection(),
			"airflow_dag":        resourceDag(),
			"airflow_dag_run":    resourceDagRun(),
			"airflow_pool":       resourcePool(),
			"airflow_role":       resourceRole(),
			"airflow_user":       resourceUser(),
			"airflow_user_roles": resourceUserRoles(),
			// airflow_variable is served by the Plugin Framework provider (internal/fwprovider), muxed in main.go.
		},
		// ConfigureContextFunc: providerConfigure,
	}

	provider.ConfigureContextFunc = func(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
		return providerConfigure(ctx, d)
	}

	return provider
}

func providerConfigure(_ context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	oauth2Token, _ := d.Get("oauth2_token").(string)
	username, _ := d.Get("username").(string)
	password, _ := d.Get("password").(string)
	sessionCookie, _ := d.Get("session_cookie").(string)

	prov, err := NewProviderConfig(
		d.Get("base_endpoint").(string),
		oauth2Token,
		username,
		password,
		d.Get("disable_ssl_verification").(bool),
		d.Get("base_path").(string),
		sessionCookie,
	)
	if err != nil {
		return nil, diag.FromErr(err)
	}

	return prov, diag.Diagnostics{}
}

// NewProviderConfig builds the Airflow API client and auth context from the
// already-resolved provider configuration values. It is shared by the SDKv2
// provider (this package) and the Plugin Framework provider (internal/fwprovider)
// so both code paths construct the client identically.
func NewProviderConfig(endpoint, oauth2Token, username, password string, disableSSL bool, basePath, sessionCookie string) (ProviderConfig, error) {
	var transport http.RoundTripper

	if disableSSL {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	} else {
		transport = logging.NewLoggingHTTPTransport(http.DefaultTransport)
	}

	client := &http.Client{
		Transport: transport,
	}

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
		HTTPClient:    client,
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
