// Package fwprovider holds the terraform-plugin-framework implementation of the
// Airflow provider. Resources are migrated here one at a time from the SDKv2
// provider (internal/provider) and the two are muxed together in main.go.
//
// The provider schema below must stay byte-for-byte equivalent (at the protocol
// level) to the SDKv2 provider schema, otherwise tf6muxserver rejects the
// combined server. Keep attribute names, types, and optional/required/sensitive
// flags in sync with internal/provider/provider.go.
package fwprovider

import (
	"context"
	"os"

	"github.com/drfaust92/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	fwprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// defaultBasePath mirrors the SDKv2 AIRFLOW_API_BASE_PATH default.
const defaultBasePath = "/api/v1"

var _ fwprovider.Provider = &airflowProvider{}

type airflowProvider struct {
	version string
}

// New returns a constructor for the framework provider.
func New(version string) func() fwprovider.Provider {
	return func() fwprovider.Provider {
		return &airflowProvider{version: version}
	}
}

func (p *airflowProvider) Metadata(_ context.Context, _ fwprovider.MetadataRequest, resp *fwprovider.MetadataResponse) {
	resp.TypeName = "airflow"
	resp.Version = p.version
}

func (p *airflowProvider) Schema(_ context.Context, _ fwprovider.SchemaRequest, resp *fwprovider.SchemaResponse) {
	// Mirror SDKv2's EnvDefaultFunc behaviour: a Required attribute whose
	// DefaultFunc can supply a value is downgraded to Optional at the protocol
	// level (helper/schema/core_schema.go), which means base_endpoint is
	// Required when AIRFLOW_BASE_ENDPOINT is unset and Optional when it is set.
	// The framework schema must track this so the muxed provider schemas stay
	// identical in every environment. Presence is enforced in Configure.
	baseEndpoint := schema.StringAttribute{Required: true}
	if os.Getenv("AIRFLOW_BASE_ENDPOINT") != "" {
		baseEndpoint = schema.StringAttribute{Optional: true}
	}

	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"base_endpoint": baseEndpoint,
			"oauth2_token": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "The oauth to use for API authentication",
			},
			"username": schema.StringAttribute{
				Optional:    true,
				Description: "The username to use for API basic authentication",
			},
			"password": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "The password to use for API basic authentication",
			},
			"disable_ssl_verification": schema.BoolAttribute{
				Optional:    true,
				Description: "Disable SSL verification",
			},
			"base_path": schema.StringAttribute{
				Optional:    true,
				Description: "Base path for Airflow API endpoints",
			},
			"session_cookie": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "A session cookie value to use for authentication (sent as Cookie: session={value}). Useful for AWS MWAA private environments.",
			},
		},
	}
}

type airflowProviderModel struct {
	BaseEndpoint           types.String `tfsdk:"base_endpoint"`
	OAuth2Token            types.String `tfsdk:"oauth2_token"`
	Username               types.String `tfsdk:"username"`
	Password               types.String `tfsdk:"password"`
	DisableSSLVerification types.Bool   `tfsdk:"disable_ssl_verification"`
	BasePath               types.String `tfsdk:"base_path"`
	SessionCookie          types.String `tfsdk:"session_cookie"`
}

func (p *airflowProvider) Configure(_ context.Context, req fwprovider.ConfigureRequest, resp *fwprovider.ConfigureResponse) {
	var config airflowProviderModel
	resp.Diagnostics.Append(req.Config.Get(context.Background(), &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Resolve values with environment-variable fallbacks, matching the SDKv2
	// EnvDefaultFunc behaviour that the framework does not provide natively.
	endpoint := stringOrEnv(config.BaseEndpoint, "AIRFLOW_BASE_ENDPOINT", "")
	oauth2Token := stringOrEnv(config.OAuth2Token, "AIRFLOW_OAUTH2_TOKEN", "")
	username := stringOrEnv(config.Username, "AIRFLOW_API_USERNAME", "")
	password := stringOrEnv(config.Password, "AIRFLOW_API_PASSWORD", "")
	basePath := stringOrEnv(config.BasePath, "AIRFLOW_API_BASE_PATH", defaultBasePath)
	sessionCookie := stringOrEnv(config.SessionCookie, "AIRFLOW_SESSION_COOKIE", "")
	disableSSL := config.DisableSSLVerification.ValueBool()

	if endpoint == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("base_endpoint"),
			"Missing Airflow API endpoint",
			"The provider requires base_endpoint to be set, either in the configuration or via the AIRFLOW_BASE_ENDPOINT environment variable.",
		)
		return
	}

	cfg, err := provider.NewProviderConfig(endpoint, oauth2Token, username, password, disableSSL, basePath, sessionCookie)
	if err != nil {
		resp.Diagnostics.AddError("Unable to configure Airflow API client", err.Error())
		return
	}

	resp.ResourceData = cfg
	resp.DataSourceData = cfg
}

func (p *airflowProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		newVariableResource,
		newPoolResource,
	}
}

func (p *airflowProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return nil
}

// stringOrEnv returns the configured value when known and non-null, otherwise
// the named environment variable, otherwise the supplied default.
func stringOrEnv(v types.String, envKey, def string) string {
	if !v.IsNull() && !v.IsUnknown() {
		return v.ValueString()
	}
	if env, ok := os.LookupEnv(envKey); ok {
		return env
	}
	return def
}
