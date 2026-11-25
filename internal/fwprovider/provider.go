// Package fwprovider holds the terraform-plugin-framework implementation of the
// Airflow provider, which serves every resource.
package fwprovider

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework-validators/providervalidator"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/list"
	"github.com/hashicorp/terraform-plugin-framework/path"
	fwprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// defaultBasePath is the default AIRFLOW_API_BASE_PATH.
const defaultBasePath = "/api/v1"

var (
	_ fwprovider.Provider                     = &airflowProvider{}
	_ fwprovider.ProviderWithListResources    = &airflowProvider{}
	_ fwprovider.ProviderWithConfigValidators = &airflowProvider{}
)

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
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			// Optional so the endpoint may come from AIRFLOW_BASE_ENDPOINT;
			// presence is enforced in Configure.
			"base_endpoint": schema.StringAttribute{
				Optional: true,
			},
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
				Description: "Disable TLS certificate verification. Prefer `ca_certificate` for a server with a private CA.",
			},
			"base_path": schema.StringAttribute{
				Optional:    true,
				Description: "Base path for Airflow API endpoints",
			},
			"auth_manager_base_path": schema.StringAttribute{
				Optional:    true,
				Description: "Base path for the auth manager API used by `airflow_role`, `airflow_user` and `airflow_user_roles`. Defaults to `/auth/fab/v1` when `base_path` is `/api/v2` (Airflow 3), otherwise to `base_path`.",
			},
			"auth_manager_basic_auth": schema.BoolAttribute{
				Optional:    true,
				Description: "Use `username`/`password` basic auth for the auth manager API even when `oauth2_token` is set. Needed for FAB provider releases before 3.2 (users) / 3.3 (roles), which do not accept the Airflow 3 token. Can also be set with `AIRFLOW_AUTH_MANAGER_BASIC_AUTH`.",
			},
			"ca_certificate": schema.StringAttribute{
				Optional:    true,
				Description: "PEM-encoded CA certificates used to verify the Airflow server certificate, instead of the system roots. Can also be set with the `AIRFLOW_CA_CERTIFICATE` environment variable.",
				Validators:  []validator.String{pemCertificateValidator{}},
			},
			"client_certificate": schema.StringAttribute{
				Optional:    true,
				Description: "PEM-encoded client certificate, for an Airflow API (or a proxy in front of it) that requires TLS client authentication. Set it together with `client_key`, both in the configuration or both in environment variables. Can also be set with the `AIRFLOW_CLIENT_CERTIFICATE` environment variable.",
				Validators:  []validator.String{pemCertificateValidator{}},
			},
			"client_key": schema.StringAttribute{
				Optional:    true,
				Sensitive:   true,
				Description: "PEM-encoded private key of `client_certificate`. Can also be set with the `AIRFLOW_CLIENT_KEY` environment variable.",
			},
			"timeout": schema.StringAttribute{
				Optional:    true,
				Description: "Timeout of each API request, as a duration such as `30s` or `5m`. Can also be set with the `AIRFLOW_TIMEOUT` environment variable. Defaults to `2m`.",
				Validators:  []validator.String{durationValidator{}},
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
	AuthManagerBasePath    types.String `tfsdk:"auth_manager_base_path"`
	AuthManagerBasicAuth   types.Bool   `tfsdk:"auth_manager_basic_auth"`
	CACertificate          types.String `tfsdk:"ca_certificate"`
	ClientCertificate      types.String `tfsdk:"client_certificate"`
	ClientKey              types.String `tfsdk:"client_key"`
	Timeout                types.String `tfsdk:"timeout"`
}

// ConfigValidators checks the provider configuration at plan time.
func (p *airflowProvider) ConfigValidators(context.Context) []fwprovider.ConfigValidator {
	return []fwprovider.ConfigValidator{
		providervalidator.RequiredTogether(path.MatchRoot("client_certificate"), path.MatchRoot("client_key")),
	}
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
	authManagerBasePath := stringOrEnv(config.AuthManagerBasePath, "AIRFLOW_AUTH_MANAGER_BASE_PATH", "")
	authManagerBasicAuth := boolOrEnv(config.AuthManagerBasicAuth, "AIRFLOW_AUTH_MANAGER_BASIC_AUTH")
	disableSSL := config.DisableSSLVerification.ValueBool()

	httpOpts := client.HTTPOptions{
		CACertificate:     stringOrEnv(config.CACertificate, "AIRFLOW_CA_CERTIFICATE", ""),
		ClientCertificate: stringOrEnv(config.ClientCertificate, "AIRFLOW_CLIENT_CERTIFICATE", ""),
		ClientKey:         stringOrEnv(config.ClientKey, "AIRFLOW_CLIENT_KEY", ""),
	}
	if v := stringOrEnv(config.Timeout, "AIRFLOW_TIMEOUT", ""); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			resp.Diagnostics.AddAttributeError(path.Root("timeout"), "Invalid timeout", fmt.Sprintf("Expected a positive duration such as 30s or 5m, got %q.", v))
			return
		}
		httpOpts.Timeout = d
	}

	if endpoint == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("base_endpoint"),
			"Missing Airflow API endpoint",
			"The provider requires base_endpoint to be set, either in the configuration or via the AIRFLOW_BASE_ENDPOINT environment variable.",
		)
		return
	}

	cfg, err := client.NewProviderConfig(endpoint, oauth2Token, username, password, disableSSL, basePath, sessionCookie, authManagerBasePath, authManagerBasicAuth, httpOpts)
	if err != nil {
		resp.Diagnostics.AddError("Unable to configure Airflow API client", err.Error())
		return
	}

	resp.ResourceData = cfg
	resp.DataSourceData = cfg
	// List resources receive their provider data from a separate field; without
	// this they are never configured and panic on the first API call.
	resp.ListResourceData = cfg
}

func (p *airflowProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		newVariableResource,
		newPoolResource,
		newRoleResource,
		newUserResource,
		newUserRolesResource,
		newDagResource,
		newDagRunResource,
		newConnectionResource,
	}
}

func (p *airflowProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		newVariableDataSource,
		newConnectionDataSource,
		newPoolDataSource,
		newDagDataSource,
	}
}

// ListResources returns the resource types that support `terraform query` /
// list-based import-config generation.
func (p *airflowProvider) ListResources(_ context.Context) []func() list.ListResource {
	return []func() list.ListResource{
		newVariableListResource,
		newPoolListResource,
		newConnectionListResource,
		newRoleListResource,
		newUserListResource,
		newDagListResource,
	}
}

// boolOrEnv returns the configured value when known and non-null, otherwise
// the named environment variable parsed with strconv.ParseBool (false if
// unset or invalid).
func boolOrEnv(v types.Bool, envKey string) bool {
	if !v.IsNull() && !v.IsUnknown() {
		return v.ValueBool()
	}
	b, _ := strconv.ParseBool(os.Getenv(envKey))
	return b
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

// pemCertificateValidator checks that a value contains a PEM-encoded certificate.
type pemCertificateValidator struct{}

func (pemCertificateValidator) Description(context.Context) string {
	return "value must contain a PEM-encoded certificate"
}

func (v pemCertificateValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (pemCertificateValidator) ValidateString(_ context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if !x509.NewCertPool().AppendCertsFromPEM([]byte(req.ConfigValue.ValueString())) {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid PEM certificate", "The value does not contain a PEM-encoded certificate.")
	}
}

// durationValidator checks that a value is a positive duration such as "30s".
type durationValidator struct{}

func (durationValidator) Description(context.Context) string {
	return "value must be a positive duration such as 30s or 5m"
}

func (v durationValidator) MarkdownDescription(ctx context.Context) string {
	return v.Description(ctx)
}

func (durationValidator) ValidateString(_ context.Context, req validator.StringRequest, resp *validator.StringResponse) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if d, err := time.ParseDuration(req.ConfigValue.ValueString()); err != nil || d <= 0 {
		resp.Diagnostics.AddAttributeError(req.Path, "Invalid duration", "Expected a positive duration such as 30s or 5m.")
	}
}
