package fwprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &connectionResource{}
	_ resource.ResourceWithConfigure   = &connectionResource{}
	_ resource.ResourceWithImportState = &connectionResource{}
	_ resource.ResourceWithIdentity    = &connectionResource{}
)

type connectionIdentityModel struct {
	ID types.String `tfsdk:"id"`
}

func newConnectionResource() resource.Resource {
	return &connectionResource{}
}

type connectionResource struct {
	config client.ProviderConfig
}

type connectionResourceModel struct {
	ID                types.String `tfsdk:"id"`
	ConnectionID      types.String `tfsdk:"connection_id"`
	ConnType          types.String `tfsdk:"conn_type"`
	Description       types.String `tfsdk:"description"`
	Host              types.String `tfsdk:"host"`
	Login             types.String `tfsdk:"login"`
	Schema            types.String `tfsdk:"schema"`
	Port              types.Int64  `tfsdk:"port"`
	Password          types.String `tfsdk:"password"`
	PasswordWO        types.String `tfsdk:"password_wo"`
	PasswordWOVersion types.String `tfsdk:"password_wo_version"`
	Extra             types.String `tfsdk:"extra"`
	ExtraWO           types.String `tfsdk:"extra_wo"`
	ExtraWOVersion    types.String `tfsdk:"extra_wo_version"`
	TeamName          types.String `tfsdk:"team_name"`
}

func (r *connectionResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_connection"
}

func (r *connectionResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow connection.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The connection ID.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"connection_id": schema.StringAttribute{
				MarkdownDescription: "The connection ID.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"conn_type": schema.StringAttribute{
				MarkdownDescription: "The connection type.",
				Required:            true,
			},
			"description": schema.StringAttribute{
				MarkdownDescription: "The description of the connection.",
				Optional:            true,
			},
			"host": schema.StringAttribute{
				MarkdownDescription: "The host of the connection.",
				Optional:            true,
			},
			"login": schema.StringAttribute{
				MarkdownDescription: "The login of the connection.",
				Optional:            true,
			},
			"schema": schema.StringAttribute{
				MarkdownDescription: "The schema of the connection.",
				Optional:            true,
			},
			"port": schema.Int64Attribute{
				MarkdownDescription: "The port of the connection.",
				Optional:            true,
				Validators: []validator.Int64{
					int64validator.Between(0, 65535),
				},
			},
			"password": schema.StringAttribute{
				MarkdownDescription: "The password of the connection.",
				Optional:            true,
				Sensitive:           true,
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("password_wo")),
				},
			},
			"password_wo": schema.StringAttribute{
				MarkdownDescription: "The password of the connection. This field is write-only and will not be returned by the API.",
				Optional:            true,
				WriteOnly:           true,
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("password")),
					stringvalidator.AlsoRequires(path.MatchRoot("password_wo_version")),
				},
			},
			"password_wo_version": schema.StringAttribute{
				MarkdownDescription: "Triggers update of password_wo write-only. For more info see [updating write-only attributes](https://developer.hashicorp.com/terraform/language/manage-sensitive-data/write-only)",
				Optional:            true,
				Validators: []validator.String{
					stringvalidator.AlsoRequires(path.MatchRoot("password_wo")),
				},
			},
			"extra": schema.StringAttribute{
				MarkdownDescription: "Other values that cannot be put into another field, e.g. RSA keys.",
				Optional:            true,
				Sensitive:           true,
				PlanModifiers: []planmodifier.String{
					suppressEquivalentJSON{},
				},
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("extra_wo")),
				},
			},
			"extra_wo": schema.StringAttribute{
				MarkdownDescription: "Other values that cannot be put into another field, e.g. RSA keys. This field is write-only and is never stored in state. Requires Terraform 1.11 or later.",
				Optional:            true,
				WriteOnly:           true,
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("extra")),
					stringvalidator.AlsoRequires(path.MatchRoot("extra_wo_version")),
				},
			},
			"extra_wo_version": schema.StringAttribute{
				MarkdownDescription: "Triggers update of `extra_wo` write-only. For more info see [updating write-only attributes](https://developer.hashicorp.com/terraform/language/manage-sensitive-data/write-only).",
				Optional:            true,
				Validators: []validator.String{
					stringvalidator.AlsoRequires(path.MatchRoot("extra_wo")),
				},
			},
			"team_name": schema.StringAttribute{
				MarkdownDescription: "Team name for Airflow 3 multi-team deployments. Requires multi-team mode enabled and the team to exist; ignored on Airflow 2.",
				Optional:            true,
			},
		},
	}
}

func (r *connectionResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"id": identityschema.StringAttribute{RequiredForImport: true},
		},
	}
}

func (r *connectionResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(client.ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected client.ProviderConfig, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.config = cfg
}

func (r *connectionResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan connectionResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	connID := plan.ConnectionID.ValueString()
	connType := plan.ConnType.ValueString()
	conn := airflow.Connection{ConnectionId: &connID, ConnType: &connType}

	if !plan.Host.IsNull() {
		conn.SetHost(plan.Host.ValueString())
	}
	if !plan.Description.IsNull() {
		conn.SetDescription(plan.Description.ValueString())
	}
	if !plan.Login.IsNull() {
		conn.SetLogin(plan.Login.ValueString())
	}
	if !plan.Schema.IsNull() {
		conn.SetSchema(plan.Schema.ValueString())
	}
	if !plan.Port.IsNull() {
		conn.SetPort(int32(plan.Port.ValueInt64()))
	}
	if e := r.resolveExtra(ctx, plan.Extra, req.Config, &resp.Diagnostics); e != "" {
		conn.SetExtra(e)
	}
	if !plan.TeamName.IsNull() && !plan.TeamName.IsUnknown() {
		conn.SetTeamName(plan.TeamName.ValueString())
	}

	if pw := r.resolvePassword(ctx, plan.Password, req.Config, &resp.Diagnostics); pw != "" {
		conn.SetPassword(pw)
	}
	if resp.Diagnostics.HasError() {
		return
	}

	_, httpResp, err := r.config.ApiClient.ConnectionApi.PostConnection(r.config.AuthContext).Connection(conn).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow connection", clientError("create", connID, httpResp, err))
		return
	}

	plan.ID = types.StringValue(connID)
	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow connection after create", fmt.Sprintf("connection %q not found immediately after creation", connID))
		return
	}

	resp.Diagnostics.Append(resp.Identity.Set(ctx, connectionIdentityModel{ID: plan.ID})...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *connectionResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state connectionResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	found := r.readInto(&state, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	if !found {
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.Identity.Set(ctx, connectionIdentityModel{ID: state.ID})...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *connectionResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state connectionResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	connID := plan.ID.ValueString()
	connType := plan.ConnType.ValueString()
	conn := airflow.Connection{ConnectionId: &connID, ConnType: &connType}

	// Optional fields are explicitly cleared when absent, mirroring the SDKv2
	// resource's *Nil() calls.
	if !plan.Host.IsNull() {
		conn.SetHost(plan.Host.ValueString())
	} else {
		conn.SetHostNil()
	}
	if !plan.Description.IsNull() {
		conn.SetDescription(plan.Description.ValueString())
	} else {
		conn.SetDescriptionNil()
	}
	if !plan.Login.IsNull() {
		conn.SetLogin(plan.Login.ValueString())
	} else {
		conn.SetLoginNil()
	}
	if !plan.Schema.IsNull() {
		conn.SetSchema(plan.Schema.ValueString())
	} else {
		conn.SetSchemaNil()
	}
	if !plan.Port.IsNull() {
		conn.SetPort(int32(plan.Port.ValueInt64()))
	} else {
		conn.SetPortNil()
	}
	if !plan.Extra.IsNull() {
		conn.SetExtra(plan.Extra.ValueString())
	} else if !plan.ExtraWOVersion.IsNull() {
		// Write-only extra: only re-send on a version bump; otherwise leave the
		// stored extra untouched (do not clear it).
		if !plan.ExtraWOVersion.Equal(state.ExtraWOVersion) {
			if e := r.writeOnlyExtra(ctx, req.Config, &resp.Diagnostics); e != "" {
				conn.SetExtra(e)
			}
		}
	} else {
		conn.SetExtraNil()
	}
	if !plan.TeamName.IsNull() && !plan.TeamName.IsUnknown() {
		conn.SetTeamName(plan.TeamName.ValueString())
	}

	if !plan.Password.IsNull() && plan.Password.ValueString() != "" {
		conn.SetPassword(plan.Password.ValueString())
	} else if !plan.PasswordWOVersion.Equal(state.PasswordWOVersion) {
		if pw := r.writeOnlyPassword(ctx, req.Config, &resp.Diagnostics); pw != "" {
			conn.SetPassword(pw)
		}
	}
	if resp.Diagnostics.HasError() {
		return
	}

	_, httpResp, err := r.config.ApiClient.ConnectionApi.PatchConnection(r.config.AuthContext, connID).Connection(conn).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow connection", clientError("update", connID, httpResp, err))
		return
	}

	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.Identity.Set(ctx, connectionIdentityModel{ID: plan.ID})...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *connectionResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state connectionResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id := state.ID.ValueString()
	httpResp, err := r.config.ApiClient.ConnectionApi.DeleteConnection(r.config.AuthContext, id).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow connection", clientError("delete", id, httpResp, err))
	}
}

func (r *connectionResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// resolvePassword returns the password to send on create: the configured
// password if set, otherwise the write-only password_wo value.
func (r *connectionResource) resolvePassword(ctx context.Context, password types.String, config tfsdk.Config, diags *diag.Diagnostics) string {
	if !password.IsNull() && password.ValueString() != "" {
		return password.ValueString()
	}
	return r.writeOnlyPassword(ctx, config, diags)
}

// writeOnlyPassword reads the write-only password_wo attribute from config.
func (r *connectionResource) writeOnlyPassword(ctx context.Context, config tfsdk.Config, diags *diag.Diagnostics) string {
	var pwWO types.String
	diags.Append(config.GetAttribute(ctx, path.Root("password_wo"), &pwWO)...)
	if diags.HasError() || pwWO.IsNull() || pwWO.IsUnknown() {
		return ""
	}
	return pwWO.ValueString()
}

// resolveExtra returns the extra to send on create: the configured extra if
// set, otherwise the write-only extra_wo value.
func (r *connectionResource) resolveExtra(ctx context.Context, extra types.String, config tfsdk.Config, diags *diag.Diagnostics) string {
	if !extra.IsNull() && extra.ValueString() != "" {
		return extra.ValueString()
	}
	return r.writeOnlyExtra(ctx, config, diags)
}

// writeOnlyExtra reads the write-only extra_wo attribute from config.
func (r *connectionResource) writeOnlyExtra(ctx context.Context, config tfsdk.Config, diags *diag.Diagnostics) string {
	var eWO types.String
	diags.Append(config.GetAttribute(ctx, path.Root("extra_wo"), &eWO)...)
	if diags.HasError() || eWO.IsNull() || eWO.IsUnknown() {
		return ""
	}
	return eWO.ValueString()
}

// readInto fetches the connection identified by m.ID and populates m. Optional
// attributes preserve their null-ness when the API returns an empty value, and
// the password is preserved when the API hides it (absent or masked). Returns
// false (without diagnostics) when the connection no longer exists.
func (r *connectionResource) readInto(m *connectionResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	conn, httpResp, err := r.config.ApiClient.ConnectionApi.GetConnection(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow connection", clientError("read", id, httpResp, err))
		return false
	}

	m.ConnectionID = types.StringValue(conn.GetConnectionId())
	m.ConnType = types.StringValue(conn.GetConnType())
	setOptionalString(&m.Host, conn.GetHost())
	setOptionalString(&m.Login, conn.GetLogin())
	setOptionalString(&m.Schema, conn.GetSchema())
	setOptionalString(&m.Description, conn.GetDescription())
	setOptionalString(&m.TeamName, conn.GetTeamName())

	if p := conn.GetPort(); p != 0 {
		m.Port = types.Int64Value(int64(p))
	} else if !m.Port.IsNull() {
		m.Port = types.Int64Value(0)
	}

	apiExtra := conn.GetExtra()
	switch {
	case !m.Extra.IsNull() && jsonSemanticEqual(m.Extra.ValueString(), apiExtra):
		// Keep the configured/state value when it is semantically-equal JSON to
		// what the API returns, so the post-apply value matches the plan
		// (the API may reformat JSON).
	case !m.Extra.IsNull() && jsonEqualIgnoringMasked(m.Extra.ValueString(), apiExtra):
		// Airflow's SecretsMasker returns secret-like keys inside `extra` as
		// masked placeholders (e.g. {"api_key":"***"}). When that masking is the
		// only difference from state, keep the real state value so we neither
		// persist "***" nor produce a perpetual diff. See GH issue #34.
	case apiExtra != "":
		m.Extra = types.StringValue(apiExtra)
	case !m.Extra.IsNull():
		m.Extra = types.StringValue("")
	}

	// Preserve the configured password unless the API returns a real
	// (non-masked, non-empty) value.
	if pw, ok := conn.GetPasswordOk(); ok && pw != nil {
		if s := *pw; strings.TrimSpace(s) != "" && strings.Trim(s, "*") != "" {
			m.Password = types.StringValue(s)
		}
	}

	return true
}

// setOptionalString updates an Optional string attribute from an API value while
// preserving null-ness: an empty API value leaves a null attribute null (so it
// keeps matching a config that omits it) but clears a previously set value.
func setOptionalString(attr *types.String, apiValue string) {
	if apiValue != "" {
		*attr = types.StringValue(apiValue)
	} else if !attr.IsNull() {
		*attr = types.StringValue("")
	}
}

// suppressEquivalentJSON is a plan modifier for the connection `extra`
// attribute. It suppresses diffs when the prior state and the configured value
// are both valid JSON and semantically equal (ignoring formatting/key order).
// Crucially it does NOT validate the value: `extra` may be empty or non-JSON
// (e.g. when migrating state written by the SDKv2 provider, which stored an
// unset extra as ""), so non-JSON values are left untouched rather than rejected.
type suppressEquivalentJSON struct{}

func (m suppressEquivalentJSON) Description(_ context.Context) string {
	return "Suppress diffs between semantically-equivalent JSON extra values."
}

func (m suppressEquivalentJSON) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m suppressEquivalentJSON) PlanModifyString(_ context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.StateValue.IsNull() || req.PlanValue.IsNull() || req.PlanValue.IsUnknown() {
		return
	}

	if jsonSemanticEqual(req.StateValue.ValueString(), req.PlanValue.ValueString()) {
		resp.PlanValue = req.StateValue
	}
}

// jsonSemanticEqual reports whether a and b are both valid JSON and deeply
// equal (ignoring formatting/key order). Non-JSON values are never equal here,
// so callers fall back to normal string comparison for them.
func jsonSemanticEqual(a, b string) bool {
	if a == b {
		return true
	}
	var av, bv interface{}
	if json.Unmarshal([]byte(a), &av) != nil || json.Unmarshal([]byte(b), &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// jsonEqualIgnoringMasked reports whether state and api are both valid JSON and
// equal once masked placeholders in api (e.g. "***", returned by Airflow's
// SecretsMasker for secret-like keys) are treated as equal to the corresponding
// value in state. Non-JSON values are never equal here.
func jsonEqualIgnoringMasked(state, api string) bool {
	var sv, av interface{}
	if json.Unmarshal([]byte(state), &sv) != nil || json.Unmarshal([]byte(api), &av) != nil {
		return false
	}
	return reflect.DeepEqual(sv, unmaskAgainst(av, sv))
}

// unmaskAgainst returns a copy of api with every masked string leaf (non-empty
// and consisting solely of '*') replaced by the corresponding value from state
// when one is available, leaving all other values untouched.
func unmaskAgainst(api, state interface{}) interface{} {
	switch a := api.(type) {
	case map[string]interface{}:
		s, _ := state.(map[string]interface{})
		out := make(map[string]interface{}, len(a))
		for k, v := range a {
			var sv interface{}
			if s != nil {
				sv = s[k]
			}
			out[k] = unmaskAgainst(v, sv)
		}
		return out
	case []interface{}:
		s, _ := state.([]interface{})
		out := make([]interface{}, len(a))
		for i, v := range a {
			var sv interface{}
			if s != nil && i < len(s) {
				sv = s[i]
			}
			out[i] = unmaskAgainst(v, sv)
		}
		return out
	case string:
		if a != "" && strings.Trim(a, "*") == "" {
			if s, ok := state.(string); ok && s != "" {
				return s
			}
		}
		return a
	default:
		return a
	}
}
