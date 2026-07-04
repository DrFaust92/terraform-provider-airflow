package fwprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &variableResource{}
	_ resource.ResourceWithConfigure   = &variableResource{}
	_ resource.ResourceWithImportState = &variableResource{}
	_ resource.ResourceWithIdentity    = &variableResource{}
)

// variableIdentityModel is the resource identity for airflow_variable (its key).
type variableIdentityModel struct {
	ID types.String `tfsdk:"id"`
}

func newVariableResource() resource.Resource {
	return &variableResource{}
}

type variableResource struct {
	config client.ProviderConfig
}

type variableResourceModel struct {
	ID          types.String `tfsdk:"id"`
	Key         types.String `tfsdk:"key"`
	Value       types.String `tfsdk:"value"`
	Description types.String `tfsdk:"description"`
	TeamName    types.String `tfsdk:"team_name"`
}

func (r *variableResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_variable"
}

func (r *variableResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow variable.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The variable key.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"key": schema.StringAttribute{
				MarkdownDescription: "The variable key.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"value": schema.StringAttribute{
				MarkdownDescription: "The variable value.",
				Required:            true,
			},
			"description": schema.StringAttribute{
				MarkdownDescription: "The variable description.",
				Optional:            true,
				Computed:            true,
			},
			"team_name": schema.StringAttribute{
				MarkdownDescription: "Team name for Airflow 3 multi-team deployments. Requires multi-team mode enabled and the team to exist; ignored on Airflow 2.",
				Optional:            true,
				Computed:            true,
			},
		},
	}
}

func (r *variableResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"id": identityschema.StringAttribute{
				RequiredForImport: true,
			},
		},
	}
}

func (r *variableResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *variableResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan variableResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	key := plan.Key.ValueString()
	val := plan.Value.ValueString()

	variableReq := airflow.Variable{
		Key:   &key,
		Value: &val,
	}
	if !plan.Description.IsNull() && !plan.Description.IsUnknown() {
		variableReq.SetDescription(plan.Description.ValueString())
	}
	if !plan.TeamName.IsNull() && !plan.TeamName.IsUnknown() {
		variableReq.SetTeamName(plan.TeamName.ValueString())
	}

	_, httpResp, err := r.config.ApiClient.VariableApi.PostVariables(r.config.AuthContext).Variable(variableReq).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow variable", clientError("create", key, httpResp, err))
		return
	}

	plan.ID = types.StringValue(key)
	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow variable after create", fmt.Sprintf("variable %q not found immediately after creation", key))
		return
	}

	resp.Diagnostics.Append(resp.Identity.Set(ctx, variableIdentityModel{ID: types.StringValue(key)})...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *variableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state variableResourceModel
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

	resp.Diagnostics.Append(resp.Identity.Set(ctx, variableIdentityModel{ID: state.ID})...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *variableResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan variableResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	key := plan.ID.ValueString()
	val := plan.Value.ValueString()

	variableReq := airflow.Variable{
		Key:   &key,
		Value: &val,
	}
	// Mirror the SDKv2 resource: an absent description is cleared to "".
	if !plan.Description.IsNull() && !plan.Description.IsUnknown() {
		variableReq.SetDescription(plan.Description.ValueString())
	} else {
		variableReq.SetDescription("")
	}
	if !plan.TeamName.IsNull() && !plan.TeamName.IsUnknown() {
		variableReq.SetTeamName(plan.TeamName.ValueString())
	}

	_, httpResp, err := r.config.ApiClient.VariableApi.PatchVariable(r.config.AuthContext, key).Variable(variableReq).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow variable", clientError("update", key, httpResp, err))
		return
	}

	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.Identity.Set(ctx, variableIdentityModel{ID: plan.ID})...)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *variableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state variableResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	key := state.ID.ValueString()
	httpResp, err := r.config.ApiClient.VariableApi.DeleteVariable(r.config.AuthContext, key).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow variable", clientError("delete", key, httpResp, err))
	}
}

func (r *variableResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// readInto fetches the variable identified by m.ID and populates m. It returns
// false (without adding diagnostics) when the variable no longer exists.
func (r *variableResource) readInto(m *variableResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	variable, httpResp, err := r.config.ApiClient.VariableApi.GetVariable(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow variable", clientError("read", id, httpResp, err))
		return false
	}

	m.Key = types.StringValue(variable.GetKey())
	m.Value = types.StringValue(variable.GetValue())
	m.Description = types.StringValue(variable.GetDescription())
	m.TeamName = types.StringValue(variable.GetTeamName())
	return true
}

// clientError builds a diagnostic detail for a failed Airflow API call. The
// resource type is already named in the diagnostic summary, so the detail only
// carries the operation, the object id, the HTTP status, and -- crucially --
// Airflow's own error message from the response body (see apiErrorDetail),
// rather than just the bare HTTP status the client's error string exposes.
func clientError(op, id string, httpResp *http.Response, err error) string {
	msg := fmt.Sprintf("failed to %s %q", op, id)
	if httpResp != nil {
		msg += fmt.Sprintf(" (status %s)", httpResp.Status)
	}
	if detail := apiErrorDetail(err); detail != "" {
		return msg + ": " + detail
	}
	if err != nil {
		return msg + ": " + err.Error()
	}
	return msg
}

// apiErrorDetail extracts Airflow's error message from a client error. The
// generated client's error string is only the HTTP status; the useful message
// (RFC 7807 problem detail) is in the response body. Returns "" when absent.
func apiErrorDetail(err error) string {
	var apiErr *airflow.GenericOpenAPIError
	if !errors.As(err, &apiErr) {
		return ""
	}
	return problemDetail(apiErr.Body())
}

// problemDetail returns the human-readable message from an Airflow error
// response body. Airflow uses RFC 7807 problem details, so prefer `detail`,
// then `title`; fall back to the raw body for non-JSON payloads.
func problemDetail(body []byte) string {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return ""
	}
	var problem struct {
		Detail string `json:"detail"`
		Title  string `json:"title"`
	}
	if json.Unmarshal(body, &problem) == nil {
		if problem.Detail != "" {
			return problem.Detail
		}
		if problem.Title != "" {
			return problem.Title
		}
	}
	return string(body)
}
