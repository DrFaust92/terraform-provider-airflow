package fwprovider

import (
	"context"
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

func clientError(op, key string, httpResp *http.Response, err error) string {
	if httpResp != nil {
		return fmt.Sprintf("failed to %s variable %q, Status: %q from Airflow: %s", op, key, httpResp.Status, err)
	}
	return fmt.Sprintf("failed to %s variable %q from Airflow: %s", op, key, err)
}
