package fwprovider

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework-validators/setvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &roleResource{}
	_ resource.ResourceWithConfigure   = &roleResource{}
	_ resource.ResourceWithImportState = &roleResource{}
)

func newRoleResource() resource.Resource {
	return &roleResource{}
}

type roleResource struct {
	config provider.ProviderConfig
}

type roleResourceModel struct {
	ID      types.String      `tfsdk:"id"`
	Name    types.String      `tfsdk:"name"`
	Actions []roleActionModel `tfsdk:"action"`
}

type roleActionModel struct {
	Action   types.String `tfsdk:"action"`
	Resource types.String `tfsdk:"resource"`
}

func (r *roleResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role"
}

func (r *roleResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow role. Note this resource is not supported on Airflow v3 (API v2): the Roles API is not available in Airflow v3.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The role name.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "The name of the role.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
		Blocks: map[string]schema.Block{
			"action": schema.SetNestedBlock{
				MarkdownDescription: "The action struct that defines the role.",
				Validators: []validator.Set{
					setvalidator.SizeAtLeast(1),
				},
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"action": schema.StringAttribute{
							MarkdownDescription: "The name of the permission.",
							Required:            true,
						},
						"resource": schema.StringAttribute{
							MarkdownDescription: "The name of the resource.",
							Required:            true,
						},
					},
				},
			},
		},
	}
}

func (r *roleResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(provider.ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected provider.ProviderConfig, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.config = cfg
}

func (r *roleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan roleResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := plan.Name.ValueString()
	role := airflow.Role{
		Name:    &name,
		Actions: expandRoleActions(plan.Actions),
	}

	_, httpResp, err := r.config.ApiClient.RoleApi.PostRole(r.config.AuthContext).Role(role).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow role", clientError("create", name, httpResp, err))
		return
	}

	plan.ID = types.StringValue(name)
	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow role after create", fmt.Sprintf("role %q not found immediately after creation", name))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *roleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state roleResourceModel
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

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *roleResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan roleResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := plan.ID.ValueString()
	role := airflow.Role{
		Name:    &name,
		Actions: expandRoleActions(plan.Actions),
	}

	_, httpResp, err := r.config.ApiClient.RoleApi.PatchRole(r.config.AuthContext, name).Role(role).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow role", clientError("update", name, httpResp, err))
		return
	}

	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *roleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := state.ID.ValueString()
	httpResp, err := r.config.ApiClient.RoleApi.DeleteRole(r.config.AuthContext, name).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow role", clientError("delete", name, httpResp, err))
	}
}

func (r *roleResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// readInto fetches the role identified by m.ID and populates m. It returns
// false (without adding diagnostics) when the role no longer exists.
func (r *roleResource) readInto(m *roleResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	role, httpResp, err := r.config.ApiClient.RoleApi.GetRole(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow role", clientError("read", id, httpResp, err))
		return false
	}

	m.Name = types.StringValue(role.GetName())
	m.Actions = flattenRoleActions(role.Actions)
	return true
}

func expandRoleActions(actions []roleActionModel) []airflow.ActionResource {
	if len(actions) == 0 {
		return nil
	}

	apiObjects := make([]airflow.ActionResource, 0, len(actions))
	for _, a := range actions {
		action := a.Action.ValueString()
		resource := a.Resource.ValueString()
		apiObjects = append(apiObjects, airflow.ActionResource{
			Action:   &airflow.Action{Name: &action},
			Resource: &airflow.Resource{Name: &resource},
		})
	}

	return apiObjects
}

func flattenRoleActions(apiObjects []airflow.ActionResource) []roleActionModel {
	if len(apiObjects) == 0 {
		return nil
	}

	actions := make([]roleActionModel, 0, len(apiObjects))
	for _, apiObject := range apiObjects {
		var a roleActionModel
		if apiObject.Action != nil {
			a.Action = types.StringValue(apiObject.Action.GetName())
		}
		if apiObject.Resource != nil {
			a.Resource = types.StringValue(apiObject.Resource.GetName())
		}
		actions = append(actions, a)
	}

	return actions
}
