package fwprovider

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework-validators/setvalidator"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/identityschema"
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
	_ resource.ResourceWithIdentity    = &roleResource{}
)

func newRoleResource() resource.Resource {
	return &roleResource{}
}

type roleResource struct {
	config client.ProviderConfig
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
		MarkdownDescription: "Provides an Airflow role. On Airflow 3, set the provider `auth_manager_base_path` (e.g. `/auth/fab/v1`). The FAB auth manager grants every custom role `can_read` on `Website` (homepage access); it is not reported as drift unless you configure it.",
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

	_, httpResp, err := r.config.AuthManagerApiClient.RoleApi.PostRole(r.config.AuthManagerAuthContext).Role(role).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow role", clientError("create", name, httpResp, err))
		return
	}

	// Save the plan rather than reading back: actions the server adds itself
	// would fail the post-apply consistency check. They show as drift instead.
	plan.ID = types.StringValue(name)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, roleIdentity(plan.ID))...)
}

func (r *roleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	prior := state.Actions
	found := r.readInto(&state, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	if !found {
		// Terraform before 1.12 sends no prior identity, and the framework
		// rejects a read without one even when the resource is gone.
		resp.Diagnostics.Append(resp.Identity.Set(ctx, roleIdentity(state.ID))...)
		resp.State.RemoveResource(ctx)
		return
	}
	state.Actions = withoutImplicitRoleActions(state.Actions, prior)

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, roleIdentity(state.ID))...)
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

	_, httpResp, err := r.config.AuthManagerApiClient.RoleApi.PatchRole(r.config.AuthManagerAuthContext, name).Role(role).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow role", clientError("update", name, httpResp, err))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
	resp.Diagnostics.Append(resp.Identity.Set(ctx, roleIdentity(plan.ID))...)
}

func (r *roleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state roleResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := state.ID.ValueString()
	httpResp, err := r.config.AuthManagerApiClient.RoleApi.DeleteRole(r.config.AuthManagerAuthContext, name).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow role", clientError("delete", name, httpResp, err))
	}
}

// roleIdentityModel is the resource identity for airflow_role.
type roleIdentityModel struct {
	Name types.String `tfsdk:"name"`
}

func (r *roleResource) IdentitySchema(_ context.Context, _ resource.IdentitySchemaRequest, resp *resource.IdentitySchemaResponse) {
	resp.IdentitySchema = identityschema.Schema{
		Attributes: map[string]identityschema.Attribute{
			"name": identityschema.StringAttribute{
				RequiredForImport: true,
				Description:       "The role name.",
			},
		},
	}
}

// roleIdentity builds the resource identity from the resource ID.
func roleIdentity(id types.String) roleIdentityModel {
	return roleIdentityModel{Name: id}
}

func (r *roleResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughWithIdentity(ctx, path.Root("id"), path.Root("name"), req, resp)
}

// readInto fetches the role identified by m.ID and populates m. It returns
// false (without adding diagnostics) when the role no longer exists.
func (r *roleResource) readInto(m *roleResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	role, httpResp, err := r.config.AuthManagerApiClient.RoleApi.GetRole(r.config.AuthManagerAuthContext, id).Execute()
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

// implicitRoleAction is the permission the FAB auth manager adds to every
// custom role (homepage access; see add_homepage_access_to_custom_roles) and
// re-adds on sync, so it cannot be managed away.
var implicitRoleAction = roleActionModel{
	Action:   types.StringValue("can_read"),
	Resource: types.StringValue("Website"),
}

// withoutImplicitRoleActions drops the FAB-managed implicit permission from
// actions read back from the API unless prior state already tracked it, so
// roles that do not configure it do not show a perpetual diff.
func withoutImplicitRoleActions(actions, prior []roleActionModel) []roleActionModel {
	if slices.Contains(prior, implicitRoleAction) {
		return actions
	}
	out := slices.DeleteFunc(slices.Clone(actions), func(a roleActionModel) bool { return a == implicitRoleAction })
	if len(out) == 0 {
		return nil
	}
	return out
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
