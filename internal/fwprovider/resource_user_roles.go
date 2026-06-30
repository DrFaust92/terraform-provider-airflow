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
	_ resource.Resource                = &userRolesResource{}
	_ resource.ResourceWithConfigure   = &userRolesResource{}
	_ resource.ResourceWithImportState = &userRolesResource{}
)

func newUserRolesResource() resource.Resource {
	return &userRolesResource{}
}

type userRolesResource struct {
	config provider.ProviderConfig
}

type userRolesResourceModel struct {
	ID       types.String `tfsdk:"id"`
	Roles    []string     `tfsdk:"roles"`
	Username types.String `tfsdk:"username"`
}

func (r *userRolesResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user_roles"
}

func (r *userRolesResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow user roles management. Note this resource is not supported on Airflow v3 (API v2): the User Roles API is not available in Airflow v3.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The username.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"roles": schema.SetAttribute{
				MarkdownDescription: "A set of User roles to attach to the User.",
				Required:            true,
				ElementType:         types.StringType,
				Validators: []validator.Set{
					setvalidator.SizeAtLeast(1),
				},
			},
			"username": schema.StringAttribute{
				MarkdownDescription: "The username.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
		},
	}
}

func (r *userRolesResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *userRolesResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan userRolesResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	username := plan.Username.ValueString()
	if err := r.patchRoles(username, expandUserRoles(plan.Roles)); err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow user roles", err.Error())
		return
	}

	plan.ID = types.StringValue(username)
	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow user after assigning roles", fmt.Sprintf("user %q not found immediately after creation", username))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *userRolesResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state userRolesResourceModel
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

func (r *userRolesResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan userRolesResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	username := plan.ID.ValueString()
	if err := r.patchRoles(username, expandUserRoles(plan.Roles)); err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow user roles", err.Error())
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

func (r *userRolesResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state userRolesResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	username := state.ID.ValueString()
	// Mirror the SDKv2 resource: clear the user's roles, then delete the user.
	_ = r.patchRoles(username, []airflow.UserCollectionItemRoles{})

	httpResp, err := r.config.ApiClient.UserApi.DeleteUser(r.config.AuthContext, username).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow user", clientError("delete", username, httpResp, err))
	}
}

func (r *userRolesResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// patchRoles sets the user's roles via a roles-only update mask, matching the
// SDKv2 resource which fills the required name fields with the username.
func (r *userRolesResource) patchRoles(username string, roles []airflow.UserCollectionItemRoles) error {
	_, httpResp, err := r.config.ApiClient.UserApi.PatchUser(r.config.AuthContext, username).
		UpdateMask([]string{"roles"}).
		User(airflow.User{
			Roles:     roles,
			Username:  &username,
			FirstName: &username,
			LastName:  &username,
			Email:     &username,
		}).Execute()
	if err != nil {
		return fmt.Errorf("%s", clientError("update roles for user", username, httpResp, err))
	}
	return nil
}

// readInto fetches the user identified by m.ID and populates m. It returns
// false (without adding diagnostics) when the user no longer exists.
func (r *userRolesResource) readInto(m *userRolesResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	user, httpResp, err := r.config.ApiClient.UserApi.GetUser(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow user", clientError("read", id, httpResp, err))
		return false
	}

	m.Username = types.StringValue(user.GetUsername())
	m.Roles = flattenUserRoles(user.Roles)
	return true
}
