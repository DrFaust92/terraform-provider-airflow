package fwprovider

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/client"
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
	_ resource.Resource                = &userResource{}
	_ resource.ResourceWithConfigure   = &userResource{}
	_ resource.ResourceWithImportState = &userResource{}
)

func newUserResource() resource.Resource {
	return &userResource{}
}

type userResource struct {
	config client.ProviderConfig
}

type userResourceModel struct {
	ID               types.String `tfsdk:"id"`
	Active           types.Bool   `tfsdk:"active"`
	Email            types.String `tfsdk:"email"`
	FailedLoginCount types.Int64  `tfsdk:"failed_login_count"`
	FirstName        types.String `tfsdk:"first_name"`
	LastName         types.String `tfsdk:"last_name"`
	LoginCount       types.String `tfsdk:"login_count"`
	Roles            []string     `tfsdk:"roles"`
	Username         types.String `tfsdk:"username"`
	Password         types.String `tfsdk:"password"`
}

func (r *userResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (r *userResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow user. Note this resource is not supported on Airflow v3 (API v2): the Users API is not available in Airflow v3.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The username.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"active": schema.BoolAttribute{
				MarkdownDescription: "Whether the user is active.",
				Computed:            true,
			},
			"email": schema.StringAttribute{
				MarkdownDescription: "The user's email.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"failed_login_count": schema.Int64Attribute{
				MarkdownDescription: "The number of times the login failed.",
				Computed:            true,
			},
			"first_name": schema.StringAttribute{
				MarkdownDescription: "The user firstname.",
				Required:            true,
			},
			"last_name": schema.StringAttribute{
				MarkdownDescription: "The user lastname.",
				Required:            true,
			},
			"login_count": schema.StringAttribute{
				MarkdownDescription: "The login count.",
				Computed:            true,
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
			"password": schema.StringAttribute{
				MarkdownDescription: "The user password.",
				Required:            true,
				Sensitive:           true,
			},
		},
	}
}

func (r *userResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *userResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan userResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	email := plan.Email.ValueString()
	firstName := plan.FirstName.ValueString()
	lastName := plan.LastName.ValueString()
	username := plan.Username.ValueString()
	password := plan.Password.ValueString()

	_, httpResp, err := r.config.ApiClient.UserApi.PostUser(r.config.AuthContext).User(airflow.User{
		Email:     &email,
		FirstName: &firstName,
		LastName:  &lastName,
		Username:  &username,
		Password:  &password,
		Roles:     expandUserRoles(plan.Roles),
	}).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow user", clientError("create", email, httpResp, err))
		return
	}

	plan.ID = types.StringValue(username)
	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow user after create", fmt.Sprintf("user %q not found immediately after creation", username))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *userResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state userResourceModel
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

func (r *userResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan userResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	email := plan.Email.ValueString()
	firstName := plan.FirstName.ValueString()
	lastName := plan.LastName.ValueString()
	password := plan.Password.ValueString()
	username := plan.ID.ValueString()

	_, httpResp, err := r.config.ApiClient.UserApi.PatchUser(r.config.AuthContext, username).User(airflow.User{
		Email:     &email,
		FirstName: &firstName,
		LastName:  &lastName,
		Password:  &password,
		Roles:     expandUserRoles(plan.Roles),
		Username:  &username,
	}).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow user", clientError("update", email, httpResp, err))
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

func (r *userResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state userResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	username := state.ID.ValueString()
	httpResp, err := r.config.ApiClient.UserApi.DeleteUser(r.config.AuthContext, username).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow user", clientError("delete", username, httpResp, err))
	}
}

func (r *userResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// readInto fetches the user identified by m.ID and populates m. The password is
// never returned by the API, so the existing model value is preserved. Returns
// false (without adding diagnostics) when the user no longer exists.
func (r *userResource) readInto(m *userResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	user, httpResp, err := r.config.ApiClient.UserApi.GetUser(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow user", clientError("read", id, httpResp, err))
		return false
	}

	m.Active = types.BoolValue(user.GetActive())
	m.Email = types.StringValue(user.GetEmail())
	m.FailedLoginCount = types.Int64Value(int64(user.GetFailedLoginCount()))
	m.FirstName = types.StringValue(user.GetFirstName())
	m.LastName = types.StringValue(user.GetLastName())
	m.LoginCount = types.StringValue(user.GetLastLogin())
	m.Username = types.StringValue(user.GetUsername())
	m.Roles = flattenUserRoles(user.Roles)
	// password is intentionally not set: the API does not return it.

	return true
}

func expandUserRoles(roles []string) []airflow.UserCollectionItemRoles {
	if len(roles) == 0 {
		return nil
	}

	apiObjects := make([]airflow.UserCollectionItemRoles, 0, len(roles))
	for _, name := range roles {
		n := name
		apiObjects = append(apiObjects, airflow.UserCollectionItemRoles{Name: &n})
	}

	return apiObjects
}

func flattenUserRoles(apiObjects []airflow.UserCollectionItemRoles) []string {
	roles := make([]string, 0, len(apiObjects))
	for _, v := range apiObjects {
		roles = append(roles, v.GetName())
	}

	return roles
}
