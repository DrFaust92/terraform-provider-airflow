package fwprovider

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &poolResource{}
	_ resource.ResourceWithConfigure   = &poolResource{}
	_ resource.ResourceWithImportState = &poolResource{}
)

func newPoolResource() resource.Resource {
	return &poolResource{}
}

type poolResource struct {
	config provider.ProviderConfig
}

type poolResourceModel struct {
	ID              types.String `tfsdk:"id"`
	Name            types.String `tfsdk:"name"`
	Slots           types.Int64  `tfsdk:"slots"`
	Description     types.String `tfsdk:"description"`
	IncludeDeferred types.Bool   `tfsdk:"include_deferred"`
	OccupiedSlots   types.Int64  `tfsdk:"occupied_slots"`
	QueuedSlots     types.Int64  `tfsdk:"queued_slots"`
	OpenSlots       types.Int64  `tfsdk:"open_slots"`
	RunningSlots    types.Int64  `tfsdk:"running_slots"`
	DeferredSlots   types.Int64  `tfsdk:"deferred_slots"`
	ScheduledSlots  types.Int64  `tfsdk:"scheduled_slots"`
}

func (r *poolResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_pool"
}

func (r *poolResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Required: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"slots": schema.Int64Attribute{
				Required: true,
			},
			"description": schema.StringAttribute{
				Optional: true,
				Computed: true,
			},
			"include_deferred": schema.BoolAttribute{
				Optional: true,
				Computed: true,
				Default:  booldefault.StaticBool(false),
			},
			"occupied_slots":  schema.Int64Attribute{Computed: true},
			"queued_slots":    schema.Int64Attribute{Computed: true},
			"open_slots":      schema.Int64Attribute{Computed: true},
			"running_slots":   schema.Int64Attribute{Computed: true},
			"deferred_slots":  schema.Int64Attribute{Computed: true},
			"scheduled_slots": schema.Int64Attribute{Computed: true},
		},
	}
}

func (r *poolResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *poolResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan poolResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := plan.Name.ValueString()
	slots := int32(plan.Slots.ValueInt64())
	includeDeferred := plan.IncludeDeferred.ValueBool()

	pool := airflow.Pool{
		Name:            &name,
		Slots:           &slots,
		IncludeDeferred: &includeDeferred,
	}
	if !plan.Description.IsNull() && !plan.Description.IsUnknown() {
		pool.SetDescription(plan.Description.ValueString())
	}

	_, httpResp, err := r.config.ApiClient.PoolApi.PostPool(r.config.AuthContext).Pool(pool).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow pool", clientError("create", name, httpResp, err))
		return
	}

	plan.ID = types.StringValue(name)
	if found := r.readInto(&plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow pool after create", fmt.Sprintf("pool %q not found immediately after creation", name))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *poolResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state poolResourceModel
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

func (r *poolResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan poolResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := plan.ID.ValueString()
	slots := int32(plan.Slots.ValueInt64())
	includeDeferred := plan.IncludeDeferred.ValueBool()

	pool := airflow.Pool{
		Name:            &name,
		Slots:           &slots,
		IncludeDeferred: &includeDeferred,
	}
	// Mirror the SDKv2 resource: an absent description is cleared on update.
	if !plan.Description.IsNull() && !plan.Description.IsUnknown() {
		pool.SetDescription(plan.Description.ValueString())
	} else {
		pool.SetDescriptionNil()
	}

	_, httpResp, err := r.config.ApiClient.PoolApi.PatchPool(r.config.AuthContext, name).Pool(pool).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to update Airflow pool", clientError("update", name, httpResp, err))
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

func (r *poolResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state poolResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := state.ID.ValueString()
	httpResp, err := r.config.ApiClient.PoolApi.DeletePool(r.config.AuthContext, name).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow pool", clientError("delete", name, httpResp, err))
	}
}

func (r *poolResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// readInto fetches the pool identified by m.ID and populates m. It returns
// false (without adding diagnostics) when the pool no longer exists.
func (r *poolResource) readInto(m *poolResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	pool, httpResp, err := r.config.ApiClient.PoolApi.GetPool(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow pool", clientError("read", id, httpResp, err))
		return false
	}

	m.Name = types.StringValue(pool.GetName())
	m.Slots = types.Int64Value(int64(pool.GetSlots()))
	m.IncludeDeferred = types.BoolValue(pool.GetIncludeDeferred())
	m.OccupiedSlots = types.Int64Value(int64(pool.GetOccupiedSlots()))
	m.QueuedSlots = types.Int64Value(int64(pool.GetQueuedSlots()))
	m.OpenSlots = types.Int64Value(int64(pool.GetOpenSlots()))
	m.RunningSlots = types.Int64Value(int64(pool.GetRunningSlots()))
	m.DeferredSlots = types.Int64Value(int64(pool.GetDeferredSlots()))
	m.ScheduledSlots = types.Int64Value(int64(pool.GetScheduledSlots()))

	if pool.Description.IsSet() && pool.Description.Get() != nil {
		m.Description = types.StringValue(*pool.Description.Get())
	} else {
		m.Description = types.StringNull()
	}

	return true
}
