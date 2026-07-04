package fwprovider

import (
	"context"
	"fmt"

	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = &poolDataSource{}
	_ datasource.DataSourceWithConfigure = &poolDataSource{}
)

func newPoolDataSource() datasource.DataSource {
	return &poolDataSource{}
}

type poolDataSource struct {
	config client.ProviderConfig
}

type poolDataSourceModel struct {
	ID              types.String `tfsdk:"id"`
	Name            types.String `tfsdk:"name"`
	Slots           types.Int64  `tfsdk:"slots"`
	Description     types.String `tfsdk:"description"`
	IncludeDeferred types.Bool   `tfsdk:"include_deferred"`
	TeamName        types.String `tfsdk:"team_name"`
	OccupiedSlots   types.Int64  `tfsdk:"occupied_slots"`
	QueuedSlots     types.Int64  `tfsdk:"queued_slots"`
	OpenSlots       types.Int64  `tfsdk:"open_slots"`
	RunningSlots    types.Int64  `tfsdk:"running_slots"`
	DeferredSlots   types.Int64  `tfsdk:"deferred_slots"`
	ScheduledSlots  types.Int64  `tfsdk:"scheduled_slots"`
}

func (d *poolDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_pool"
}

func (d *poolDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Fetches an existing Airflow pool.",
		Attributes: map[string]schema.Attribute{
			"id":               schema.StringAttribute{MarkdownDescription: "The pool name.", Computed: true},
			"name":             schema.StringAttribute{MarkdownDescription: "The pool name.", Required: true},
			"slots":            schema.Int64Attribute{MarkdownDescription: "The number of slots in the pool.", Computed: true},
			"description":      schema.StringAttribute{MarkdownDescription: "The pool description.", Computed: true},
			"include_deferred": schema.BoolAttribute{MarkdownDescription: "Whether deferred tasks are included when calculating open slots.", Computed: true},
			"team_name":        schema.StringAttribute{MarkdownDescription: "Team name (Airflow 3 multi-team deployments).", Computed: true},
			"occupied_slots":   schema.Int64Attribute{MarkdownDescription: "The number of slots used.", Computed: true},
			"queued_slots":     schema.Int64Attribute{MarkdownDescription: "The number of slots used by queued tasks.", Computed: true},
			"open_slots":       schema.Int64Attribute{MarkdownDescription: "The number of free slots.", Computed: true},
			"running_slots":    schema.Int64Attribute{MarkdownDescription: "The number of slots used by running tasks.", Computed: true},
			"deferred_slots":   schema.Int64Attribute{MarkdownDescription: "The number of slots used by deferred tasks.", Computed: true},
			"scheduled_slots":  schema.Int64Attribute{MarkdownDescription: "The number of slots used by scheduled tasks.", Computed: true},
		},
	}
}

func (d *poolDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	cfg, ok := req.ProviderData.(client.ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected client.ProviderConfig, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}
	d.config = cfg
}

func (d *poolDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data poolDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()
	pool, httpResp, err := d.config.ApiClient.PoolApi.GetPool(d.config.AuthContext, name).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to read Airflow pool", clientError("read", name, httpResp, err))
		return
	}

	data.ID = types.StringValue(name)
	data.Name = types.StringValue(pool.GetName())
	data.Slots = types.Int64Value(int64(pool.GetSlots()))
	data.Description = types.StringValue(pool.GetDescription())
	data.IncludeDeferred = types.BoolValue(pool.GetIncludeDeferred())
	data.TeamName = types.StringValue(pool.GetTeamName())
	data.OccupiedSlots = types.Int64Value(int64(pool.GetOccupiedSlots()))
	data.QueuedSlots = types.Int64Value(int64(pool.GetQueuedSlots()))
	data.OpenSlots = types.Int64Value(int64(pool.GetOpenSlots()))
	data.RunningSlots = types.Int64Value(int64(pool.GetRunningSlots()))
	data.DeferredSlots = types.Int64Value(int64(pool.GetDeferredSlots()))
	data.ScheduledSlots = types.Int64Value(int64(pool.GetScheduledSlots()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
