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
	_ datasource.DataSource              = &connectionDataSource{}
	_ datasource.DataSourceWithConfigure = &connectionDataSource{}
)

func newConnectionDataSource() datasource.DataSource {
	return &connectionDataSource{}
}

type connectionDataSource struct {
	config client.ProviderConfig
}

type connectionDataSourceModel struct {
	ID           types.String `tfsdk:"id"`
	ConnectionID types.String `tfsdk:"connection_id"`
	ConnType     types.String `tfsdk:"conn_type"`
	Description  types.String `tfsdk:"description"`
	Host         types.String `tfsdk:"host"`
	Login        types.String `tfsdk:"login"`
	Schema       types.String `tfsdk:"schema"`
	Port         types.Int64  `tfsdk:"port"`
	Extra        types.String `tfsdk:"extra"`
	TeamName     types.String `tfsdk:"team_name"`
}

func (d *connectionDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_connection"
}

func (d *connectionDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Fetches an existing Airflow connection. The password is never returned by the API and is not exposed here.",
		Attributes: map[string]schema.Attribute{
			"id":            schema.StringAttribute{MarkdownDescription: "The connection ID.", Computed: true},
			"connection_id": schema.StringAttribute{MarkdownDescription: "The connection ID.", Required: true},
			"conn_type":     schema.StringAttribute{MarkdownDescription: "The connection type.", Computed: true},
			"description":   schema.StringAttribute{MarkdownDescription: "The connection description.", Computed: true},
			"host":          schema.StringAttribute{MarkdownDescription: "The connection host.", Computed: true},
			"login":         schema.StringAttribute{MarkdownDescription: "The connection login.", Computed: true},
			"schema":        schema.StringAttribute{MarkdownDescription: "The connection schema.", Computed: true},
			"port":          schema.Int64Attribute{MarkdownDescription: "The connection port.", Computed: true},
			"extra":         schema.StringAttribute{MarkdownDescription: "The connection extra field. Secret-like values may be masked by Airflow.", Computed: true},
			"team_name":     schema.StringAttribute{MarkdownDescription: "Team name (Airflow 3 multi-team deployments).", Computed: true},
		},
	}
}

func (d *connectionDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *connectionDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data connectionDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id := data.ConnectionID.ValueString()
	conn, httpResp, err := d.config.ApiClient.ConnectionApi.GetConnection(d.config.AuthContext, id).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to read Airflow connection", clientError("read", id, httpResp, err))
		return
	}

	data.ID = types.StringValue(id)
	data.ConnectionID = types.StringValue(conn.GetConnectionId())
	data.ConnType = types.StringValue(conn.GetConnType())
	data.Description = types.StringValue(conn.GetDescription())
	data.Host = types.StringValue(conn.GetHost())
	data.Login = types.StringValue(conn.GetLogin())
	data.Schema = types.StringValue(conn.GetSchema())
	data.Port = types.Int64Value(int64(conn.GetPort()))
	data.Extra = types.StringValue(conn.GetExtra())
	data.TeamName = types.StringValue(conn.GetTeamName())

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
