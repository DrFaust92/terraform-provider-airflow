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
	_ datasource.DataSource              = &variableDataSource{}
	_ datasource.DataSourceWithConfigure = &variableDataSource{}
)

func newVariableDataSource() datasource.DataSource {
	return &variableDataSource{}
}

type variableDataSource struct {
	config client.ProviderConfig
}

type variableDataSourceModel struct {
	ID          types.String `tfsdk:"id"`
	Key         types.String `tfsdk:"key"`
	Value       types.String `tfsdk:"value"`
	Description types.String `tfsdk:"description"`
	TeamName    types.String `tfsdk:"team_name"`
}

func (d *variableDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_variable"
}

func (d *variableDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Fetches an existing Airflow variable.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The variable key.",
				Computed:            true,
			},
			"key": schema.StringAttribute{
				MarkdownDescription: "The variable key.",
				Required:            true,
			},
			"value": schema.StringAttribute{
				MarkdownDescription: "The variable value.",
				Computed:            true,
			},
			"description": schema.StringAttribute{
				MarkdownDescription: "The variable description.",
				Computed:            true,
			},
			"team_name": schema.StringAttribute{
				MarkdownDescription: "Team name (Airflow 3 multi-team deployments).",
				Computed:            true,
			},
		},
	}
}

func (d *variableDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *variableDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data variableDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	key := data.Key.ValueString()
	variable, httpResp, err := d.config.ApiClient.VariableApi.GetVariable(d.config.AuthContext, key).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to read Airflow variable", clientError("read", key, httpResp, err))
		return
	}

	data.ID = types.StringValue(key)
	data.Key = types.StringValue(variable.GetKey())
	data.Value = types.StringValue(variable.GetValue())
	data.Description = types.StringValue(variable.GetDescription())
	data.TeamName = types.StringValue(variable.GetTeamName())

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
