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
	_ datasource.DataSource              = &dagDataSource{}
	_ datasource.DataSourceWithConfigure = &dagDataSource{}
)

func newDagDataSource() datasource.DataSource {
	return &dagDataSource{}
}

type dagDataSource struct {
	config client.ProviderConfig
}

type dagDataSourceModel struct {
	ID          types.String `tfsdk:"id"`
	DagID       types.String `tfsdk:"dag_id"`
	Description types.String `tfsdk:"description"`
	FileToken   types.String `tfsdk:"file_token"`
	Fileloc     types.String `tfsdk:"fileloc"`
	IsActive    types.Bool   `tfsdk:"is_active"`
	IsPaused    types.Bool   `tfsdk:"is_paused"`
	IsSubdag    types.Bool   `tfsdk:"is_subdag"`
	RootDagID   types.String `tfsdk:"root_dag_id"`
}

func (d *dagDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dag"
}

func (d *dagDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Fetches an existing Airflow DAG.",
		Attributes: map[string]schema.Attribute{
			"id":          schema.StringAttribute{MarkdownDescription: "The DAG ID.", Computed: true},
			"dag_id":      schema.StringAttribute{MarkdownDescription: "The DAG ID.", Required: true},
			"description": schema.StringAttribute{MarkdownDescription: "The DAG description.", Computed: true},
			"file_token":  schema.StringAttribute{MarkdownDescription: "The DAG file token.", Computed: true},
			"fileloc":     schema.StringAttribute{MarkdownDescription: "The DAG file location.", Computed: true},
			"is_active":   schema.BoolAttribute{MarkdownDescription: "Whether the DAG is active.", Computed: true},
			"is_paused":   schema.BoolAttribute{MarkdownDescription: "Whether the DAG is paused.", Computed: true},
			"is_subdag":   schema.BoolAttribute{MarkdownDescription: "Whether the DAG is a subdag.", Computed: true},
			"root_dag_id": schema.StringAttribute{MarkdownDescription: "The root DAG ID (for subdags).", Computed: true},
		},
	}
}

func (d *dagDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *dagDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data dagDataSourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	id := data.DagID.ValueString()
	dag, httpResp, err := d.config.ApiClient.DAGApi.GetDag(d.config.AuthContext, id).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to read Airflow DAG", clientError("read", id, httpResp, err))
		return
	}

	data.ID = types.StringValue(id)
	data.DagID = types.StringValue(dag.GetDagId())
	data.Description = types.StringValue(derefString(dag.Description.Get()))
	data.FileToken = types.StringValue(dag.GetFileToken())
	data.Fileloc = types.StringValue(dag.GetFileloc())
	data.IsActive = types.BoolValue(derefBool(dag.IsActive.Get()))
	data.IsPaused = types.BoolValue(derefBool(dag.IsPaused.Get()))
	data.IsSubdag = types.BoolValue(dag.GetIsSubdag())
	data.RootDagID = types.StringValue(derefString(dag.RootDagId.Get()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
