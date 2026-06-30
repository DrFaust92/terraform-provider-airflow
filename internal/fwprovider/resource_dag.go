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
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &dagResource{}
	_ resource.ResourceWithConfigure   = &dagResource{}
	_ resource.ResourceWithImportState = &dagResource{}
)

func newDagResource() resource.Resource {
	return &dagResource{}
}

type dagResource struct {
	config client.ProviderConfig
}

type dagResourceModel struct {
	ID          types.String `tfsdk:"id"`
	DagID       types.String `tfsdk:"dag_id"`
	Description types.String `tfsdk:"description"`
	DeleteDag   types.Bool   `tfsdk:"delete_dag"`
	FileToken   types.String `tfsdk:"file_token"`
	Fileloc     types.String `tfsdk:"fileloc"`
	IsActive    types.Bool   `tfsdk:"is_active"`
	IsPaused    types.Bool   `tfsdk:"is_paused"`
	IsSubdag    types.Bool   `tfsdk:"is_subdag"`
	RootDagID   types.String `tfsdk:"root_dag_id"`
}

func (r *dagResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dag"
}

func (r *dagResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow DAG. This resource adopts an existing DAG and does not create one; on delete, the DAG is only removed from state and not actually deleted (unless `delete_dag` is set).",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The DAG ID.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"dag_id": schema.StringAttribute{
				MarkdownDescription: "The ID of the DAG.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"description": schema.StringAttribute{
				MarkdownDescription: "User-provided DAG description, which can consist of several sentences or paragraphs that describe DAG contents.",
				Computed:            true,
			},
			"delete_dag": schema.BoolAttribute{
				MarkdownDescription: "Whether to delete the DAG when deleted from Terraform.",
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(false),
			},
			"file_token": schema.StringAttribute{
				MarkdownDescription: "The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading a non-DAG file.",
				Computed:            true,
			},
			"fileloc": schema.StringAttribute{
				MarkdownDescription: "The absolute path to the file.",
				Computed:            true,
			},
			"is_active": schema.BoolAttribute{
				MarkdownDescription: "Whether the DAG is currently seen by the scheduler(s).",
				Computed:            true,
			},
			"is_paused": schema.BoolAttribute{
				MarkdownDescription: "Whether the DAG is paused.",
				Required:            true,
			},
			"is_subdag": schema.BoolAttribute{
				MarkdownDescription: "Whether the DAG is a SubDAG.",
				Computed:            true,
			},
			"root_dag_id": schema.StringAttribute{
				MarkdownDescription: "If the DAG is a SubDAG then it is the top level DAG identifier. Otherwise, null.",
				Computed:            true,
			},
		},
	}
}

func (r *dagResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// airflow_dag adopts an existing DAG, so Create and Update both patch it.
func (r *dagResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan dagResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	plan.ID = types.StringValue(plan.DagID.ValueString())
	r.apply(ctx, &plan, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *dagResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state dagResourceModel
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

func (r *dagResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan dagResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	r.apply(ctx, &plan, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *dagResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state dagResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Only actually delete the DAG when delete_dag is set; otherwise it is just
	// removed from state.
	if !state.DeleteDag.ValueBool() {
		return
	}

	id := state.ID.ValueString()
	httpResp, err := r.config.ApiClient.DAGApi.DeleteDag(r.config.AuthContext, id).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow DAG", clientError("delete", id, httpResp, err))
	}
}

func (r *dagResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// apply patches the DAG's is_paused flag and refreshes the model.
func (r *dagResource) apply(_ context.Context, m *dagResourceModel, diags *diag.Diagnostics) {
	dagID := m.DagID.ValueString()

	dag := *airflow.NewDAG()
	dag.SetIsPaused(m.IsPaused.ValueBool())

	_, httpResp, err := r.config.ApiClient.DAGApi.PatchDag(r.config.AuthContext, dagID).DAG(dag).Execute()
	if err != nil {
		diags.AddError("Failed to update Airflow DAG", clientError("update", dagID, httpResp, err))
		return
	}

	if found := r.readInto(m, diags); diags.HasError() {
		return
	} else if !found {
		diags.AddError("Failed to read Airflow DAG after update", fmt.Sprintf("DAG %q not found", dagID))
	}
}

// readInto fetches the DAG identified by m.ID and populates m (except delete_dag,
// which is Terraform-only). Returns false (without diagnostics) when the DAG no
// longer exists.
func (r *dagResource) readInto(m *dagResourceModel, diags *diag.Diagnostics) (found bool) {
	id := m.ID.ValueString()

	dag, httpResp, err := r.config.ApiClient.DAGApi.GetDag(r.config.AuthContext, id).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow DAG", clientError("read", id, httpResp, err))
		return false
	}

	m.DagID = types.StringValue(dag.GetDagId())
	m.IsPaused = types.BoolValue(derefBool(dag.IsPaused.Get()))
	m.IsActive = types.BoolValue(derefBool(dag.IsActive.Get()))
	m.IsSubdag = types.BoolValue(dag.GetIsSubdag())
	m.Description = types.StringValue(derefString(dag.Description.Get()))
	m.FileToken = types.StringValue(dag.GetFileToken())
	m.Fileloc = types.StringValue(dag.GetFileloc())
	m.RootDagID = types.StringValue(derefString(dag.RootDagId.Get()))
	return true
}

func derefBool(p *bool) bool {
	if p != nil {
		return *p
	}
	return false
}

func derefString(p *string) string {
	if p != nil {
		return *p
	}
	return ""
}
