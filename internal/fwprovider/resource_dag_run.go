package fwprovider

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework-timeouts/resource/timeouts"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &dagRunResource{}
	_ resource.ResourceWithConfigure   = &dagRunResource{}
	_ resource.ResourceWithImportState = &dagRunResource{}
)

func newDagRunResource() resource.Resource {
	return &dagRunResource{}
}

type dagRunResource struct {
	config client.ProviderConfig
}

type dagRunResourceModel struct {
	ID       types.String   `tfsdk:"id"`
	DagID    types.String   `tfsdk:"dag_id"`
	DagRunID types.String   `tfsdk:"dag_run_id"`
	Conf     types.Map      `tfsdk:"conf"`
	State    types.String   `tfsdk:"state"`
	Timeouts timeouts.Value `tfsdk:"timeouts"`
}

func (r *dagRunResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dag_run"
}

func (r *dagRunResource) Schema(ctx context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Provides an Airflow dag run resource (triggers a DAG).",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "The DAG run identifier in the form `dag_id:dag_run_id`.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"dag_id": schema.StringAttribute{
				MarkdownDescription: "The DAG ID to run.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"dag_run_id": schema.StringAttribute{
				MarkdownDescription: "The DAG Run ID. If a value is not passed, a random one will be generated based on execution date.",
				Optional:            true,
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"conf": schema.MapAttribute{
				MarkdownDescription: "A map describing additional configuration parameters.",
				Optional:            true,
				Computed:            true,
				ElementType:         types.StringType,
				PlanModifiers: []planmodifier.Map{
					mapplanmodifier.RequiresReplace(),
					mapplanmodifier.UseStateForUnknown(),
				},
			},
			"state": schema.StringAttribute{
				MarkdownDescription: "The DAG state.",
				Computed:            true,
			},
		},
		Blocks: map[string]schema.Block{
			"timeouts": timeouts.Block(ctx, timeouts.Opts{Create: true}),
		},
	}
}

func (r *dagRunResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *dagRunResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan dagRunResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	createTimeout, diags := plan.Timeouts.Create(ctx, 10*time.Minute)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	dagID := plan.DagID.ValueString()
	dagRun := *airflow.NewDAGRunWithDefaults()
	if !plan.DagRunID.IsNull() && !plan.DagRunID.IsUnknown() {
		dagRun.SetDagRunId(plan.DagRunID.ValueString())
	}
	if conf := r.expandConf(ctx, plan.Conf, &resp.Diagnostics); conf != nil {
		dagRun.SetConf(conf)
	}
	if resp.Diagnostics.HasError() {
		return
	}

	res, httpResp, err := r.config.ApiClient.DAGRunApi.PostDagRun(r.config.AuthContext, dagID).DAGRun(dagRun).Execute()
	if err != nil {
		resp.Diagnostics.AddError("Failed to create Airflow DAG run", clientError("create", dagID, httpResp, err))
		return
	}

	id := fmt.Sprintf("%s:%s", dagID, res.GetDagRunId())
	plan.ID = types.StringValue(id)

	if !r.waitForRun(ctx, id, createTimeout, &resp.Diagnostics) {
		return
	}

	if found := r.readInto(ctx, &plan, &resp.Diagnostics); resp.Diagnostics.HasError() {
		return
	} else if !found {
		resp.Diagnostics.AddError("Failed to read Airflow DAG run after create", fmt.Sprintf("DAG run %q not found immediately after creation", id))
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *dagRunResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state dagRunResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	found := r.readInto(ctx, &state, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	if !found {
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update is unreachable in practice: every configurable attribute uses
// RequiresReplace, so any change forces a replace rather than an in-place
// update. It exists only to satisfy the resource.Resource interface.
func (r *dagRunResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan dagRunResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *dagRunResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state dagRunResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dagID, dagRunID, err := parseDagRunID(state.ID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Invalid DAG run ID", err.Error())
		return
	}

	httpResp, err := r.config.ApiClient.DAGRunApi.DeleteDagRun(r.config.AuthContext, dagID, dagRunID).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			return
		}
		resp.Diagnostics.AddError("Failed to delete Airflow DAG run", clientError("delete", state.ID.ValueString(), httpResp, err))
	}
}

func (r *dagRunResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// readInto fetches the DAG run identified by m.ID and populates m. Returns false
// (without diagnostics) when the DAG run no longer exists.
func (r *dagRunResource) readInto(ctx context.Context, m *dagRunResourceModel, diags *diag.Diagnostics) (found bool) {
	dagID, dagRunID, err := parseDagRunID(m.ID.ValueString())
	if err != nil {
		diags.AddError("Invalid DAG run ID", err.Error())
		return false
	}

	dagRun, httpResp, err := r.config.ApiClient.DAGRunApi.GetDagRun(r.config.AuthContext, dagID, dagRunID).Execute()
	if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
		return false
	}
	if err != nil {
		diags.AddError("Failed to read Airflow DAG run", clientError("read", m.ID.ValueString(), httpResp, err))
		return false
	}

	m.DagID = types.StringValue(dagRun.GetDagId())
	m.DagRunID = types.StringValue(dagRun.GetDagRunId())
	m.State = types.StringValue(string(dagRun.GetState()))

	conf := dagRun.GetConf()
	confMap := make(map[string]string, len(conf))
	for k, v := range conf {
		confMap[k] = fmt.Sprintf("%v", v)
	}
	confValue, d := types.MapValueFrom(ctx, types.StringType, confMap)
	diags.Append(d...)
	m.Conf = confValue

	return true
}

// waitForRun polls until the DAG run reaches the "success" state, mirroring the
// SDKv2 StateChangeConf (pending: queued/running/success, target: success).
func (r *dagRunResource) waitForRun(ctx context.Context, id string, timeout time.Duration, diags *diag.Diagnostics) bool {
	dagID, dagRunID, err := parseDagRunID(id)
	if err != nil {
		diags.AddError("Invalid DAG run ID", err.Error())
		return false
	}

	deadline := time.Now().Add(timeout)
	for {
		dagRun, _, err := r.config.ApiClient.DAGRunApi.GetDagRun(r.config.AuthContext, dagID, dagRunID).Execute()
		if err != nil {
			diags.AddError("Failed to poll Airflow DAG run", fmt.Sprintf("failed to get DAG run %q from Airflow: %s", id, err))
			return false
		}

		switch state := string(dagRun.GetState()); state {
		case "success":
			return true
		case "queued", "running":
			// keep waiting
		default:
			diags.AddError("Unexpected DAG run state", fmt.Sprintf("DAG run %q entered unexpected state %q while waiting for success", id, state))
			return false
		}

		if time.Now().After(deadline) {
			diags.AddError("Timed out waiting for DAG run", fmt.Sprintf("DAG run %q did not finish within %s", id, timeout))
			return false
		}

		select {
		case <-ctx.Done():
			diags.AddError("Cancelled waiting for DAG run", ctx.Err().Error())
			return false
		case <-time.After(5 * time.Second):
		}
	}
}

func (r *dagRunResource) expandConf(ctx context.Context, m types.Map, diags *diag.Diagnostics) map[string]interface{} {
	if m.IsNull() || m.IsUnknown() {
		return nil
	}

	elements := make(map[string]string, len(m.Elements()))
	diags.Append(m.ElementsAs(ctx, &elements, false)...)
	if diags.HasError() {
		return nil
	}

	conf := make(map[string]interface{}, len(elements))
	for k, v := range elements {
		conf[k] = v
	}
	return conf
}

func parseDagRunID(id string) (string, string, error) {
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("unexpected format of ID (%s), expected DAG-ID:DAG-RUN-ID", id)
	}
	return parts[0], parts[1], nil
}
