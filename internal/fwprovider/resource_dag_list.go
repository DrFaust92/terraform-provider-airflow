package fwprovider

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/list"
	listschema "github.com/hashicorp/terraform-plugin-framework/list/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ list.ListResource              = &dagListResource{}
	_ list.ListResourceWithConfigure = &dagListResource{}
)

func newDagListResource() list.ListResource {
	return &dagListResource{}
}

type dagListResource struct {
	config client.ProviderConfig
}

func (r *dagListResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_dag"
}

func (r *dagListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = listschema.Schema{
		MarkdownDescription: "Lists all Airflow DAGs. Use with `terraform query` (Terraform 1.14 and later) to enumerate existing DAGs.",
	}
}

func (r *dagListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	cfg, ok := req.ProviderData.(client.ProviderConfig)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type", fmt.Sprintf("Expected client.ProviderConfig, got: %T.", req.ProviderData))
		return
	}
	r.config = cfg
}

func (r *dagListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var httpResp *http.Response
	items, err := fetchAllPages(func(limit, offset int32) ([]airflow.DAG, error) {
		collection, resp, err := r.config.ApiClient.DAGApi.GetDags(r.config.AuthContext).Limit(limit).Offset(offset).Execute()
		httpResp = resp
		if err != nil {
			return nil, err
		}
		return collection.GetDags(), nil
	})
	if err != nil {
		var diags diag.Diagnostics
		diags.AddError("Failed to list Airflow DAGs", clientError("list", "DAGs", httpResp, err))
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	reader := &dagResource{config: r.config}
	stream.Results = func(push func(list.ListResult) bool) {
		for _, item := range items {
			id := types.StringValue(item.GetDagId())
			result := req.NewListResult(ctx)
			result.DisplayName = id.ValueString()
			result.Diagnostics.Append(result.Identity.Set(ctx, dagIdentity(id))...)

			if req.IncludeResource {
				m := dagResourceModel{ID: id, DeleteDag: types.BoolValue(false)}
				if found := reader.readInto(&m, &result.Diagnostics); found {
					result.Diagnostics.Append(result.Resource.Set(ctx, m)...)
				}
			}

			if !push(result) {
				return
			}
		}
	}
}
