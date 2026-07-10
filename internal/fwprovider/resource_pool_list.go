package fwprovider

import (
	"context"
	"fmt"

	"github.com/drfaust92/terraform-provider-airflow/internal/client"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/list"
	listschema "github.com/hashicorp/terraform-plugin-framework/list/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ list.ListResource              = &poolListResource{}
	_ list.ListResourceWithConfigure = &poolListResource{}
)

func newPoolListResource() list.ListResource {
	return &poolListResource{}
}

type poolListResource struct {
	config client.ProviderConfig
}

func (r *poolListResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_pool"
}

func (r *poolListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = listschema.Schema{
		MarkdownDescription: "Lists all Airflow pools. Use with `terraform query` (Terraform 1.14 and later) to enumerate existing pools.",
	}
}

func (r *poolListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *poolListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	collection, httpResp, err := r.config.ApiClient.PoolApi.GetPools(r.config.AuthContext).Execute()
	if err != nil {
		var diags diag.Diagnostics
		diags.AddError("Failed to list Airflow pools", clientError("list", "pools", httpResp, err))
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	pools := collection.GetPools()
	stream.Results = func(push func(list.ListResult) bool) {
		for _, p := range pools {
			result := req.NewListResult(ctx)
			result.DisplayName = p.GetName()
			result.Diagnostics.Append(result.Identity.Set(ctx, poolIdentityModel{ID: types.StringValue(p.GetName())})...)

			if req.IncludeResource {
				m := poolResourceModel{
					ID:              types.StringValue(p.GetName()),
					Name:            types.StringValue(p.GetName()),
					Slots:           types.Int64Value(int64(p.GetSlots())),
					IncludeDeferred: types.BoolValue(p.GetIncludeDeferred()),
					OccupiedSlots:   types.Int64Value(int64(p.GetOccupiedSlots())),
					QueuedSlots:     types.Int64Value(int64(p.GetQueuedSlots())),
					OpenSlots:       types.Int64Value(int64(p.GetOpenSlots())),
					RunningSlots:    types.Int64Value(int64(p.GetRunningSlots())),
					DeferredSlots:   types.Int64Value(int64(p.GetDeferredSlots())),
					ScheduledSlots:  types.Int64Value(int64(p.GetScheduledSlots())),
				}
				if p.Description.IsSet() && p.Description.Get() != nil {
					m.Description = types.StringValue(*p.Description.Get())
				} else {
					m.Description = types.StringNull()
				}
				result.Diagnostics.Append(result.Resource.Set(ctx, m)...)
			}

			if !push(result) {
				return
			}
		}
	}
}
