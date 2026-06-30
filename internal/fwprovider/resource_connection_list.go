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
	_ list.ListResource              = &connectionListResource{}
	_ list.ListResourceWithConfigure = &connectionListResource{}
)

func newConnectionListResource() list.ListResource {
	return &connectionListResource{}
}

type connectionListResource struct {
	config client.ProviderConfig
}

func (r *connectionListResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_connection"
}

func (r *connectionListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = listschema.Schema{}
}

func (r *connectionListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *connectionListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	collection, httpResp, err := r.config.ApiClient.ConnectionApi.GetConnections(r.config.AuthContext).Execute()
	if err != nil {
		var diags diag.Diagnostics
		diags.AddError("Failed to list Airflow connections", clientError("list", "connections", httpResp, err))
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	connections := collection.GetConnections()
	stream.Results = func(push func(list.ListResult) bool) {
		for _, c := range connections {
			result := req.NewListResult(ctx)
			result.DisplayName = c.GetConnectionId()
			result.Diagnostics.Append(result.Identity.Set(ctx, connectionIdentityModel{ID: types.StringValue(c.GetConnectionId())})...)

			if req.IncludeResource {
				// password / password_wo / extra are not returned by the list
				// endpoint (sensitive), so they are left null.
				m := connectionResourceModel{
					ID:           types.StringValue(c.GetConnectionId()),
					ConnectionID: types.StringValue(c.GetConnectionId()),
					ConnType:     types.StringValue(c.GetConnType()),
				}
				setListOptionalString(&m.Description, c.GetDescription())
				setListOptionalString(&m.Host, c.GetHost())
				setListOptionalString(&m.Login, c.GetLogin())
				setListOptionalString(&m.Schema, c.GetSchema())
				if p := c.GetPort(); p != 0 {
					m.Port = types.Int64Value(int64(p))
				}
				result.Diagnostics.Append(result.Resource.Set(ctx, m)...)
			}

			if !push(result) {
				return
			}
		}
	}
}

// setListOptionalString sets an optional string from a list item, leaving it
// null when the API returns an empty value.
func setListOptionalString(dst *types.String, v string) {
	if v != "" {
		*dst = types.StringValue(v)
	} else {
		*dst = types.StringNull()
	}
}
