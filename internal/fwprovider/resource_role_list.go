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
	_ list.ListResource              = &roleListResource{}
	_ list.ListResourceWithConfigure = &roleListResource{}
)

func newRoleListResource() list.ListResource {
	return &roleListResource{}
}

type roleListResource struct {
	config client.ProviderConfig
}

func (r *roleListResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role"
}

func (r *roleListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = listschema.Schema{
		MarkdownDescription: "Lists all Airflow roles. Use with `terraform query` (Terraform 1.14 and later) to enumerate existing roles. On Airflow 3 roles are read from the auth manager API (`auth_manager_base_path`).",
	}
}

func (r *roleListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *roleListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var httpResp *http.Response
	items, err := fetchAllPages(func(limit, offset int32) ([]airflow.Role, error) {
		collection, resp, err := r.config.AuthManagerApiClient.RoleApi.GetRoles(r.config.AuthManagerAuthContext).Limit(limit).Offset(offset).Execute()
		httpResp = resp
		if err != nil {
			return nil, err
		}
		return collection.GetRoles(), nil
	})
	if err != nil {
		var diags diag.Diagnostics
		diags.AddError("Failed to list Airflow roles", clientError("list", "roles", httpResp, err))
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	reader := &roleResource{config: r.config}
	stream.Results = func(push func(list.ListResult) bool) {
		for _, item := range items {
			id := types.StringValue(item.GetName())
			result := req.NewListResult(ctx)
			result.DisplayName = id.ValueString()
			result.Diagnostics.Append(result.Identity.Set(ctx, roleIdentity(id))...)

			if req.IncludeResource {
				m := roleResourceModel{ID: id}
				if found := reader.readInto(&m, &result.Diagnostics); found {
					// Match the resource: drop FAB's implicit homepage permission.
					m.Actions = withoutImplicitRoleActions(m.Actions, nil)
					result.Diagnostics.Append(result.Resource.Set(ctx, m)...)
				}
			}

			if !push(result) {
				return
			}
		}
	}
}
