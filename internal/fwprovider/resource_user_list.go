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
	_ list.ListResource              = &userListResource{}
	_ list.ListResourceWithConfigure = &userListResource{}
)

func newUserListResource() list.ListResource {
	return &userListResource{}
}

type userListResource struct {
	config client.ProviderConfig
}

func (r *userListResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (r *userListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	resp.Schema = listschema.Schema{
		MarkdownDescription: "Lists all Airflow users. Use with `terraform query` (Terraform 1.14 and later) to enumerate existing users. On Airflow 3 users are read from the auth manager API (`auth_manager_base_path`). Passwords are never returned.",
	}
}

func (r *userListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *userListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	var httpResp *http.Response
	items, err := fetchAllPages(func(limit, offset int32) ([]airflow.UserCollectionItem, error) {
		collection, resp, err := r.config.AuthManagerApiClient.UserApi.GetUsers(r.config.AuthManagerAuthContext).Limit(limit).Offset(offset).Execute()
		httpResp = resp
		if err != nil {
			return nil, err
		}
		return collection.GetUsers(), nil
	})
	if err != nil {
		var diags diag.Diagnostics
		diags.AddError("Failed to list Airflow users", clientError("list", "users", httpResp, err))
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	reader := &userResource{config: r.config}
	stream.Results = func(push func(list.ListResult) bool) {
		for _, item := range items {
			id := types.StringValue(item.GetUsername())
			result := req.NewListResult(ctx)
			result.DisplayName = id.ValueString()
			result.Diagnostics.Append(result.Identity.Set(ctx, userIdentity(id))...)

			if req.IncludeResource {
				m := userResourceModel{ID: id}
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
