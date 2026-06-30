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
	_ list.ListResource              = &variableListResource{}
	_ list.ListResourceWithConfigure = &variableListResource{}
)

func newVariableListResource() list.ListResource {
	return &variableListResource{}
}

type variableListResource struct {
	config client.ProviderConfig
}

func (r *variableListResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_variable"
}

func (r *variableListResource) ListResourceConfigSchema(_ context.Context, _ list.ListResourceSchemaRequest, resp *list.ListResourceSchemaResponse) {
	// No filter arguments: list all variables.
	resp.Schema = listschema.Schema{}
}

func (r *variableListResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *variableListResource) List(ctx context.Context, req list.ListRequest, stream *list.ListResultsStream) {
	collection, httpResp, err := r.config.ApiClient.VariableApi.GetVariables(r.config.AuthContext).Execute()
	if err != nil {
		var diags diag.Diagnostics
		diags.AddError("Failed to list Airflow variables", clientError("list", "variables", httpResp, err))
		stream.Results = list.ListResultsStreamDiagnostics(diags)
		return
	}

	variables := collection.GetVariables()
	stream.Results = func(push func(list.ListResult) bool) {
		for _, v := range variables {
			result := req.NewListResult(ctx)
			result.DisplayName = v.GetKey()

			result.Diagnostics.Append(result.Identity.Set(ctx, variableIdentityModel{ID: types.StringValue(v.GetKey())})...)

			// The list endpoint omits the variable value; fetch the full object
			// when Terraform asks for the resource state (e.g. config generation).
			if req.IncludeResource {
				full, fHTTP, fErr := r.config.ApiClient.VariableApi.GetVariable(r.config.AuthContext, v.GetKey()).Execute()
				if fErr != nil {
					result.Diagnostics.AddError("Failed to read Airflow variable", clientError("read", v.GetKey(), fHTTP, fErr))
				} else {
					result.Diagnostics.Append(result.Resource.Set(ctx, variableResourceModel{
						ID:          types.StringValue(full.GetKey()),
						Key:         types.StringValue(full.GetKey()),
						Value:       types.StringValue(full.GetValue()),
						Description: types.StringValue(full.GetDescription()),
					})...)
				}
			}

			if !push(result) {
				return
			}
		}
	}
}
