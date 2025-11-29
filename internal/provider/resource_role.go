package provider

import (
	"context"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceRole() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceRoleCreate,
		ReadWithoutTimeout:   resourceRoleRead,
		UpdateWithoutTimeout: resourceRoleUpdate,
		DeleteWithoutTimeout: resourceRoleDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"action": {
				Type:     schema.TypeSet,
				Required: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"action": {
							Type:     schema.TypeString,
							Required: true,
						},
						"resource": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}

func resourceRoleCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	name := d.Get("name").(string)
	varApi := client.RoleApi
	role := airflow.Role{
		Name:    &name,
		Actions: expandAirflowRoleActions(d.Get("action").(*schema.Set).List()),
	}

	_, _, err := varApi.PostRole(pcfg.AuthContext).Role(role).Execute()
	if err != nil {
		return diag.Errorf("failed to create role `%s` from Airflow: %s", name, err)
	}
	d.SetId(name)

	return resourceRoleRead(ctx, d, m)
}

func resourceRoleRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	role, resp, err := client.RoleApi.GetRole(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get role `%s` from Airflow: %s", d.Id(), err)
	}

	if err := d.Set("name", role.Name); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("action", flattenAirflowRoleActions(role.Actions)); err != nil {
		return diag.Errorf("error setting action: %s", err)
	}

	return nil
}

func resourceRoleUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	name := d.Id()
	actions := expandAirflowRoleActions(d.Get("action").(*schema.Set).List())
	role := airflow.Role{
		Name:    &name,
		Actions: actions,
	}

	_, _, err := client.RoleApi.PatchRole(pcfg.AuthContext, name).Role(role).Execute()
	if err != nil {
		return diag.Errorf("failed to update role `%s` from Airflow: %s", name, err)
	}

	return resourceRoleRead(ctx, d, m)
}

func resourceRoleDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	resp, err := client.RoleApi.DeleteRole(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete role `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}

func expandAirflowRoleActions(tfList []interface{}) []airflow.ActionResource {
	if len(tfList) == 0 {
		return nil
	}

	apiObjects := make([]airflow.ActionResource, 0)

	for _, tfMapRaw := range tfList {
		tfMap, ok := tfMapRaw.(map[string]interface{})

		if !ok {
			continue
		}

		action := tfMap["action"].(string)
		resource := tfMap["resource"].(string)

		apiObject := airflow.ActionResource{
			Action: &airflow.Action{
				Name: &action,
			},
			Resource: &airflow.Resource{
				Name: &resource,
			},
		}
		apiObjects = append(apiObjects, apiObject)
	}

	return apiObjects
}

func flattenAirflowRoleActions(apiObjects []airflow.ActionResource) []interface{} {
	if len(apiObjects) == 0 {
		return nil
	}

	var tfList []interface{}

	for _, apiObject := range apiObjects {
		tfList = append(tfList, map[string]interface{}{
			"action":   apiObject.Action.Name,
			"resource": apiObject.Resource.Name,
		})
	}

	return tfList
}
