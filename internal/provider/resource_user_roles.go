package provider

import (
	"context"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceUserRoles() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceUserRolesCreate,
		ReadWithoutTimeout:   resourceUserRolesRead,
		UpdateWithoutTimeout: resourceUserRolesUpdate,
		DeleteWithoutTimeout: resourceUserRolesDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"roles": {
				Type:     schema.TypeSet,
				Required: true,
				MinItems: 1,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"username": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
		},
	}
}

func resourceUserRolesCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	username := d.Get("username").(string)
	roles := expandAirflowUserRoles(d.Get("roles").(*schema.Set))

	userApi := client.UserApi

	_, _, err := userApi.PatchUser(pcfg.AuthContext, username).UpdateMask([]string{"roles"}).User(airflow.User{
		Roles:     &roles,
		Username:  &username,
		FirstName: &username,
		LastName:  &username,
		Email:     &username,
	}).Execute()
	if err != nil {
		return diag.Errorf("failed to create user `%s` from Airflow: %s", username, err)
	}
	d.SetId(username)

	return resourceUserRolesRead(ctx, d, m)
}

func resourceUserRolesRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	user, resp, err := client.UserApi.GetUser(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get user `%s` from Airflow: %s", d.Id(), err)
	}

	d.Set("username", user.Username)
	d.Set("roles", flattenAirflowUserRoles(*user.Roles))

	return nil
}

func resourceUserRolesUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	roles := expandAirflowUserRoles(d.Get("roles").(*schema.Set))
	username := d.Id()

	_, _, err := client.UserApi.PatchUser(pcfg.AuthContext, username).UpdateMask([]string{"roles"}).User(airflow.User{
		Roles:     &roles,
		Username:  &username,
		FirstName: &username,
		LastName:  &username,
		Email:     &username,
	}).Execute()
	if err != nil {
		return diag.Errorf("failed to update user `%s` from Airflow: %s", username, err)
	}

	return resourceUserRolesRead(ctx, d, m)
}

func resourceUserRolesDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	roles := make([]airflow.UserCollectionItemRoles, 0)
	username := d.Id()

	var err error
	_, _, _ = client.UserApi.PatchUser(pcfg.AuthContext, username).UpdateMask([]string{"roles"}).User(airflow.User{
		Roles:     &roles,
		Username:  &username,
		FirstName: &username,
		LastName:  &username,
		Email:     &username,
	}).Execute()

	resp, err := client.UserApi.DeleteUser(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete user `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}
