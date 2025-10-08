package provider

import (
	"context"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceUser() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceUserCreate,
		ReadWithoutTimeout:   resourceUserRead,
		UpdateWithoutTimeout: resourceUserUpdate,
		DeleteWithoutTimeout: resourceUserDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"active": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"email": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"failed_login_count": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"first_name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"last_name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"login_count": {
				Type:     schema.TypeString,
				Computed: true,
			},
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
			"password": {
				Type:      schema.TypeString,
				Required:  true,
				Sensitive: true,
			},
		},
	}
}

func resourceUserCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	email := d.Get("email").(string)
	firstName := d.Get("first_name").(string)
	lastName := d.Get("last_name").(string)
	username := d.Get("username").(string)
	password := d.Get("password").(string)
	roles := expandAirflowUserRoles(d.Get("roles").(*schema.Set))

	userApi := client.UserApi

	_, _, err := userApi.PostUser(pcfg.AuthContext).User(airflow.User{
		Email:     &email,
		FirstName: &firstName,
		LastName:  &lastName,
		Username:  &username,
		Password:  &password,
		Roles:     &roles,
	}).Execute()
	if err != nil {
		return diag.Errorf("failed to create user `%s` from Airflow: %s", email, err)
	}
	d.SetId(username)

	return resourceUserRead(ctx, d, m)
}

func resourceUserRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
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

	if err := d.Set("active", user.GetActive()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("email", user.Email); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("failed_login_count", user.GetFailedLoginCount()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("first_name", user.FirstName); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("last_name", user.LastName); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("login_count", user.GetLastLogin()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("username", user.Username); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("password", d.Get("password").(string)); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("roles", flattenAirflowUserRoles(*user.Roles)); err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func resourceUserUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	email := d.Get("email").(string)
	firstName := d.Get("first_name").(string)
	lastName := d.Get("last_name").(string)
	password := d.Get("password").(string)
	roles := expandAirflowUserRoles(d.Get("roles").(*schema.Set))
	username := d.Id()

	_, _, err := client.UserApi.PatchUser(pcfg.AuthContext, username).User(airflow.User{
		Email:     &email,
		FirstName: &firstName,
		LastName:  &lastName,
		Password:  &password,
		Roles:     &roles,
		Username:  &username,
	}).Execute()
	if err != nil {
		return diag.Errorf("failed to update user `%s` from Airflow: %s", email, err)
	}

	return resourceUserRead(ctx, d, m)
}

func resourceUserDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	resp, err := client.UserApi.DeleteUser(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete user `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}

func expandAirflowUserRoles(tfList *schema.Set) []airflow.UserCollectionItemRoles {
	if tfList.Len() == 0 {
		return nil
	}

	apiObjects := make([]airflow.UserCollectionItemRoles, 0)

	for _, tfMapRaw := range tfList.List() {
		val, ok := tfMapRaw.(string)

		if !ok {
			continue
		}

		apiObject := airflow.UserCollectionItemRoles{
			Name: &val,
		}
		apiObjects = append(apiObjects, apiObject)
	}

	return apiObjects
}

func flattenAirflowUserRoles(apiObjects []airflow.UserCollectionItemRoles) []string {
	vs := make([]string, 0, len(apiObjects))
	for _, v := range apiObjects {
		name := *v.Name
		vs = append(vs, name)
	}
	return vs
}
