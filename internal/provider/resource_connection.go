package provider

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceConnection() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceConnectionCreate,
		ReadWithoutTimeout:   resourceConnectionRead,
		UpdateWithoutTimeout: resourceConnectionUpdate,
		DeleteWithoutTimeout: resourceConnectionDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"connection_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"conn_type": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"host": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"login": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"schema": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"port": {
				Type:         schema.TypeInt,
				Optional:     true,
				ValidateFunc: validation.IsPortNumberOrZero,
			},
			"password": {
				Type:          schema.TypeString,
				Optional:      true,
				Sensitive:     true,
				ConflictsWith: []string{"password_wo"},
			},
			"password_wo": {
				Type:          schema.TypeString,
				Optional:      true,
				WriteOnly:     true,
				ConflictsWith: []string{"password"},
				RequiredWith:  []string{"password_wo_version"},
			},
			"password_wo_version": {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  `Triggers update of password_wo write-only. For more info see [updating write-only attributes](https://developer.hashicorp.com/terraform/language/manage-sensitive-data/write-only)`,
				RequiredWith: []string{"password_wo"},
			},
			"extra": {
				Type:             schema.TypeString,
				DiffSuppressFunc: suppressSameJsonDiff,
				ValidateFunc:     validation.StringIsJSON,
				Optional:         true,
				Sensitive:        true,
			},
		},
	}
}

func suppressSameJsonDiff(k, oldo, newo string, d *schema.ResourceData) bool {
	if strings.TrimSpace(oldo) == strings.TrimSpace(newo) {
		return true
	}

	var oldIface interface{}
	var newIface interface{}

	if err := json.Unmarshal([]byte(oldo), &oldIface); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(newo), &newIface); err != nil {
		return false
	}

	return reflect.DeepEqual(oldIface, newIface)
}

func resourceConnectionCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient
	connId := d.Get("connection_id").(string)
	connType := d.Get("conn_type").(string)

	conn := airflow.Connection{
		ConnectionId: &connId,
		ConnType:     &connType,
	}

	if v, ok := d.GetOk("host"); ok {
		conn.SetHost(v.(string))
	}

	if v, ok := d.GetOk("description"); ok {
		conn.SetDescription(v.(string))
	}

	if v, ok := d.GetOk("login"); ok {
		conn.SetLogin(v.(string))
	}

	if v, ok := d.GetOk("schema"); ok {
		conn.SetSchema(v.(string))
	}

	if v, ok := d.GetOk("port"); ok {
		conn.SetPort(int32(v.(int)))
	}

	if v, ok := d.GetOk("password"); ok {
		conn.SetPassword(v.(string))
	} else if v, ok := d.GetOk("password_wo"); ok {
		conn.SetPassword(v.(string))
	}

	if v, ok := d.GetOk("extra"); ok {
		conn.SetExtra(v.(string))
	}

	connApi := client.ConnectionApi

	_, _, err := connApi.PostConnection(pcfg.AuthContext).Connection(conn).Execute()
	if err != nil {
		return diag.Errorf("failed to create connection `%s` from Airflow: %s", connId, err)
	}
	d.SetId(connId)

	return resourceConnectionRead(ctx, d, m)
}

func resourceConnectionRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient
	connection, resp, err := client.ConnectionApi.GetConnection(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get connection `%s` from Airflow: %s", d.Id(), err)
	}

	if err := d.Set("connection_id", connection.GetConnectionId()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("conn_type", connection.GetConnType()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("host", connection.GetHost()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("login", connection.GetLogin()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("schema", connection.GetSchema()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("port", connection.GetPort()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("extra", connection.GetExtra()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("description", connection.GetDescription()); err != nil {
		return diag.FromErr(err)
	}

	if v, ok := connection.GetPasswordOk(); ok {
		if err := d.Set("password", v); err != nil {
			return diag.FromErr(err)
		}
	} else if v, ok := d.GetOk("password"); ok {
		if err := d.Set("password", v); err != nil {
			return diag.FromErr(err)
		}
	}

	return nil
}

func resourceConnectionUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient
	connId := d.Id()
	connType := d.Get("conn_type").(string)

	conn := airflow.Connection{
		ConnectionId: &connId,
		ConnType:     &connType,
	}

	if v, ok := d.GetOk("host"); ok {
		conn.SetHost(v.(string))
	} else {
		conn.SetHostNil()
	}

	if v, ok := d.GetOk("description"); ok {
		conn.SetDescription(v.(string))
	} else {
		conn.SetDescriptionNil()
	}

	if v, ok := d.GetOk("login"); ok {
		conn.SetLogin(v.(string))
	} else {
		conn.SetLoginNil()
	}

	if v, ok := d.GetOk("schema"); ok {
		conn.SetSchema(v.(string))
	} else {
		conn.SetSchemaNil()
	}

	if v, ok := d.GetOk("port"); ok {
		conn.SetPort(int32(v.(int)))
	} else {
		conn.SetPortNil()
	}

	if v, ok := d.GetOk("password"); ok && v.(string) != "" {
		conn.SetPassword(v.(string))
	} else if v, ok := d.GetOk("password_wo"); ok && v.(string) != "" {
		conn.SetPassword(v.(string))
	}

	if v, ok := d.GetOk("extra"); ok {
		conn.SetExtra(v.(string))
	} else {
		conn.SetExtraNil()
	}

	_, _, err := client.ConnectionApi.PatchConnection(pcfg.AuthContext, connId).Connection(conn).Execute()
	if err != nil {
		return diag.Errorf("failed to update connection `%s` from Airflow: %s", connId, err)
	}

	return resourceConnectionRead(ctx, d, m)
}

func resourceConnectionDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	resp, err := client.ConnectionApi.DeleteConnection(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete connection `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}
