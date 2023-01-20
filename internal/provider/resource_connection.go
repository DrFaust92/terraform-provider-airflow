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
				Type:     schema.TypeString,
				Optional: true,
			},
			"extra": {
				Type:             schema.TypeString,
				DiffSuppressFunc: suppressSameJsonDiff,
				Optional:         true,
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

	if v, ok := d.GetOk("login"); ok {
		conn.SetLogin(v.(string))
	}

	if v, ok := d.GetOk("schema"); ok {
		conn.SetSchema(v.(string))
	}

	if v, ok := d.GetOk("port"); ok {
		conn.SetPort(int32(v.(int)))
	}

	conn.SetPassword(d.Get("password").(string))

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

	d.Set("connection_id", connection.GetConnectionId())
	d.Set("conn_type", connection.GetConnType())
	d.Set("host", connection.GetHost())
	d.Set("login", connection.GetLogin())
	d.Set("schema", connection.GetSchema())
	d.Set("port", connection.GetPort())
	d.Set("extra", connection.GetExtra())

	if v, ok := connection.GetPasswordOk(); ok {
		d.Set("password", v)
	} else if v, ok := d.GetOk("password"); ok {
		d.Set("password", v)
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

	if v, ok := d.GetOk("password"); ok && v.(string) != "" {
		conn.SetPassword(v.(string))
	}

	if v, ok := d.GetOk("extra"); ok {
		conn.SetExtra(v.(string))
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
