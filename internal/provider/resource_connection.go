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
				Type:      schema.TypeString,
				Optional:  true,
				Sensitive: true,
				DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
					// Suppress diffs only when the new value is a masked placeholder
					// returned by the API (e.g. "***"). This prevents Terraform
					// from trying to copy the masked placeholder back to the remote.
					// Do NOT suppress when the old value is masked and the user
					// provides a real password in configuration — that should update.
					np := strings.TrimSpace(new)
					if np != "" && strings.Trim(np, "*") == "" {
						return old != "" // suppress only if there was an existing value
					}
					return false
				},
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

	// Try to unmarshal both; keep errors for conditional handling below
	oldErr := json.Unmarshal([]byte(oldo), &oldIface)
	newErr := json.Unmarshal([]byte(newo), &newIface)

	// Helper: consider nil, empty map, and empty slice as 'empty JSON'
	isEmptyJSON := func(v interface{}) bool {
		if v == nil {
			return true
		}
		switch t := v.(type) {
		case map[string]interface{}:
			return len(t) == 0
		case []interface{}:
			return len(t) == 0
		default:
			return false
		}
	}

	// If both unmarshaled successfully, compare deeply. Before comparing,
	// replace any masked placeholder strings (e.g. "***") in the API/new
	// value with the corresponding value from the old/state value when
	// available. This avoids diffs when the API hides sensitive values.
	if oldErr == nil && newErr == nil {
		// helper to replace masked strings in new with values from old when possible
		var replaceMasked func(n, o interface{}) interface{}
		replaceMasked = func(n, o interface{}) interface{} {
			switch nv := n.(type) {
			case map[string]interface{}:
				// create a new map to avoid mutating original
				out := map[string]interface{}{}
				var ov map[string]interface{}
				if ovt, ok := o.(map[string]interface{}); ok {
					ov = ovt
				}
				for k, v := range nv {
					out[k] = replaceMasked(v, func() interface{} {
						if ov != nil {
							return ov[k]
						}
						return nil
					}())
				}
				return out
			case []interface{}:
				out := make([]interface{}, len(nv))
				var ov []interface{}
				if ovt, ok := o.([]interface{}); ok {
					ov = ovt
				}
				for i := range nv {
					var oe interface{}
					if ov != nil && i < len(ov) {
						oe = ov[i]
					}
					out[i] = replaceMasked(nv[i], oe)
				}
				return out
			case string:
				s := nv
				if strings.TrimSpace(s) != "" && strings.Trim(s, "*") == "" {
					if os, ok := o.(string); ok && os != "" {
						return os
					}
				}
				return s
			default:
				return nv
			}
		}

		newNorm := replaceMasked(newIface, oldIface)
		if reflect.DeepEqual(oldIface, newNorm) {
			return true
		}
		// treat empty vs null/empty-object/empty-array as equivalent
		if isEmptyJSON(oldIface) && isEmptyJSON(newIface) {
			return true
		}
		return false
	}

	// If one side failed to unmarshal but the other unmarshaled to an empty JSON
	// value (null/{} / []), treat them as equal to avoid spurious diffs.
	if oldErr != nil && newErr == nil {
		if isEmptyJSON(newIface) && strings.TrimSpace(oldo) == "" {
			return true
		}
	}
	if newErr != nil && oldErr == nil {
		if isEmptyJSON(oldIface) && strings.TrimSpace(newo) == "" {
			return true
		}
	}

	return false
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
	// Handle `extra` carefully: the API may return semantically equivalent
	// JSON with different formatting which should not cause diffs. Use the
	// existing `suppressSameJsonDiff` helper to avoid overwriting the state
	// when the values are equivalent. If the API returns an empty string or
	// a JSON null/empty object, prefer keeping the current state value.
	apiExtra := connection.GetExtra()
	if apiExtra != "" {
		var oldExtra string
		if v, ok := d.GetOk("extra"); ok {
			oldExtra = v.(string)
		}
		if !suppressSameJsonDiff("extra", oldExtra, apiExtra, d) {
			if err := d.Set("extra", apiExtra); err != nil {
				return diag.FromErr(err)
			}
		}
	}
	if err := d.Set("description", connection.GetDescription()); err != nil {
		return diag.FromErr(err)
	}

	if vptr, ok := connection.GetPasswordOk(); ok {
		if vptr != nil {
			pw := *vptr
			// If the API returns a masked placeholder (e.g. "***"), do not
			// overwrite the state value with the mask. This prevents Terraform
			// from detecting a spurious diff when the remote hides the real
			// password value.
			if !(strings.TrimSpace(pw) != "" && strings.Trim(pw, "*") == "") {
				if err := d.Set("password", pw); err != nil {
					return diag.FromErr(err)
				}
			}
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
