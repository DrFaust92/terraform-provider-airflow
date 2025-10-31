package provider

import (
	"context"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourcePool() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourcePoolCreate,
		ReadWithoutTimeout:   resourcePoolRead,
		UpdateWithoutTimeout: resourcePoolUpdate,
		DeleteWithoutTimeout: resourcePoolDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"slots": {
				Type:     schema.TypeInt,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"include_deferred": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"occupied_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"queued_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"open_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"running_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"deferred_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"scheduled_slots": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func resourcePoolCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	name := d.Get("name").(string)
	slots := int32(d.Get("slots").(int))
	includeDeferred := d.Get("include_deferred").(bool)
	varApi := client.PoolApi

	pool := airflow.Pool{
		Name:            &name,
		Slots:           &slots,
		IncludeDeferred: &includeDeferred,
	}

	if description, ok := d.GetOk("description"); ok {
		pool.SetDescription(description.(string))
	}

	_, _, err := varApi.PostPool(pcfg.AuthContext).Pool(pool).Execute()
	if err != nil {
		return diag.Errorf("failed to create pool `%s` from Airflow: %s", name, err)
	}
	d.SetId(name)

	return resourcePoolRead(ctx, d, m)
}

func resourcePoolRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	pool, resp, err := client.PoolApi.GetPool(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if err != nil {
		return diag.Errorf("failed to get pool `%s` from Airflow: %s", d.Id(), err)
	}

	if err := d.Set("name", pool.Name); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("slots", pool.Slots); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("occupied_slots", pool.OccupiedSlots); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("queued_slots", pool.QueuedSlots); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("open_slots", pool.OpenSlots); err != nil {
		return diag.FromErr(err)
	}
	if pool.Description.IsSet() && pool.Description.Get() != nil {
		if err := d.Set("description", *pool.Description.Get()); err != nil {
			return diag.FromErr(err)
		}
	}
	if err := d.Set("include_deferred", pool.IncludeDeferred); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("running_slots", pool.RunningSlots); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("deferred_slots", pool.DeferredSlots); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("scheduled_slots", pool.ScheduledSlots); err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func resourcePoolUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	slots := int32(d.Get("slots").(int))
	includeDeferred := d.Get("include_deferred").(bool)
	name := d.Id()

	pool := airflow.Pool{
		Name:            &name,
		Slots:           &slots,
		IncludeDeferred: &includeDeferred,
	}

	if description, ok := d.GetOk("description"); ok {
		pool.SetDescription(description.(string))
	}

	_, _, err := client.PoolApi.PatchPool(pcfg.AuthContext, name).Pool(pool).Execute()
	if err != nil {
		return diag.Errorf("failed to update pool `%s` from Airflow: %s", name, err)
	}

	return resourcePoolRead(ctx, d, m)
}

func resourcePoolDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	resp, err := client.PoolApi.DeletePool(pcfg.AuthContext, d.Id()).Execute()
	if err != nil {
		return diag.Errorf("failed to delete pool `%s` from Airflow: %s", d.Id(), err)
	}

	if resp != nil && resp.StatusCode == 404 {
		return nil
	}

	return nil
}
