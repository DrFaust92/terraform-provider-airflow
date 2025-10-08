package provider

import (
	"context"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceDag() *schema.Resource {
	return &schema.Resource{
		CreateWithoutTimeout: resourceDagUpdate,
		ReadWithoutTimeout:   resourceDagRead,
		UpdateWithoutTimeout: resourceDagUpdate,
		DeleteWithoutTimeout: resourceDagDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"dag_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"description": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"delete_dag": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"file_token": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"fileloc": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"is_active": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"is_paused": {
				Type:     schema.TypeBool,
				Required: true,
			},
			"is_subdag": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"root_dag_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceDagUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	dagId := d.Get("dag_id").(string)
	dagApi := client.DAGApi
	dag := *airflow.NewDAG()
	dag.SetIsPaused(d.Get("is_paused").(bool))

	_, res, err := dagApi.PatchDag(pcfg.AuthContext, dagId).DAG(dag).Execute()
	if res.StatusCode != 200 {
		return diag.Errorf("failed to update DAG `%s` from Airflow: %s", dagId, err)
	}
	d.SetId(dagId)

	return resourceDagRead(ctx, d, m)
}

func resourceDagRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient

	DAG, resp, err := client.DAGApi.GetDag(pcfg.AuthContext, d.Id()).Execute()
	if resp != nil && resp.StatusCode == 404 {
		d.SetId("")
		return nil
	}
	if resp.StatusCode != 200 {
		return diag.Errorf("failed to get DAG `%s` from Airflow: %s", d.Id(), err)
	}

	if err := d.Set("dag_id", DAG.DagId); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("is_paused", DAG.IsPaused.Get()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("is_active", DAG.IsActive.Get()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("is_subdag", DAG.IsSubdag); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("description", DAG.Description.Get()); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("file_token", DAG.FileToken); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set("fileloc", DAG.Fileloc); err != nil {
		return diag.FromErr(err)
	}
	d.Set("root_dag_id", DAG.RootDagId.Get())

	return nil
}

func resourceDagDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	pcfg := m.(ProviderConfig)
	client := pcfg.ApiClient.DAGApi

	if d.Get("delete_dag").(bool) {
		resp, err := client.DeleteDag(pcfg.AuthContext, d.Id()).Execute()
		if err != nil {
			return diag.Errorf("failed to delete DAG `%s` from Airflow: %s", d.Id(), err)
		}

		if resp != nil && resp.StatusCode == 404 {
			return nil
		}
	}

	return nil
}
