package provider

import (
	"context"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceDag() *schema.Resource {
	return &schema.Resource{
		Description:          "Provides an Airflow DAG. This resource adopts an existing DAG and does not create one; on delete, the DAG is only removed from state and not actually deleted (unless `delete_dag` is set).",
		CreateWithoutTimeout: resourceDagUpdate,
		ReadWithoutTimeout:   resourceDagRead,
		UpdateWithoutTimeout: resourceDagUpdate,
		DeleteWithoutTimeout: resourceDagDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			"dag_id": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The ID of the DAG.",
			},
			"description": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "User-provided DAG description, which can consist of several sentences or paragraphs that describe DAG contents.",
			},
			"delete_dag": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Whether to delete the DAG when deleted from Terraform.",
			},
			"file_token": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The key containing the encrypted path to the file. Encryption and decryption take place only on the server. This prevents the client from reading a non-DAG file.",
			},
			"fileloc": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The absolute path to the file.",
			},
			"is_active": {
				Type:        schema.TypeBool,
				Computed:    true,
				Description: "Whether the DAG is currently seen by the scheduler(s).",
			},
			"is_paused": {
				Type:        schema.TypeBool,
				Required:    true,
				Description: "Whether the DAG is paused.",
			},
			"is_subdag": {
				Type:        schema.TypeBool,
				Computed:    true,
				Description: "Whether the DAG is a SubDAG.",
			},
			"root_dag_id": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "If the DAG is a SubDAG then it is the top level DAG identifier. Otherwise, null.",
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
	if err != nil {
		return diag.Errorf("failed to update DAG `%s` from Airflow: %s", dagId, err)
	}
	if res != nil && res.StatusCode != 200 {
		return diag.Errorf("failed to update DAG `%s`, received non-200 status code: %d", dagId, res.StatusCode)
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
	if err != nil {
		return diag.Errorf("failed to get DAG `%s` from Airflow: %s", d.Id(), err)
	}
	if resp != nil && resp.StatusCode != 200 {
		return diag.Errorf("failed to get DAG `%s`, received non-200 status code: %d", d.Id(), resp.StatusCode)
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
	if err := d.Set("root_dag_id", DAG.RootDagId.Get()); err != nil {
		return diag.FromErr(err)
	}

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
