---
layout: "airflow"
page_title: "Airflow: airflow_dag_run"
sidebar_current: "docs-airflow-resource-dag-run"
description: |-
  Provides an Airflow dag run resource
---

# airflow_dag_run

Provides an Airflow dag run resource (triggers a DAG).

## Example Usage

```hcl
resource "airflow_dag_run" "example" {
  dag_id     = "example"
  dag_run_id = "example"

  conf = {
    "example" = "example"
  }  
}
```

## Argument Reference

The following arguments are supported:

* `dag_id` - (Required) The DAG ID to run.
* `dag_run_id` - (Optional) The DAG Run ID. If a value is not passed, a random one will be generated based on execution date.
* `conf` - (Optional) A map describing additional configuration parameters.

## Attributes Reference

This resource exports the following attributes:

* `id` - The `dag_id:dag_run_id`.
* `state` - The DAG state.

## Import

DAG Runs can be imported using the `dag_id:dag_run_id`.

```terraform
terraform import airflow_dag_run.default example:example
```
