---
layout: "airflow"
page_title: "Airflow: airflow_role"
sidebar_current: "docs-airflow-resource-role"
description: |-
  Provides an Airflow role
---

# Note

> Not supported on Airflow v3 (API v2): the Roles API is not available in Airflow v3.

# airflow_role

Provides an Airflow role.

## Example Usage

```hcl
resource "airflow_role" "example" {
  name   = "example"

  action {
    action   = "can_read"
    resource = "Audit Logs"
  }
}
```

## Argument Reference

The following arguments are supported:

* `name` - (Required) The name of the role
* `action` - (Required) The action struct that defines the role. See [Action](#action).

### Action

* `action` - (Required) The name of the permission.
* `resource` - (Required) The name of the resource.

## Attributes Reference

This resource exports the following attributes:

* `id` - The role name.

## Import

Roles can be imported using the role key.

```terraform
terraform import airflow_role.default example
```
