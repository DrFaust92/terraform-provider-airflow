---
layout: "airflow"
page_title: "Airflow: airflow_user_roles"
sidebar_current: "docs-airflow-resource-user-roles"
description: |-
  Provides an Airflow user roles
---

# airflow_user roles

Provides an Airflow user roles management.

## Example Usage

```hcl
resource "airflow_user_roles" "example" {
  username   = "example"
  roles      = [airflow_role.example.name]
}
```

## Argument Reference

The following arguments are supported:
* `username` - (Required) The username
* `roles` - (Required) A set of User roles to attach to the User.

## Import

User's roles can be imported using the username.

```terraform
terraform import airflow_user_roles.example example
```
