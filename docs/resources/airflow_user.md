---
layout: "airflow"
page_title: "Airflow: airflow_user"
sidebar_current: "docs-airflow-resource-user"
description: |-
  Provides an Airflow user
---

# airflow_user

Provides an Airflow user.

## Example Usage

```hcl
resource "airflow_user" "example" {
  email      = "example"
  first_name = "example"
  last_name  = "example"
  username   = "example"
  password   = "example"
  roles      = [airflow_role.example.name]
}
```

### GCP Cloud Composer

It is possible to create Airflow users when using Cloud Composer by [using the primary e-mail as the username](https://cloud.google.com/composer/docs/composer-2/airflow-rbac#registering-users). Upon first login, GCP with replace the username with a GCP user Id (formatted like `accounts.google.com:<12345678...>`). Because of this, Terraform will try to update this user during the next `apply`, which forces replacement of the complete user. To prevent this from happening, ignore any changes to the username using the `lifecycle` meta argument.

```hcl
resource "airflow_user" "user" {
  email      = "user@email.com"
  first_name = "user@email.com"
  last_name  = "-"
  username   = "user@email.com"
  password   = ""
  roles      = [airflow_role.example.name]

  lifecycle {
    ignore_changes = [username]
  }
}
```

## Argument Reference

The following arguments are supported:

- `email` - (Required) The user's email
- `first_name` - (Required) The user firstname
- `last_name` - (Required) The user lastname
- `username` - (Required) The username
- `password` - (Required) The user password.
- `roles` - (Required) A set of User roles to attach to the User.

## Attributes Reference

This resource exports the following attributes:

- `active` - Whether the user is active.
- `id` - The username.
- `failed_login_count` - The number of times the login failed.
- `login_count` - The login count.

## Import

Users can be imported using the user key.

```terraform
terraform import airflow_user.example example
```
