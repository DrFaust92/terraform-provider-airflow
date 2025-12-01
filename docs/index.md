---
layout: "airflow"
page_title: "Provider: Airflow"
sidebar_current: "docs-airflow-index"
description: |-
  The Airflow provider is used to interact with Airflow.
---

# Airflow Provider

The Airflow provider is used to interact with the Airflow. The
provider needs to be configured with the proper credentials before it can be
used.

Use the navigation to the left to read about the available data sources.

## Example Usage

```hcl
provider "airflow" {
  base_endpoint = "airflow.net"
  oauth2_token  = "token"
}

resource "airflow_variable" "default" {
  key   = "foo"
  value = "bar"
}
```

## Authentication

### Google Composer Example (OAUTH2 token)

```terraform
data "http" "client_id" {
  url = "composer-url"
}

resource "google_service_account" "example" {
  account_id = "example"
}

data "google_service_account_access_token" "impersonated" {
  target_service_account = google_service_account.example.email
  delegates              = []
  scopes                 = ["userinfo-email", "cloud-platform"]
  lifetime               = "300s"
}

provider "google" {
  alias        = "impersonated"
  access_token = data.google_service_account_access_token.impersonated.access_token
}

data "google_service_account_id_token" "oidc" {
  provider               = google.impersonated
  target_service_account = google_service_account.example.email
  delegates              = []
  include_email          = true
  target_audience        = regex("[A-Za-z0-9-]*\\.apps\\.googleusercontent\\.com", data.http.client_id.body)
}

provider "airflow" {
  base_endpoint = data.http.client_id.url
  oauth2_token  = data.google_service_account_id_token.oidc.id_token
}
```

## Argument Reference

- `base_endpoint` - (Required) The Airflow API endpoint.
- `oauth2_token` - (Optional) An OAUTH2 identity token used to authenticate against an Airflow server. **Conflicts with username and password**
- `username` - (Optional) The username to use for API basic authentication. **Conflicts with oauth2_token**
- `password` - (Optional) The password to use for API basic authentication. **Conflicts with oauth2_token**
- `disable_ssl_verification` - (Optional) Disable SSL verification. Default is `false`
- `base_path` - (Optional) Base path for the Airflow API. Default is `/api/v1`. pass `/api/v1` for Airflow v3 (API v2).

## Running Acceptence Tests

### Setting Up Local Environment

- See [Official docs](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) and run `docker-compose up` spin up a local airflow cluster.
- `export AIRFLOW_BASE_ENDPOINT=http://localhost:8080`
- `export AIRFLOW_API_PASSWORD=airflow`
- `export AIRFLOW_API_USERNAME=airflow`

### Running Tests

Run `make testacc`
