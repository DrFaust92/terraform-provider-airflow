terraform {
  required_providers {
    airflow = {
      source = "drfaust92/airflow"
    }
  }
}

# assumes local airflow
provider "airflow" {
  base_endpoint = "http://localhost:8080/"
  username      = "airflow"
  password      = "airflow"
}

resource "airflow_variable" "foo" {
  key   = "foo"
  value = "bar"
}

resource "airflow_variable" "hello" {
  key   = "hello"
  value = "world"
}
