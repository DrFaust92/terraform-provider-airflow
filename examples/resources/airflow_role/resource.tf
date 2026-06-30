resource "airflow_role" "example" {
  name = "example"

  action {
    action   = "can_read"
    resource = "Audit Logs"
  }
}
