resource "airflow_dag_run" "example" {
  dag_id     = "example"
  dag_run_id = "example"

  conf = {
    "example" = "example"
  }
}
