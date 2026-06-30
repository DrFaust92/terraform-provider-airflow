resource "airflow_user" "example" {
  email      = "example"
  first_name = "example"
  last_name  = "example"
  username   = "example"
  password   = "example"
  roles      = [airflow_role.example.name]
}
