resource "airflow_user_roles" "example" {
  username = "example"
  roles    = [airflow_role.example.name]
}
