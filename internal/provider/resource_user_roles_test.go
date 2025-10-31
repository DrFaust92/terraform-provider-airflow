package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/apache/airflow-client-go/airflow"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/logging"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

const (
	accName = "tf-acc-test-user-roles"
)

func TestAccAirflowUserRoles_basic(t *testing.T) {
	rName := acctest.RandomWithPrefix("tf-role-test")
	r2Name := acctest.RandomWithPrefix("tf-role-test2")

	resourceName := "airflow_user_roles.test"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheckCreateUser(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAirflowUserRolesCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAirflowUserRolesConfigBasic(accName, rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "username", accName),
					resource.TestCheckResourceAttr(resourceName, "roles.#", "1"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "roles.*", "airflow_role.test", "name"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: testAccAirflowUserAddRolesConfigBasic(accName, rName, r2Name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "username", accName),
					resource.TestCheckResourceAttr(resourceName, "roles.#", "2"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "roles.*", "airflow_role.test", "name"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "roles.*", "airflow_role.test2", "name"),
				),
			},
		},
	})
}

func testAccCheckAirflowUserRolesCheckDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(ProviderConfig)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "airflow_user_roles" {
			continue
		}

		user, res, err := client.ApiClient.UserApi.GetUser(client.AuthContext, rs.Primary.ID).Execute()
		if err == nil {
			if len(user.Roles) != 0 {
				return fmt.Errorf("Airflow User (%s) still have some roles.", rs.Primary.ID)
			}
		}

		if res != nil && res.StatusCode == 404 {
			continue
		}
	}
	_, _ = client.ApiClient.UserApi.DeleteUser(client.AuthContext, accName).Execute()

	return nil
}

func testAccAirflowUserRolesConfigBasic(accName, rName string) string {
	return fmt.Sprintf(`
resource "airflow_role" "test" {
  name   = %[1]q

  action {
    action   = "can_read"
	resource = "Audit Logs"
  } 
}

resource "airflow_user_roles" "test" {
  username   = %[2]q
  roles      = [airflow_role.test.name]
}
`, rName, accName)
}

func testAccAirflowUserAddRolesConfigBasic(accName, rName, r2Name string) string {
	return fmt.Sprintf(`
resource "airflow_role" "test" {
  name   = %[1]q

  action {
    action   = "can_read"
	resource = "Audit Logs"
  } 
}

resource "airflow_role" "test2" {
  name   = %[2]q

  action {
    action   = "menu_access"
	resource = "Audit Logs"
  } 
}

resource "airflow_user_roles" "test" {
  username   = %[3]q
  roles      = [airflow_role.test.name, airflow_role.test2.name]
}
`, rName, r2Name, accName)
}

func testAccPreCheckCreateUser(t *testing.T) {
	testAccPreCheck(t)
	endpoint := os.Getenv("AIRFLOW_BASE_ENDPOINT")
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("failed to initialise Airflow at `%s`: %s", endpoint, err)
	}

	client := &http.Client{
		Transport: logging.NewLoggingHTTPTransport(http.DefaultTransport),
	}
	path := strings.TrimSuffix(u.Path, "/")
	apiClient := airflow.NewAPIClient(&airflow.Configuration{
		Scheme:     u.Scheme,
		Host:       u.Host,
		Debug:      true,
		HTTPClient: client,
		Servers: airflow.ServerConfigurations{
			{
				URL:         fmt.Sprint(path, "/api/v1"),
				Description: "Apache Airflow Stable API.",
			},
		},
	})
	authContext := context.Background()
	cred := airflow.BasicAuth{
		UserName: os.Getenv("AIRFLOW_API_USERNAME"),
		Password: os.Getenv("AIRFLOW_API_PASSWORD"),
	}
	authContext = context.WithValue(authContext, airflow.ContextBasicAuth, cred)

	email := acctest.RandomWithPrefix("tf-role-email-test")
	firstName := acctest.RandomWithPrefix("tf-role-first-name-test")
	lastName := acctest.RandomWithPrefix("tf-role-last-name-test")
	password := acctest.RandomWithPrefix("tf-role-password-test")
	publicRoleName := "Public"
	roles := []airflow.UserCollectionItemRoles{{Name: &publicRoleName}}
	username := accName
	_, _, err = apiClient.UserApi.PostUser(authContext).User(airflow.User{
		Email:     &email,
		FirstName: &firstName,
		LastName:  &lastName,
		Username:  &username,
		Password:  &password,
		Roles:     roles,
	}).Execute()
	if err != nil {
		t.Fatalf("failed to create user `%s` from Airflow: %s", username, err)
	}
}
