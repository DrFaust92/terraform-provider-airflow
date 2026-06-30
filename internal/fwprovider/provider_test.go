package fwprovider

import (
	"context"
	"os"
	"testing"

	fwprovider "github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// testAccProtoV6ProviderFactories serves the framework provider for acceptance
// tests.
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"airflow": providerserver.NewProtocol6WithError(New("test")()),
}

// TestProvider validates that the provider schema builds without error.
func TestProvider(t *testing.T) {
	ctx := context.Background()
	resp := fwprovider.SchemaResponse{}
	New("test")().Schema(ctx, fwprovider.SchemaRequest{}, &resp)
	if resp.Diagnostics.HasError() {
		t.Fatalf("provider schema diagnostics: %+v", resp.Diagnostics)
	}
}

func testAccPreCheck(t *testing.T) {
	_, tokenOk := os.LookupEnv("AIRFLOW_OAUTH2_TOKEN")
	_, userOk := os.LookupEnv("AIRFLOW_API_USERNAME")
	_, passOk := os.LookupEnv("AIRFLOW_API_PASSWORD")

	if !tokenOk && (!userOk || !passOk) {
		t.Fatal("AIRFLOW_OAUTH2_TOKEN OR AIRFLOW_API_USERNAME/AIRFLOW_API_PASSWORD must be set for acceptance tests")
	}

	if v := os.Getenv("AIRFLOW_BASE_ENDPOINT"); v == "" {
		t.Fatal("AIRFLOW_BASE_ENDPOINT must be set for acceptance tests")
	}
}

// TestListResourcesRegistered verifies the provider advertises the list
// resources and that each list resource's type name matches a managed resource
// (the framework returns an error diagnostic at GetMetadata otherwise).
func TestListResourcesRegistered(t *testing.T) {
	ctx := context.Background()
	srv := providerserver.NewProtocol6(New("test")())()

	resp, err := srv.GetMetadata(ctx, &tfprotov6.GetMetadataRequest{})
	if err != nil {
		t.Fatalf("GetMetadata error: %s", err)
	}
	for _, d := range resp.Diagnostics {
		if d.Severity == tfprotov6.DiagnosticSeverityError {
			t.Fatalf("metadata diagnostic: %s: %s", d.Summary, d.Detail)
		}
	}

	got := make(map[string]bool)
	for _, lr := range resp.ListResources {
		got[lr.TypeName] = true
	}
	for _, want := range []string{"airflow_variable", "airflow_pool", "airflow_connection"} {
		if !got[want] {
			t.Errorf("expected list resource %q to be registered; got %v", want, got)
		}
	}
}
