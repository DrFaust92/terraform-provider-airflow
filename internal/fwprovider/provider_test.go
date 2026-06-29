package fwprovider

import (
	"context"
	"os"
	"testing"

	"github.com/drfaust92/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-mux/tf5to6server"
	"github.com/hashicorp/terraform-plugin-mux/tf6muxserver"
)

// testAccProtoV6ProviderFactories serves the same muxed (SDKv2 + framework)
// provider used by main.go, so acceptance tests exercise the real wiring.
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"airflow": func() (tfprotov6.ProviderServer, error) {
		ctx := context.Background()

		upgraded, err := tf5to6server.UpgradeServer(ctx, provider.AirflowProvider().GRPCProvider)
		if err != nil {
			return nil, err
		}

		muxServer, err := tf6muxserver.NewMuxServer(ctx,
			func() tfprotov6.ProviderServer { return upgraded },
			providerserver.NewProtocol6(New("test")()),
		)
		if err != nil {
			return nil, err
		}

		return muxServer.ProviderServer(), nil
	},
}

// TestMuxServer asserts that the SDKv2 and framework providers can be muxed.
// NewMuxServer compares the provider schema of every server, so this fails fast
// if the framework provider schema drifts from the SDKv2 one. Runs without
// TF_ACC.
func TestMuxServer(t *testing.T) {
	ctx := context.Background()

	upgraded, err := tf5to6server.UpgradeServer(ctx, provider.AirflowProvider().GRPCProvider)
	if err != nil {
		t.Fatalf("error upgrading SDKv2 provider: %s", err)
	}

	if _, err := tf6muxserver.NewMuxServer(ctx,
		func() tfprotov6.ProviderServer { return upgraded },
		providerserver.NewProtocol6(New("test")()),
	); err != nil {
		t.Fatalf("error setting up muxed provider: %s", err)
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
