package main

import (
	"context"
	"flag"
	"log"

	"github.com/drfaust92/terraform-provider-airflow/internal/fwprovider"
	"github.com/drfaust92/terraform-provider-airflow/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6/tf6server"
	"github.com/hashicorp/terraform-plugin-mux/tf5to6server"
	"github.com/hashicorp/terraform-plugin-mux/tf6muxserver"
)

// version is set at build/release time via ldflags.
var version = "dev"

const providerAddr = "registry.terraform.io/drfaust92/airflow"

func main() {
	var debug bool

	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	ctx := context.Background()

	// Upgrade the legacy SDKv2 provider (protocol v5) to protocol v6 so it can
	// be muxed with the Plugin Framework provider, which speaks v6 natively.
	upgradedSDKProvider, err := tf5to6server.UpgradeServer(ctx, provider.AirflowProvider().GRPCProvider)
	if err != nil {
		log.Fatal(err)
	}

	providers := []func() tfprotov6.ProviderServer{
		func() tfprotov6.ProviderServer { return upgradedSDKProvider },
		providerserver.NewProtocol6(fwprovider.New(version)()),
	}

	muxServer, err := tf6muxserver.NewMuxServer(ctx, providers...)
	if err != nil {
		log.Fatal(err)
	}

	var serveOpts []tf6server.ServeOpt
	if debug {
		serveOpts = append(serveOpts, tf6server.WithManagedDebug())
	}

	if err := tf6server.Serve(providerAddr, muxServer.ProviderServer, serveOpts...); err != nil {
		log.Fatal(err)
	}
}
