package main

import (
	"context"
	"flag"
	"log"

	"github.com/drfaust92/terraform-provider-airflow/internal/fwprovider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs generate --provider-name airflow

// version is set at build/release time via ldflags.
var version = "dev"

const providerAddr = "registry.terraform.io/drfaust92/airflow"

func main() {
	var debug bool

	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	err := providerserver.Serve(context.Background(), fwprovider.New(version), providerserver.ServeOpts{
		Address: providerAddr,
		Debug:   debug,
	})
	if err != nil {
		log.Fatal(err)
	}
}
