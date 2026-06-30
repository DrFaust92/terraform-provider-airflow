//go:build tools

// Package tools tracks build-time tool dependencies so they are versioned in
// go.mod. tfplugindocs generates the provider documentation under docs/ from
// the provider schema and the examples/ directory; the `go:generate` directive
// that invokes it lives in main.go (untagged) so `go generate ./...` runs it.
package tools

import (
	_ "github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs"
)
