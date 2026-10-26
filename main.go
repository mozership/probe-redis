// Command probe-redis serves a Probe action that runs commands on a Redis
// or Valkey server. Probe starts it for a step that uses
// github.com/mozership/probe-redis@<commit>.
package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/actionrpc"
)

// version is set at build time.
var version = "dev"

func main() {
	actionrpc.Serve(func(log hclog.Logger) actionrpc.Action {
		return &Action{log: log}
	})
}
