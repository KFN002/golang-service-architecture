// Command audit runs the append-only audit-log microservice.
package main

import (
	"fmt"
	"os"

	// Sets GOMAXPROCS from the container CPU quota — without it a limited
	// container schedules against the host core count and thrashes.
	_ "go.uber.org/automaxprocs"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/internal/app"
)

func main() {
	if err := app.RunAudit(config.LoadAudit()); err != nil {
		fmt.Fprintf(os.Stderr, "audit: %v\n", err)
		os.Exit(1)
	}
}
