// Command audit runs the append-only audit-log microservice.
package main

import (
	"fmt"
	"os"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/internal/app"
)

func main() {
	if err := app.RunAudit(config.LoadAudit()); err != nil {
		fmt.Fprintf(os.Stderr, "audit: %v\n", err)
		os.Exit(1)
	}
}
